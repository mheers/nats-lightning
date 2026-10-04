// Package elect provides single-writer leader election over NATS KV.
//
// Exactly one process may hold the upstream connection, and that is not a
// design preference: the upstream throttles new connections severely. Measured
// against the live service, attempts with no delay between them succeeded zero
// times out of six. Two ingest instances would spend their lives fighting over
// a connection neither could hold reliably.
//
// The same lock keeps SQLite to a single writer, since the database belongs to
// the leader.
package elect

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// ErrNotLeader is returned once leadership has been lost.
var ErrNotLeader = errors.New("elect: leadership lost")

// Options configures a Lock.
type Options struct {
	// Key is the KV bucket entry holding the lock.
	Key string

	// TTL is how long a lease survives without renewal. It has to exceed the
	// renewal interval by a wide margin, so a brief stall does not hand
	// leadership to a standby and then take it back.
	TTL time.Duration

	// Renew is how often the lease is refreshed.
	Renew time.Duration

	// Owner identifies this process in the lock value, for diagnostics.
	Owner string

	// Logger receives diagnostics. Nil discards.
	Logger *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.TTL <= 0 {
		o.TTL = 30 * time.Second
	}
	if o.Renew <= 0 || o.Renew >= o.TTL {
		o.Renew = o.TTL / 3
	}
	if o.Logger == nil {
		o.Logger = slog.New(discard{})
	}
	return o
}

// Lock is a renewable exclusive lease.
//
// Acquire uses Create, which succeeds only if the key does not exist. That is
// the atomic compare-and-swap the whole scheme rests on: there is no window
// between checking and writing where two candidates could both win.
type Lock struct {
	kv    nats.KeyValue
	opts  Options
	owner string

	mu           sync.Mutex
	cachedLeader cacheEntry
}

// cacheEntry is a remembered leadership answer and when it was taken.
type cacheEntry struct {
	value bool
	at    time.Time
}

// NewLock prepares a lock on the given bucket.
//
// The bucket is created with a bounded history so it cannot grow without limit;
// it holds a handful of revisions at a time.
func NewLock(js nats.JetStreamContext, bucket string, opts Options) (*Lock, error) {
	// TTL is the lease lifetime, and it is what makes this a lease rather than a
	// permanent claim.
	//
	// The per-entry update helpers in this client version do not apply a TTL, so
	// a plain Create followed by Update leaves the entry alive for ever. A leader
	// that crashed, was killed, or lost its network would then hold leadership
	// indefinitely and no standby could take over.
	//
	// Each renewal republishes the entry, resetting its age, so a live leader
	// keeps the lease and a dead one lets it lapse. The value is fixed when the
	// bucket is first created.
	defaults := opts.withDefaults()

	cfg := &nats.KeyValueConfig{
		Bucket:  bucket,
		History: 5,
		TTL:     defaults.TTL,
	}

	kv, err := js.CreateKeyValue(cfg)
	if err == nil {
		return &Lock{kv: kv, opts: defaults, owner: opts.Owner}, nil
	}

	// The bucket already exists, which is the normal case on a restart or when a
	// standby starts before the leader has created it. Fetch the existing handle
	// rather than continuing with the nil one CreateKeyValue returned: that nil
	// interface compares as non-nil and panics on first use, so the failure would
	// surface as a segfault inside Acquire rather than as an error here.
	existing, getErr := js.KeyValue(bucket)
	if getErr == nil {
		defaults.Logger.Info("adopted an existing leadership bucket; its TTL is fixed at creation",
			"bucket", bucket)
		return &Lock{kv: existing, opts: defaults, owner: defaults.Owner}, nil
	}

	return nil, fmt.Errorf("elect: preparing bucket %q: %w", bucket, err)
}

// Acquire waits until this process holds the lease, or ctx ends.
//
// Waiting rather than failing is deliberate: during a rolling restart the
// outgoing leader releases or loses the lease, and the standby should take over
// without anyone orchestrating it.
func (l *Lock) Acquire(ctx context.Context) error {
	for {
		created, err := l.kv.Create(l.opts.Key, []byte(l.opts.Owner))
		switch {
		case err == nil && created > 0:
			l.opts.Logger.Info("acquired leadership", "key", l.opts.Key, "owner", l.opts.Owner)
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		}

		// Someone else holds it. Wait a little, then retry.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Run holds the lease until ctx ends, renewing it in the background.
//
// It returns nil when ctx is cancelled cleanly, and ErrNotLeader if the lease
// was lost to another process, which is not recoverable and must stop the
// upstream connection rather than continue writing to SQLite from a process that
// no longer owns it.
func (l *Lock) Run(ctx context.Context) error {
	ticker := time.NewTicker(l.opts.Renew)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			l.release()
			return ctx.Err()
		case <-ticker.C:
			if err := l.renew(); err != nil {
				l.opts.Logger.Error("lost leadership", "error", err, "owner", l.opts.Owner)
				return fmt.Errorf("%w: %w", ErrNotLeader, err)
			}
		}
	}
}

// renew extends the lease only if this process still owns it.
//
// The update is conditional on the current revision, so a renewal cannot succeed
// after another process has taken over. Renewing unconditionally would let two
// leaders both believe they hold the lock.
func (l *Lock) renew() error {
	entry, err := l.kv.Get(l.opts.Key)
	if err != nil {
		return fmt.Errorf("reading lease: %w", err)
	}

	if owner := strings.TrimSpace(string(entry.Value())); owner != l.opts.Owner {
		return fmt.Errorf("lease is held by %q, not %q", owner, l.opts.Owner)
	}

	if _, err := l.kv.Update(l.opts.Key, []byte(l.opts.Owner), entry.Revision()); err != nil {
		return fmt.Errorf("extending lease: %w", err)
	}

	// A successful renewal is proof of ownership, so the cache is refreshed here
	// rather than being left to expire on its own.
	l.mu.Lock()
	l.cachedLeader = cacheEntry{value: true, at: time.Now()}
	l.mu.Unlock()
	return nil
}

// release drops the lease if this process owns it, so a standby can take over
// immediately rather than waiting out the TTL.
func (l *Lock) release() {
	entry, err := l.kv.Get(l.opts.Key)
	if err != nil {
		return
	}
	if owner := strings.TrimSpace(string(entry.Value())); owner != l.opts.Owner {
		return
	}
	if err := l.kv.Delete(l.opts.Key); err != nil {
		l.opts.Logger.Warn("could not release lease", "error", err)
		return
	}

	l.mu.Lock()
	l.cachedLeader = cacheEntry{value: false, at: time.Now()}
	l.mu.Unlock()

	l.opts.Logger.Info("released leadership", "key", l.opts.Key)
}

// IsLeader reports whether this process currently believes it holds the lease.
// It is a check, not a guarantee, and is intended for diagnostics.
func (l *Lock) IsLeader() bool {
	// Cached, so a scrape cannot turn into a burst of KV reads.
	//
	// The lease is renewed on a ticker, and this is called on every health check
	// and every metrics poll. Reading through to the broker each time would make
	// the observability path a load generator against the very bucket that decides
	// who owns the pipeline.
	l.mu.Lock()
	cached := l.cachedLeader
	l.mu.Unlock()
	if time.Since(cached.at) < l.opts.Renew {
		return cached.value
	}

	entry, err := l.kv.Get(l.opts.Key)
	if err != nil {
		return false
	}
	leader := strings.TrimSpace(string(entry.Value())) == l.opts.Owner

	l.mu.Lock()
	l.cachedLeader = cacheEntry{value: leader, at: time.Now()}
	l.mu.Unlock()
	return leader
}

type discard struct{}

func (discard) Enabled(context.Context, slog.Level) bool  { return false }
func (discard) Handle(context.Context, slog.Record) error { return nil }
func (d discard) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discard) WithGroup(string) slog.Handler           { return d }
