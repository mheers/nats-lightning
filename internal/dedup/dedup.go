// Package dedup suppresses replayed lightning strokes.
//
// The upstream replays roughly five minutes of history on every connection, so a
// reconnect delivers the same strokes a second time. Left alone, a consumer sees
// phantom lightning at exactly the moment it is trying to establish trust in the
// feed.
//
// This is the second of four layers. The others, in order of how early they act:
//
//  1. backfill window     — discard the replay outright, at connect time
//  2. this package        — remember (source, id) and reject a repeat
//  3. JetStream Nats-Msg-Id — the broker drops a duplicate publish
//  4. SQLite UNIQUE       — a durable constraint, surviving a restart
//
// The layers overlap deliberately. Any one of them alone can be defeated: a
// process restart forgets its memory, a broker may not have dedup enabled, and a
// TTL can expire before a slow replay arrives.
package dedup

import (
	"sync"
	"time"

	"github.com/heers-it/lightningfeed/internal/model"
)

// DefaultReconnectWindow is how far back the upstream replays on connect,
// measured at about 300 seconds.
const DefaultReconnectWindow = 5 * time.Minute

// pruneInterval is how often the remembered set is swept.
const pruneInterval = 5 * time.Second

// DefaultTTL is how long a stroke identity is remembered.
//
// It only has to outlive the upstream's own replay window. Entries older than
// that cannot be replays any more, so remembering them longer would grow the set
// for nothing.
const DefaultTTL = 10 * time.Minute

// Options configures a Filter.
type Options struct {
	// ReconnectWindow is how far back on connect to treat strokes as replayed
	// history. Set it to zero to keep everything.
	ReconnectWindow time.Duration

	// TTL is how long a seen identity is remembered.
	TTL time.Duration

	// Now supplies the current time. Nil means time.Now. Tests inject a clock
	// so expiry can be exercised without sleeping.
	Now func() time.Time
}

func (o Options) withDefaults() Options {
	if o.ReconnectWindow < 0 {
		o.ReconnectWindow = 0
	}
	if o.TTL <= 0 {
		o.TTL = DefaultTTL
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Stats counts what the filter has done, split by reason so a change in the
// feed's behaviour is visible rather than hidden inside one total.
type Stats struct {
	// Accepted is the number of strokes passed on.
	Accepted int

	// DuplicatesDropped is the number of strokes whose (source, id) had already
	// been seen.
	DuplicatesDropped int

	// BackfillDropped is the number of strokes discarded as replayed history at
	// the start of a connection.
	BackfillDropped int
}

// Total is the number of strokes offered to the filter.
func (s Stats) Total() int {
	return s.Accepted + s.DuplicatesDropped + s.BackfillDropped
}

// entry is one remembered identity and when it was last seen.
type entry struct {
	seenAt time.Time
}

// Filter decides whether a stroke is new. It is safe for concurrent use.
//
// Reconnection resets the backfill window, because a new connection replays
// history again.
type Filter struct {
	opts Options

	// backfillBefore is the instant before which strokes are replayed history.
	// Zero means no backfill suppression is active.
	backfillBefore time.Time

	mu        sync.Mutex
	seen      map[string]entry
	lastPrune time.Time

	stats Stats
}

// New returns a Filter.
func New(opts Options) *Filter {
	return &Filter{
		opts: opts.withDefaults(),
		seen: make(map[string]entry),
	}
}

// StartConnection opens a new backfill window relative to now.
//
// Strokes older than ReconnectWindow are discarded as replayed history. Calling
// this on every (re)connection is what makes the layer work.
func (f *Filter) StartConnection() {
	f.StartConnectionAt(f.opts.Now())
}

// StartConnectionAt is StartConnection with an explicit instant.
func (f *Filter) StartConnectionAt(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.opts.ReconnectWindow <= 0 {
		f.backfillBefore = time.Time{}
		return
	}
	f.backfillBefore = now.Add(-f.opts.ReconnectWindow)
}

// Accept reports whether a stroke should be delivered, and records the decision.
//
// now is the instant the stroke arrived. Passing it in rather than reading the
// clock inside keeps the decision reproducible.
func (f *Filter) Accept(s model.Stroke, now time.Time) bool {
	key := s.Key()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Replayed history from the reconnect.
	if !f.backfillBefore.IsZero() && s.Time.Before(f.backfillBefore) {
		f.stats.BackfillDropped++
		return false
	}

	f.pruneLocked(now)

	if _, seen := f.seen[key]; seen {
		f.stats.DuplicatesDropped++
		return false
	}

	f.seen[key] = entry{seenAt: now}
	f.stats.Accepted++
	return true
}

// pruneLocked forgets identities older than the TTL.
//
// Pruning scans the whole map, so it runs on a timer rather than per stroke: at
// the observed rate a full scan every few seconds is far cheaper than
// maintaining a heap for a set that holds tens of thousands of small entries.
func (f *Filter) pruneLocked(now time.Time) {
	if f.lastPrune.IsZero() {
		f.lastPrune = now
		return
	}
	if now.Sub(f.lastPrune) < pruneInterval {
		return
	}
	f.lastPrune = now

	cutoff := now.Add(-f.opts.TTL)
	for k, v := range f.seen {
		if v.seenAt.Before(cutoff) {
			delete(f.seen, k)
		}
	}
}

// Tracked returns how many identities are currently remembered.
func (f *Filter) Tracked() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seen)
}

// Stats returns a snapshot of the counters.
func (f *Filter) Stats() Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

// Reset clears the remembered set and the backfill window.
//
// Used when the upstream server changes: the two servers have independent id
// spaces, so identities from one mean nothing to the other.
func (f *Filter) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = make(map[string]entry)
	f.lastPrune = time.Time{}
	f.backfillBefore = time.Time{}
}
