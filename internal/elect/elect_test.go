package elect_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mheers/nats-lightning/internal/elect"
	"github.com/mheers/nats-lightning/internal/testsupport/natsd"
)

// Slice 12: leader election.
//
// The upstream throttles new connections severely, so exactly one process may
// hold the connection. Two instances would spend their lives fighting over a
// connection neither could hold. The same lock keeps SQLite to one writer.
//
// The seam is a real broker again: the correctness of Acquire rests on NATS KV
// Create being atomic, which a fake cannot demonstrate.

func lock(t *testing.T, ns *natsd.Server, bucket, owner string) *elect.Lock {
	t.Helper()
	nc := ns.Connect(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	l, err := elect.NewLock(js, bucket, elect.Options{
		Key:   "leader",
		Owner: owner,
		TTL:   600 * time.Millisecond,
		Renew: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLock: %v", err)
	}
	return l
}

func TestTheFirstProcessWinsTheLease(t *testing.T) {
	ns := natsd.Start(t)
	l := lock(t, ns, "TEST1", "alpha")

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := l.Acquire(ctx); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !l.IsLeader() {
		t.Error("IsLeader is false immediately after acquiring")
	}
}

// Two instances must not both believe they are leader. This is the property the
// whole package exists for.
func TestASecondProcessCannotTakeALiveLease(t *testing.T) {
	ns := natsd.Start(t)
	alpha := lock(t, ns, "TEST2", "alpha")
	bravo := lock(t, ns, "TEST2", "bravo")

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := alpha.Acquire(ctx); err != nil {
		t.Fatalf("alpha Acquire: %v", err)
	}

	// Bravo must not get the lease while alpha holds it.
	short, cancelShort := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancelShort()
	if err := bravo.Acquire(short); err == nil {
		t.Fatal("bravo acquired a lease that alpha still holds")
	}
	if bravo.IsLeader() {
		t.Error("bravo believes it is leader while alpha holds the lease")
	}
}

// When the leader leaves, a standby must take over rather than waiting out a
// long TTL. Otherwise a rolling restart would leave the feed silent.
func TestAStandbyTakesOverWhenTheLeaderReleases(t *testing.T) {
	ns := natsd.Start(t)
	alpha := lock(t, ns, "TEST3", "alpha")
	bravo := lock(t, ns, "TEST3", "bravo")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	if err := alpha.Acquire(ctx); err != nil {
		t.Fatalf("alpha Acquire: %v", err)
	}
	alphaCtx, stopAlpha := context.WithCancel(ctx)
	alphaDone := make(chan error, 1)
	go func() { alphaDone <- alpha.Run(alphaCtx) }()

	// Give alpha a moment to start renewing.
	time.Sleep(150 * time.Millisecond)
	stopAlpha()
	if err := <-alphaDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("alpha Run returned %v, want context.Canceled", err)
	}

	// Bravo should now get it promptly.
	if err := bravo.Acquire(ctx); err != nil {
		t.Fatalf("bravo failed to take over: %v", err)
	}
	if !bravo.IsLeader() {
		t.Error("bravo is not leader after taking over")
	}
}

// A leader whose lease was taken over must stop rather than continue. Carrying
// on would mean two writers on one SQLite file and two upstream connections.
func TestLosingTheLeaseStopsTheLeader(t *testing.T) {
	ns := natsd.Start(t)
	alpha := lock(t, ns, "TEST4", "alpha")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := alpha.Acquire(ctx); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	done := make(chan error, 1)
	go func() { done <- alpha.Run(runCtx) }()

	time.Sleep(150 * time.Millisecond)

	// Simulate an external takeover by overwriting the lease value directly.
	//
	// A rival cannot do this through Acquire while alpha holds a live lease,
	// which is the point of the lock. Overwriting is what an operator or a
	// leader that was partitioned and came back would do, and alpha has to
	// notice.
	nc := ns.Connect(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	kv, err := js.KeyValue("TEST4")
	if err != nil {
		t.Fatalf("key value: %v", err)
	}
	entry, err := kv.Get("leader")
	if err != nil {
		t.Fatalf("get lease: %v", err)
	}
	if _, err := kv.Update("leader", []byte("usurper"), entry.Revision()); err != nil {
		t.Fatalf("seizing lease: %v", err)
	}

	// Alpha must notice at its next renewal and stop.
	select {
	case err := <-done:
		if !errors.Is(err, elect.ErrNotLeader) {
			t.Errorf("Run returned %v, want ErrNotLeader", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the displaced leader did not stop within 3s")
	}
}

// A leader that is simply busy must keep its lease, which is what the renewal
// interval and TTL ratio exist for.
func TestAnActiveLeaderKeepsItsLease(t *testing.T) {
	ns := natsd.Start(t)
	alpha := lock(t, ns, "TEST5", "alpha")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := alpha.Acquire(ctx); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	done := make(chan error, 1)
	go func() { done <- alpha.Run(runCtx) }()

	// Longer than the TTL, so it can only survive through renewals.
	time.Sleep(700 * time.Millisecond)
	if !alpha.IsLeader() {
		t.Error("the leader lost its own lease while renewing")
	}

	stopRun()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
}

// Cancelling must release promptly so a standby is not left waiting out the TTL.
func TestCancellingReleasesTheLease(t *testing.T) {
	ns := natsd.Start(t)
	alpha := lock(t, ns, "TEST6", "alpha")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := alpha.Acquire(ctx); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	runCtx, stopRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- alpha.Run(runCtx) }()
	time.Sleep(50 * time.Millisecond)

	stopRun()
	<-done

	if alpha.IsLeader() {
		t.Error("the lease is still held after Run returned")
	}
}

// A leader that dies without releasing must not hold the lease for ever.
//
// This is the case a clean-shutdown test cannot reach: the entry is left behind
// and has to expire on its own. It was a real bug, found by running the binary
// against a broker and restarting it. The per-entry update helpers in this
// client version do not apply a TTL, so the entry survived indefinitely and no
// standby could ever acquire leadership.
func TestAnAbandonedLeaseExpires(t *testing.T) {
	ns := natsd.Start(t)
	alpha := lock(t, ns, "TEST7", "alpha")
	bravo := lock(t, ns, "TEST7", "bravo")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	if err := alpha.Acquire(ctx); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// Alpha vanishes without releasing: no Run, no cancel, no Delete.
	if !alpha.IsLeader() {
		t.Fatal("alpha should hold the lease immediately after acquiring")
	}

	// Bravo must take it purely because the lease lapsed.
	takeover, cancelTakeover := context.WithTimeout(ctx, 5*time.Second)
	defer cancelTakeover()
	if err := bravo.Acquire(takeover); err != nil {
		t.Fatalf("bravo could not take over an abandoned lease: %v", err)
	}
	if !bravo.IsLeader() {
		t.Error("bravo should be leader once the abandoned lease expired")
	}
}

// Starting against a bucket that already exists is the normal case on a restart,
// and it is a different code path from creating one.
//
// It was a real bug: the create error was swallowed and the nil handle that
// CreateKeyValue returns was kept, which compares as non-nil and then panics on
// first use. So the failure surfaced as a segfault inside Acquire instead of an
// error from NewLock. Each test here uses a fresh embedded server, which is why
// nothing caught it until the binary was run against a real broker and restarted.
func TestAdoptingAnExistingBucketYieldsAUsableLock(t *testing.T) {
	ns := natsd.Start(t)

	first := lock(t, ns, "TEST8", "alpha")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := first.Acquire(ctx); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	// A second NewLock against the same bucket must produce a working lock, not
	// one that panics when used.
	second := lock(t, ns, "TEST8", "bravo")
	if second == nil {
		t.Fatal("NewLock returned nil for an existing bucket")
	}

	// It must observe the first holder and refuse to take over.
	short, cancelShort := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancelShort()
	if err := second.Acquire(short); err == nil {
		t.Error("the second lock acquired a bucket that is already held")
	}
	if second.IsLeader() {
		t.Error("the second lock believes it leads an already-held bucket")
	}
}
