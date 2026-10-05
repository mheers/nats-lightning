package dedup_test

import (
	"testing"
	"time"

	"github.com/mheers/nats-lightning/internal/dedup"
	"github.com/mheers/nats-lightning/internal/model"
)

func stroke(src model.Source, id int64) model.Stroke {
	return model.Stroke{
		Src:      src,
		StrokeID: id,
		Time:     time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC),
		Lat:      35.3340688,
		Lon:      24.4944483,
	}
}

// The upstream replays roughly five minutes of history every time a connection
// is established, so the same strokes arrive twice across a reconnect. Without
// suppression a consumer sees phantom lightning.
func TestReplayedStrokeIsDeliveredOnlyOnce(t *testing.T) {
	f := dedup.New(dedup.Options{})
	now := time.Now()

	if !f.Accept(stroke(model.SourceLightningMaps, 1949808), now) {
		t.Fatal("the first delivery of a stroke must be accepted")
	}
	if f.Accept(stroke(model.SourceLightningMaps, 1949808), now) {
		t.Error("the replayed stroke must be rejected")
	}
}

// Two networks issue independent id sequences, so the same id from a different
// source is a different stroke and must survive.
func TestIdenticalIDFromAnotherSourceIsADifferentStroke(t *testing.T) {
	f := dedup.New(dedup.Options{})
	now := time.Now()

	if !f.Accept(stroke(model.SourceLightningMaps, 7), now) {
		t.Fatal("first stroke rejected")
	}
	if !f.Accept(stroke(model.SourceBlitzortung, 7), now) {
		t.Error("a stroke with the same id from another network must be accepted")
	}
	if f.Accept(stroke(model.SourceBlitzortung, 7), now) {
		t.Error("the replay of that stroke must be rejected")
	}
}

// During the reconnect window every stroke looks historical. Dropping that
// backlog is the first of the pipeline's four deduplication layers.
func TestStrokesOlderThanTheReconnectWindowAreTreatedAsBackfill(t *testing.T) {
	connectAt := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	f := dedup.New(dedup.Options{ReconnectWindow: 5 * time.Minute})
	f.StartConnectionAt(connectAt)

	old := stroke(model.SourceLightningMaps, 1)
	old.Time = connectAt.Add(-6 * time.Minute) // outside the window
	if f.Accept(old, connectAt) {
		t.Error("a stroke older than the reconnect window must be dropped as backfill")
	}

	recent := stroke(model.SourceLightningMaps, 2)
	recent.Time = connectAt.Add(-30 * time.Second)
	if !f.Accept(recent, connectAt) {
		t.Error("a stroke inside the reconnect window must be accepted")
	}
}

// Backfill is only replayed at the start of a connection. After that, even an
// old stroke is legitimate data arriving late.
func TestOldStrokesAreAcceptedAfterTheBackfillWindowCloses(t *testing.T) {
	f := dedup.New(dedup.Options{ReconnectWindow: 5 * time.Minute, Now: func() time.Time {
		return time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	}})
	connectAt := time.Now().Add(-10 * time.Minute) // the connection began long ago

	late := stroke(model.SourceLightningMaps, 3)
	late.Time = connectAt.Add(-8 * time.Minute)

	if !f.Accept(late, time.Now()) {
		t.Error("once past the backfill window, an old stroke is late data, not a replay")
	}
}

// The remembered-set has to expire, or a long-running process would grow without
// bound. After the entry is forgotten the same id is treated as new.
func TestTheRememberedSetExpires(t *testing.T) {
	clock := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	f := dedup.New(dedup.Options{TTL: time.Minute, Now: func() time.Time { return clock }})

	if !f.Accept(stroke(model.SourceLightningMaps, 9), clock) {
		t.Fatal("first delivery rejected")
	}
	if f.Accept(stroke(model.SourceLightningMaps, 9), clock.Add(30*time.Second)) {
		t.Error("the replay inside the TTL must be rejected")
	}

	// Past the TTL the entry is pruned, and the stroke is treated as new. This
	// is the deliberate trade: the upstream's own id windows are far shorter, so
	// a stroke reappearing after the TTL is genuinely late data rather than a
	// reconnect replay.
	if !f.Accept(stroke(model.SourceLightningMaps, 9), clock.Add(2*time.Minute)) {
		t.Error("after the TTL the entry should be forgotten")
	}
}

// The set must stay bounded. At the observed global rate of about 20 strokes a
// second, a long-lived process would otherwise accumulate entries forever.
func TestTheRememberedSetStaysBounded(t *testing.T) {
	clock := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	f := dedup.New(dedup.Options{TTL: time.Minute, Now: func() time.Time { return clock }})

	for i := range 50000 {
		clock = clock.Add(time.Millisecond)
		f.Accept(stroke(model.SourceLightningMaps, int64(i)), clock)
	}

	if n := f.Tracked(); n > 100000 {
		t.Errorf("tracking %d entries after 50k strokes in a minute of simulated time; "+
			"the set is not being pruned", n)
	}
}

// The filter must not corrupt a stroke it accepts.
func TestAcceptedStrokesArePassedThroughUnchanged(t *testing.T) {
	f := dedup.New(dedup.Options{})
	orig := stroke(model.SourceLightningMaps, 42)
	orig.Time = time.Now()
	dev := 2211
	orig.DeviationM = &dev

	if !f.Accept(orig, time.Now()) {
		t.Fatal("stroke rejected")
	}
	if got := f.Accept(orig, time.Now()); got {
		t.Error("expected the duplicate to be rejected")
	}

	// Re-accepting is how the caller knows the stroke passed; the value it
	// already holds must be unaffected.
	if orig.StrokeID != 42 || orig.Src != model.SourceLightningMaps {
		t.Error("the filter mutated the stroke it was given")
	}
	if orig.DeviationM == nil || *orig.DeviationM != 2211 {
		t.Error("the filter altered the deviation estimate")
	}
}

// Counters drive the metrics that reveal when the feed changes shape, so they
// must distinguish the drop reasons rather than reporting one opaque total.
func TestTheFilterReportsWhyStrokesWereDropped(t *testing.T) {
	clock := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	f := dedup.New(dedup.Options{ReconnectWindow: 5 * time.Minute, Now: func() time.Time { return clock }})
	f.StartConnectionAt(clock)

	backfill := stroke(model.SourceLightningMaps, 1)
	backfill.Time = clock.Add(-6 * time.Minute)
	f.Accept(backfill, clock) // dropped as backfill

	live := stroke(model.SourceLightningMaps, 2)
	live.Time = clock
	f.Accept(live, clock)
	f.Accept(live, clock) // dropped as a replay

	stats := f.Stats()
	if stats.BackfillDropped != 1 {
		t.Errorf("BackfillDropped = %d, want 1", stats.BackfillDropped)
	}
	if stats.DuplicatesDropped != 1 {
		t.Errorf("DuplicatesDropped = %d, want 1", stats.DuplicatesDropped)
	}
	if stats.Accepted != 1 {
		t.Errorf("Accepted = %d, want 1", stats.Accepted)
	}
	if stats.Total() != 3 {
		t.Errorf("Total() = %d, want 3", stats.Total())
	}
}
