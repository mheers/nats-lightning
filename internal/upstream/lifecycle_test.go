package upstream_test

import (
	"context"
	"testing"
	"time"

	"github.com/heers-it/lightningfeed/internal/model"
	"github.com/heers-it/lightningfeed/internal/testsupport/fakews"
	"github.com/heers-it/lightningfeed/internal/upstream"
)

// Connected used to be derived from the time of the last message, which stayed
// set across every failed reconnect. The upstream's characteristic failure is a
// silent dial hang rather than an error, so the gap between sessions is routinely
// minutes long and the client kept reporting itself connected throughout it.
//
// A readiness probe built on that answer reports ready during precisely the outage
// it exists to detect, so this is asserted directly rather than through the health
// endpoint.
func TestConnectedIsFalseWhileReconnecting(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		Frames: []string{
			`{"time":1791106720.0,"strokes":[{"time":1791106720000,"lat":35.3,"lon":24.5,"src":2,"id":1,"del":1800,"dev":2000}]}`,
		},
		FrameDelay:       10 * time.Millisecond,
		CloseAfterFrames: true,
	})

	// A long floor guarantees the client is still asleep in backoff when the
	// assertion runs, rather than having already reconnected.
	c := upstream.New(upstream.Options{
		URL:              srv.URL(),
		HandshakeTimeout: 5 * time.Second,
		IdleTimeout:      500 * time.Millisecond,
		ReconnectMin:     30 * time.Second,
		ReconnectMax:     30 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	delivered := make(chan model.Stroke, 4)
	go func() {
		_ = c.Run(ctx, func(_ context.Context, s model.Stroke) error {
			delivered <- s
			return nil
		})
	}()

	select {
	case <-delivered:
	case <-time.After(10 * time.Second):
		t.Fatal("no stroke was delivered")
	}

	if !c.Connected() {
		t.Fatal("Connected is false while a stroke is arriving")
	}

	// The upstream has closed. The client will notice, end the session, and sleep
	// before trying again.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && c.Reconnects() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if c.Reconnects() == 0 {
		t.Fatal("the client never noticed the upstream closed")
	}

	// A reconnect is in flight or pending. The socket is not live, and the last
	// message is stale, so Connected must be false.
	if c.Connected() {
		t.Errorf("Connected is true with %d reconnect(s) pending; the last message is from %v, "+
			"so a readiness probe would report healthy during an outage",
			c.Reconnects(), c.LastMessage())
	}
}

// LastMessage staying populated across a disconnection is fine and intended — it
// is what the age gauge reads. It is only Connected that must not be derived
// from it, and this pins the distinction so the two cannot be merged again.
func TestLastMessageSurvivesADisconnection(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		Frames: []string{
			`{"time":1791106720.0,"strokes":[{"time":1791106720000,"lat":35.3,"lon":24.5,"src":2,"id":1,"del":1800,"dev":2000}]}`,
		},
		FrameDelay:       10 * time.Millisecond,
		CloseAfterFrames: true,
	})

	c := upstream.New(upstream.Options{
		URL:              srv.URL(),
		HandshakeTimeout: 5 * time.Second,
		IdleTimeout:      500 * time.Millisecond,
		ReconnectMin:     30 * time.Second,
		ReconnectMax:     30 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	delivered := make(chan model.Stroke, 4)
	go func() {
		_ = c.Run(ctx, func(_ context.Context, s model.Stroke) error {
			delivered <- s
			return nil
		})
	}()

	select {
	case <-delivered:
	case <-time.After(10 * time.Second):
		t.Fatal("no stroke was delivered")
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && c.Reconnects() == 0 {
		time.Sleep(20 * time.Millisecond)
	}

	if c.LastMessage().IsZero() {
		t.Error("LastMessage was cleared; the age gauge needs the last frame time to report staleness")
	}
}

// The backfill window is opened by the session-start callback. Before this
// existed the pipeline had a dedup filter with a reconnect window and nothing
// ever told it a connection had started, so the first of the four
// replay-suppression layers never ran.
func TestOnSessionStartFiresOnceTheHandshakeCompletes(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		FramesPerConnection: true,
		Frames: []string{
			`{"time":1791106720.0,"strokes":[{"time":1791106720000,"lat":35.3,"lon":24.5,"src":2,"id":1,"del":1800,"dev":2000}]}`,
		},
		FrameDelay:       10 * time.Millisecond,
		CloseAfterFrames: true,
	})

	sessions := make(chan time.Time, 8)
	c := upstream.New(upstream.Options{
		URL:              srv.URL(),
		HandshakeTimeout: 5 * time.Second,
		IdleTimeout:      500 * time.Millisecond,
		ReconnectMin:     20 * time.Millisecond,
		ReconnectMax:     100 * time.Millisecond,
		OnSessionStart:   func() { sessions <- time.Now() },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = c.Run(ctx, func(context.Context, model.Stroke) error { return nil })
	}()

	select {
	case <-sessions:
	case <-time.After(10 * time.Second):
		t.Fatal("the session-start callback never fired")
	}

	// It must fire per session, not once per process, or a reconnect's replay
	// reaches the pipeline with no window open. That is the whole reason the
	// callback sits on the session rather than being called once at startup.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(sessions) < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	if len(sessions) < 2 {
		t.Fatalf("the callback fired %d time(s) across %d connection(s); it must fire once per session",
			len(sessions), srv.ConnectionCount())
	}
}

// The backoff counter is cumulative for the life of the process, so without a
// reset a deployment that reconnects every few hours would drift to the ceiling
// after a few days and sit there permanently, even though every one of those
// sessions was healthy.
func TestBackoffEscalatesAcrossQuickFailures(t *testing.T) {
	// The upstream that never accepts a connection: the silent hang, which is its
	// characteristic failure and the reason the floor exists at all.
	dead := fakews.Start(t, &fakews.Server{NoHello: true})

	c := upstream.New(upstream.Options{
		URL:              dead.URL(),
		HandshakeTimeout: 200 * time.Millisecond,
		IdleTimeout:      200 * time.Millisecond,
		ReconnectMin:     10 * time.Millisecond,
		ReconnectMax:     320 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = c.Run(ctx, func(context.Context, model.Stroke) error { return nil })

	// Several quick failures in a row must escalate. The counter only resets
	// after a session that lasted long enough to count as healthy, and none of
	// these ever handshook.
	if c.Reconnects() < 3 {
		t.Errorf("Reconnects = %d after 3s of failures; the delay is not escalating", c.Reconnects())
	}
}

// A session that ran for a meaningful length of time is evidence the upstream is
// reachable, so the delay returns to the floor. Without this the counter is
// cumulative for the life of the process: a deployment that reconnects every few
// hours drifts to the ceiling after a few days and sits there permanently, even
// though every one of those sessions was healthy.
func TestBackoffResetsAfterAHealthySession(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		FramesPerConnection: true,
		Frames: []string{
			`{"time":1791106720.0,"strokes":[{"time":1791106720000,"lat":35.3,"lon":24.5,"src":2,"id":1,"del":1800,"dev":2000}]}`,
		},
		FrameDelay:       10 * time.Millisecond,
		CloseAfterFrames: true,
	})

	// A ceiling of 10s against a 10ms floor. The reset is asserted through the
	// observable gap between reconnects: after a healthy session the client must
	// come back quickly rather than waiting out the ceiling, which is the
	// behaviour that lets a process survive weeks of intermittent drops.
	c := upstream.New(upstream.Options{
		URL:              srv.URL(),
		HandshakeTimeout: 5 * time.Second,
		IdleTimeout:      500 * time.Millisecond,
		ReconnectMin:     10 * time.Millisecond,
		ReconnectMax:     10 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = c.Run(ctx, func(context.Context, model.Stroke) error { return nil })
	}()

	// Let the client accumulate a few reconnects.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && c.Reconnects() < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	if c.Reconnects() < 3 {
		t.Fatalf("the client stopped reconnecting after %d attempts", c.Reconnects())
	}

	// Measure how long the next few reconnects take together. With the counter
	// saturating at the ceiling, each would wait seconds; with the reset, they
	// come back at the floor.
	before := c.Reconnects()
	start := time.Now()
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && c.Reconnects() < before+3 {
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(start)

	if got := c.Reconnects(); got < before+3 {
		t.Fatalf("only %d further reconnect(s) in %v", got-before, elapsed)
	}
	// Three reconnects at the 10ms floor take well under a second. Waiting the
	// ceiling three times would take half a minute.
	if elapsed > 3*time.Second {
		t.Errorf("three reconnects took %v with a %v ceiling and a %v floor; "+
			"the backoff is not resetting after healthy sessions",
			elapsed, 10*time.Second, 10*time.Millisecond)
	}
}

// The cursor must only ever move forward. The upstream replays history on every
// connect, so adopting a replayed stroke's lower id would rewind the cursor and
// make the next resume replay that window again.
func TestTheResumeCursorNeverRewinds(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		FramesPerConnection: true,
		// Ascending ids within a session; the replay of the whole script on the
		// next connection presents them all again, lowest first.
		Frames: []string{
			`{"time":1791106720.0,"strokes":[` +
				`{"time":1791106720000,"lat":35.3,"lon":24.5,"src":2,"id":10,"del":1800,"dev":2000},` +
				`{"time":1791106721000,"lat":35.31,"lon":24.51,"src":2,"id":20,"del":1800,"dev":2000},` +
				`{"time":1791106722000,"lat":35.32,"lon":24.52,"src":2,"id":30,"del":1800,"dev":2000}]}`,
		},
		FrameDelay:       10 * time.Millisecond,
		CloseAfterFrames: true,
	})

	c := upstream.New(upstream.Options{
		URL:              srv.URL(),
		HandshakeTimeout: 5 * time.Second,
		IdleTimeout:      500 * time.Millisecond,
		ReconnectMin:     20 * time.Millisecond,
		ReconnectMax:     50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = c.Run(ctx, func(context.Context, model.Stroke) error { return nil })

	// The cursor is recorded before the sink is called, so the highest id in the
	// script is what must be held.
	if got := c.LastSeen(model.SourceLightningMaps); got != 30 {
		t.Errorf("LastSeen = %d, want 30; a replayed lower id must not rewind the cursor", got)
	}
}
