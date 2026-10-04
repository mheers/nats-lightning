package upstream_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/heers-it/lightningfeed/internal/geo"
	"github.com/heers-it/lightningfeed/internal/model"
	"github.com/heers-it/lightningfeed/internal/testsupport/fakews"
	"github.com/heers-it/lightningfeed/internal/upstream"
)

// Slice 8: the client, exercised against a real WebSocket server.
//
// The seam is the connection: given a scripted upstream, does the client send a
// frame the upstream will accept, and does it deliver exactly the strokes the
// upstream sent? Reconnect timing is driven by an injected backoff so the tests
// assert the policy rather than waiting it out.

const testBackoff = 10 * time.Millisecond

// strokeFrame builds a stroke batch frame.
func strokeFrame(strokes ...string) string {
	return fmt.Sprintf(`{"time":1791106720,"flags":{"2":0},"strokes":[%s]}`,
		joinComma(strokes))
}

func oneStroke(id int, lat, lon string) string {
	return fmt.Sprintf(`{"time":1791106639235,"lat":%s,"lon":%s,"src":2,"srv":1,"id":%d,"del":1711,"dev":4556}`,
		lat, lon, id)
}

func joinComma(items []string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

// collector accumulates strokes from a sink, thread-safely.
type collector struct {
	mu      sync.Mutex
	strokes []model.Stroke
}

func (c *collector) sink(_ context.Context, s model.Stroke) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.strokes = append(c.strokes, s)
	return nil
}

func (c *collector) snapshot() []model.Stroke {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.Stroke, len(c.strokes))
	copy(out, c.strokes)
	return out
}

func (c *collector) waitFor(t *testing.T, n int, within time.Duration) []model.Stroke {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if got := c.snapshot(); len(got) >= n {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
	got := c.snapshot()
	t.Fatalf("timed out after %v waiting for %d strokes, got %d", within, n, len(got))
	return nil
}

// runUntilStrokes starts the client, waits for n strokes to arrive, then cancels.
// Without this, a test that has already proved its point would sit idle until
// its context expired, which dominated the package's runtime.
func runUntilStrokes(t *testing.T, c *upstream.Client, got *collector, n int, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), within)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, got.sink) }()

	got.waitFor(t, n, within-100*time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not shut down after cancellation")
	}
}

func newTestClient(t *testing.T, url string, mutate func(*upstream.Options)) *upstream.Client {
	t.Helper()
	opts := upstream.Options{
		URL:        url,
		SourceMask: upstream.DefaultSrcMask,
		Viewport:   viewportOfTheRegion,
		// A short floor keeps the reconnect policy testable in milliseconds.
		ReconnectMin:     testBackoff,
		ReconnectMax:     4 * testBackoff,
		IdleTimeout:      200 * time.Millisecond,
		HandshakeTimeout: 5 * time.Second,
	}
	if mutate != nil {
		mutate(&opts)
	}
	return upstream.New(opts)
}

func TestClientDeliversStrokesFromAConnectedUpstream(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		Frames: []string{
			strokeFrame(oneStroke(1940052, "35.593153", "26.354258")),
			strokeFrame(
				oneStroke(1940053, "35.643531", "26.349389"),
				oneStroke(1940054, "35.732741", "29.146832"),
			),
		},
		FrameDelay:       5 * time.Millisecond,
		HeartbeatForever: true,
	})

	c := newTestClient(t, srv.URL(), nil)
	got := &collector{}

	runUntilStrokes(t, c, got, 3, 5*time.Second)

	strokes := got.snapshot()
	if len(strokes) != 3 {
		t.Fatalf("got %d strokes, want 3", len(strokes))
	}

	if strokes[0].StrokeID != 1940052 {
		t.Errorf("first stroke id = %d, want 1940052", strokes[0].StrokeID)
	}
	if strokes[0].Src != model.SourceLightningMaps {
		t.Errorf("first stroke src = %v, want %v", strokes[0].Src, model.SourceLightningMaps)
	}
	if strokes[0].Network() != "lightningmaps.org" {
		t.Errorf("network = %q, want lightningmaps.org", strokes[0].Network())
	}
	// Order must be preserved: the upstream's sequence is the only ordering
	// information available for reconstructing a storm's development.
	if strokes[1].StrokeID != 1940053 || strokes[2].StrokeID != 1940054 {
		t.Errorf("strokes arrived out of order: %d, %d",
			strokes[1].StrokeID, strokes[2].StrokeID)
	}
}

// The upstream silently drops any connection whose subscribe frame lacks either
// the version or the last-seen id map. Both fields were verified mandatory by
// bisecting the frame one key at a time.
func TestClientSendsASubscribeFrameTheUpstreamAccepts(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{HangAfterFrames: true})

	c := newTestClient(t, srv.URL(), nil)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx, (&collector{}).sink)

	subs := srv.Subscribes()
	if len(subs) == 0 {
		t.Fatal("client never sent a subscribe frame")
	}

	var frame map[string]any
	if err := json.Unmarshal(subs[0], &frame); err != nil {
		t.Fatalf("subscribe frame is not valid JSON: %v", err)
	}

	// The two fields without which the upstream closes the connection.
	if v, ok := frame["v"]; !ok {
		t.Error(`subscribe frame is missing "v"; the upstream drops such connections`)
	} else if got, want := v.(float64), float64(upstream.ProtocolVersion); got != want {
		t.Errorf("subscribe v = %v, want %v", got, want)
	}
	if _, ok := frame["i"]; !ok {
		t.Error(`subscribe frame is missing "i"; the upstream drops such connections`)
	}

	// The viewport must carry the configured region.
	p, ok := frame["p"].([]any)
	if !ok || len(p) != 4 {
		t.Fatalf("subscribe viewport = %v, want four numbers", frame["p"])
	}
	if got, want := p[0].(float64), viewportOfTheRegion.North; got != want {
		t.Errorf("viewport north edge = %v, want %v", got, want)
	}

	// Station data must stay off: it inflates each stroke by roughly 36 times.
	if s, ok := frame["s"]; ok && s == true {
		t.Error(`subscribe frame requests station data ("s": true); that inflates payload ~36x`)
	}
}

// Heartbeats arrive roughly every ten seconds and carry no strokes. They must
// keep the connection alive without being mistaken for data.
func TestClientSurvivesHeartbeats(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		Frames: []string{
			strokeFrame(oneStroke(1, "35.5", "26.3")),
			`{"time":1791106736.835}`,
			`{"time":1791106746.837}`,
			strokeFrame(oneStroke(2, "35.6", "26.4")),
		},
		FrameDelay:       5 * time.Millisecond,
		HeartbeatForever: true,
	})

	c := newTestClient(t, srv.URL(), nil)
	got := &collector{}

	runUntilStrokes(t, c, got, 2, 5*time.Second)

	strokes := got.snapshot()
	if len(strokes) != 2 {
		t.Fatalf("got %d strokes, want exactly 2 (heartbeats must not become strokes)", len(strokes))
	}
}

// One unparseable frame must not end the session. Treating a bad frame as fatal
// would turn a momentary upstream glitch into an outage.
func TestClientKeepsRunningAfterAMalformedFrame(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		Frames: []string{
			strokeFrame(oneStroke(1, "35.5", "26.3")),
			`{"time":1791106720,"strokes":[`, // truncated
			`this is not json at all`,        // garbage
			strokeFrame(oneStroke(2, "35.6", "26.4")),
		},
		FrameDelay:       5 * time.Millisecond,
		HeartbeatForever: true,
	})

	c := newTestClient(t, srv.URL(), nil)
	got := &collector{}

	runUntilStrokes(t, c, got, 2, 5*time.Second)

	strokes := got.snapshot()
	if strokes[len(strokes)-1].StrokeID != 2 {
		t.Errorf("last delivered stroke = %d, want 2 (the frame after the bad ones)",
			strokes[len(strokes)-1].StrokeID)
	}
	if m := c.MalformedFrames(); m < 2 {
		t.Errorf("malformed frame count = %d, want at least 2", m)
	}
}

// The upstream drops connections for its own reasons. The client must reconnect
// rather than exit, and must not spin doing so.
func TestClientReconnectsAfterTheUpstreamClosesTheConnection(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		Frames:           []string{strokeFrame(oneStroke(7, "35.5", "26.3"))},
		CloseAfterFrames: true,
	})

	c := newTestClient(t, srv.URL(), nil)
	got := &collector{}

	runUntilStrokes(t, c, got, 2, 3*time.Second)

	if conns := srv.Connections(); conns < 2 {
		t.Errorf("upstream saw %d connections, want at least 2 (no reconnect)", conns)
	}
	if n := c.Reconnects(); n < 1 {
		t.Errorf("reconnect count = %d, want at least 1", n)
	}
}

// The upstream throttles new connections severely: measured 0/6 success with no
// delay between attempts, 2/6 at five seconds, 6/6 at fifteen. The production
// floor is therefore fifteen seconds; the test injects a shorter one so the
// policy can be asserted without waiting.
func TestClientWaitsAtLeastTheConfiguredMinimumBetweenAttempts(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		Frames:           nil,
		CloseAfterFrames: true,
	})

	c := newTestClient(t, srv.URL(), func(o *upstream.Options) {
		o.ReconnectMin = 60 * time.Millisecond
		o.ReconnectMax = 60 * time.Millisecond
	})

	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx, (&collector{}).sink)

	conns := srv.Connections()
	// Over 400ms with a 60ms floor, roughly six attempts are possible; anything
	// far above that means the backoff is being ignored.
	if conns > 12 {
		t.Errorf("upstream saw %d connections in 400ms with a 60ms floor; "+
			"the reconnect delay is not being applied", conns)
	}
	if conns < 2 {
		t.Errorf("upstream saw only %d connections; expected retries", conns)
	}
}

// A connection can go silent without closing. The idle watchdog has to notice,
// or the bridge would sit publishing nothing while appearing healthy.
func TestClientGivesUpOnASilentConnection(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		Frames:          nil,
		HangAfterFrames: true,
	})

	c := newTestClient(t, srv.URL(), nil)

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_ = c.Run(ctx, (&collector{}).sink)

	if conns := srv.Connections(); conns < 2 {
		t.Errorf("upstream saw %d connections, want at least 2: "+
			"a silent connection should trigger the idle watchdog", conns)
	}
}

// An upstream that never answers the subscribe must not wedge the client.
func TestClientGivesUpWhenTheUpstreamNeverAnswers(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		NoHello:         true,
		HangAfterFrames: true,
	})

	c := newTestClient(t, srv.URL(), func(o *upstream.Options) {
		o.HandshakeTimeout = 50 * time.Millisecond
	})

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_ = c.Run(ctx, (&collector{}).sink)

	if conns := srv.Connections(); conns < 2 {
		t.Errorf("upstream saw %d connections, want at least 2: "+
			"a missing hello should trigger a reconnect", conns)
	}
}

// A sink failure is a downstream problem (a full queue, a broker refusing
// writes). Spinning against it would hammer both ends, so the client must stop.
func TestClientStopsWhenTheSinkFails(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		Frames: []string{strokeFrame(
			oneStroke(1, "35.5", "26.3"),
			oneStroke(2, "35.6", "26.4"),
		)},
		FrameDelay:       5 * time.Millisecond,
		HeartbeatForever: true,
	})

	c := newTestClient(t, srv.URL(), nil)

	wantErr := errors.New("sink is full")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := c.Run(ctx, func(context.Context, model.Stroke) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run returned %v, want %v", err, wantErr)
	}
}

// Shutdown must be prompt. The ingest pipeline wraps this in a signal handler,
// and a client that takes its whole backoff to notice cancellation would make
// restarts feel broken.
func TestClientStopsPromptlyOnContextCancellation(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{HangAfterFrames: true})

	c := newTestClient(t, srv.URL(), nil)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, (&collector{}).sink) }()

	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("shutdown took %v, want under 2s", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s of cancellation")
	}
}

// Roussospiti, Crete — the configured region.
const (
	roussospitiLat = 35.3340688
	roussospitiLon = 24.4944483
	regionRadiusKm = 10.0
)

// viewportOfTheRegion is the region's bounding box in the upstream's own
// viewport form, derived from the same geo package production uses.
var viewportOfTheRegion = func() upstream.Viewport {
	b := geo.Circle{Lat: roussospitiLat, Lon: roussospitiLon, RadiusKm: regionRadiusKm}.BoundingBox()
	return upstream.Viewport{North: b.MaxLat, East: b.MaxLon, South: b.MinLat, West: b.MinLon}
}()
