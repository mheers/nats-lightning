// Package ingest's tests exist to hold the pipeline's seams shut.
//
// The wiring in this package had no tests at all, which is how four defects
// survived a green suite: a dedup layer that was defined, unit-tested, and never
// called; a connection check that reported success while disconnected; four
// metrics that were built and then dropped without being registered; and a
// duplicated dist assignment. Each is asserted here, against a real embedded
// NATS and a real WebSocket, because the assertions are about behaviour and
// neither the metrics exposition nor the archive contents can be faked usefully.
package ingest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/heers-it/lightningfeed/internal/config"
	"github.com/heers-it/lightningfeed/internal/geo"
	"github.com/heers-it/lightningfeed/internal/model"
	"github.com/heers-it/lightningfeed/internal/store"
	"github.com/heers-it/lightningfeed/internal/testsupport/fakews"
	"github.com/heers-it/lightningfeed/internal/testsupport/natsd"
)

// Roussospiti, Crete: the region this project is deployed for.
const (
	regionLat    = 35.3340688
	regionLon    = 24.4944483
	regionRadius = 10.0
)

// batch renders one upstream frame carrying a stroke inside the region and one
// far outside it, both stamped `age` in the past.
//
// This is the shape the upstream actually sends, and the reason it matters is
// that it replays it: a frame like this arrives again on every connection.
func batch(age time.Duration) string {
	now := time.Now().Add(-age)
	return fmt.Sprintf(
		`{"time":%g,"strokes":[`+
			`{"time":%d,"lat":%v,"lon":%v,"src":2,"id":1,"del":1800,"dev":2000},`+
			`{"time":%d,"lat":52.5200,"lon":13.4050,"src":2,"id":2,"del":1800,"dev":2000}]}`,
		float64(now.UnixMilli())/1000,
		now.Add(-time.Second).UnixMilli(), regionLat, regionLon,
		now.UnixMilli())
}

// testConfig is a configuration a test can actually run.
//
// Two things matter here. The metrics port is 0, so the kernel chooses one and
// the resolved address has to come from Runner rather than from the config —
// there is no other way to learn it. And leadership is off, because these tests
// are about the pipeline's own wiring and the lease is exercised in its own
// package; leaving it on would make every test contend for one key.
func testConfig(t *testing.T, natsURL, upstreamURL string) *config.Config {
	t.Helper()
	return &config.Config{
		NATSURL:          natsURL,
		Stream:           "LIGHTNING",
		BoundaryPolicy:   geo.PolicyInclude,
		UpstreamURL:      upstreamURL,
		SourceMask:       4, // MaskLightningMaps
		ReconnectMin:     15 * time.Second,
		ReconnectMax:     time.Minute,
		IdleTimeout:      2 * time.Second,
		HandshakeTimeout: 5 * time.Second,
		BackfillDrop:     5 * time.Minute,
		DedupeTTL:        10 * time.Minute,
		SQLitePath:       t.TempDir() + "/lightningfeed.db",
		Retention:        7 * 24 * time.Hour,
		PruneEvery:       time.Hour,
		PublishAttempts:  3,
		PublishBackoff:   10 * time.Millisecond,
		MetricsAddr:      "127.0.0.1:0",
		LogLevel:         -1, // discard; these tests assert behaviour, not logs
		LogFormat:        "json",
		LeaderElect:      false,
	}
}

// launch starts the pipeline and returns a harness pointed at its resolved
// metrics address.
//
// The runner's address is used rather than the configured one because with a port
// of 0 the two differ and only the resolved address can be connected to. The
// pipeline is cancelled when the test ends.
func launch(t *testing.T, cfg *config.Config, up *fakews.Server) *harness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	// RunWith blocks until the pipeline stops, and the metrics address is only
	// knowable once it has bound, so the ready callback is how the test learns
	// where to scrape. It arrives well before the first stroke.
	runnerCh := make(chan *Runner, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunWith(ctx, cfg, func(r *Runner) { runnerCh <- r })
	}()

	var runner *Runner
	select {
	case runner = <-runnerCh:
	case err := <-errCh:
		cancel()
		t.Fatalf("starting the pipeline: %v", err)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("the pipeline never reported a metrics address")
	}

	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
			// A cancellation is the expected ending.
		case <-time.After(15 * time.Second):
			t.Error("the pipeline did not stop after its context was cancelled")
		}
	})

	h := &harness{cfg: cfg, up: up, base: "http://" + runner.MetricsAddr}
	h.waitReady(t)
	return h
}

// harness is a running pipeline plus the handles a test needs.
type harness struct {
	cfg   *config.Config
	up    *fakews.Server
	store *store.Store
	base  string // resolved metrics base URL, e.g. http://127.0.0.1:34567
}

// start runs the pipeline against a fake upstream and an embedded NATS.
//
// The fake replays its batch on every connection, because that is the upstream's
// documented behaviour and a fake that sends each frame once cannot tell a
// pipeline that suppresses replays from one that does not.
func start(t *testing.T, region bool) *harness {
	t.Helper()

	up := fakews.Start(t, &fakews.Server{
		FramesPerConnection: true,
		Frames:              []string{batch(2 * time.Second)},
		FrameDelay:          20 * time.Millisecond,
		HeartbeatForever:    true,
	})
	ns := natsd.Start(t)

	cfg := testConfig(t, ns.URL, up.URL())
	if region {
		cfg.RegionEnabled = true
		cfg.RegionName = "roussospiti"
		cfg.Region = geo.Circle{Lat: regionLat, Lon: regionLon, RadiusKm: regionRadius}
	}

	db := openStore(t, cfg.SQLitePath)
	t.Cleanup(func() { db.Close() })

	h := launch(t, cfg, up)
	h.store = db
	return h
}

// waitReady blocks until the health endpoint reports ready.
//
// Readiness is the right gate for the assertions that follow: they are all about
// a working pipeline, and a metric read before the first stroke could pass simply
// because nothing had happened yet.
func (h *harness) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		body, code, err := h.get("/healthz")
		if err == nil && code == http.StatusOK && strings.Contains(body, `"ready":true`) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the pipeline never reported ready")
}

// get fetches a path from the metrics server.
func (h *harness) get(path string) (string, int, error) {
	resp, err := http.Get(h.base + path)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode, err
}

// scrape returns the metrics exposition.
func (h *harness) scrape(t *testing.T) string {
	t.Helper()
	body, code, err := h.get("/metrics")
	if err != nil {
		t.Fatalf("scraping metrics: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", code)
	}
	return body
}

// waitFor polls until cond holds or the deadline passes, returning whether it did.
func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

// expositionLines splits a scrape into its sample lines.
func expositionLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		out = append(out, l)
	}
	return out
}

// hasFamily reports whether a metric family appears at all.
//
// Presence is the whole assertion for the gauges built from collaborators. An
// unregistered collector exports nothing whatsoever and no scrape reveals that it
// was meant to be there, which is exactly how four documented metrics went
// missing while the source still looked correctly wired.
func hasFamily(s, name string) bool {
	for _, l := range expositionLines(s) {
		if strings.HasPrefix(l, name) {
			return true
		}
	}
	return false
}

// familyValue returns the first sample value of a family, matching on the name
// only so a caller need not spell out every label.
func familyValue(s, name string) (float64, bool) {
	for _, l := range expositionLines(s) {
		if !strings.HasPrefix(l, name) {
			continue
		}
		fields := strings.Fields(l)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		return v, true
	}
	return 0, false
}

// seriesTotal sums every sample whose series begins with prefix.
func seriesTotal(s, prefix string) float64 {
	total := 0.0
	for _, l := range expositionLines(s) {
		if !strings.HasPrefix(l, prefix+" ") {
			continue
		}
		fields := strings.Fields(l)
		if len(fields) != 2 {
			continue
		}
		if v, err := strconv.ParseFloat(fields[1], 64); err == nil {
			total += v
		}
	}
	return total
}

// Every metric the README names must be exported. Four of them were built and
// then never registered, so the documented alert on
// upstream_last_message_age_seconds could never fire and the archive-depth gauge
// reported nothing at all.
func TestExportsEveryDocumentedMetric(t *testing.T) {
	h := start(t, true)
	exposition := h.scrape(t)

	for _, name := range []string{
		"lightningfeed_upstream_last_message_age_seconds",
		"lightningfeed_store_rows",
		"lightningfeed_config_info",
		"lightningfeed_upstream_connected",
		"lightningfeed_leader",
		"lightningfeed_dropped_total",
		"lightningfeed_published_total",
		"lightningfeed_strokes_total",
		"lightningfeed_publish_lag_ms",
	} {
		if !hasFamily(exposition, name) {
			t.Errorf("%s is not exported; a documented alert on it can never fire", name)
		}
	}
}

// The age gauge is what separates a quiet feed from a dead one, so while frames
// are arriving it must report a plausible number rather than the sentinel that
// stands for "not connected".
func TestLastMessageAgeIsLiveWhileConnected(t *testing.T) {
	h := start(t, true)

	age, ok := familyValue(h.scrape(t), "lightningfeed_upstream_last_message_age_seconds")
	if !ok {
		t.Fatal("the last-message-age gauge is not exported")
	}
	if age >= disconnectedSentinel {
		t.Errorf("last-message age is the disconnected sentinel (%v) on a ready pipeline", age)
	}
	// The fake heartbeats every 20ms; anything beyond a second would mean the
	// gauge is reporting something other than recency.
	if age > 5 {
		t.Errorf("last-message age is %v s, far longer than frames are arriving", age)
	}
}

// disconnectedSentinel mirrors the value obs uses for "not connected". It is
// restated here so the test does not depend on obs internals.
const disconnectedSentinel = 1e9

// The connected gauge was set once at startup and never again, so it read zero
// for the entire life of a healthy process.
//
// It is driven by a poll rather than by the connection event itself, so the
// assertion waits for the poll rather than assuming an instantaneous update.
func TestUpstreamConnectedGaugeReportsTheLiveConnection(t *testing.T) {
	h := start(t, true)

	ok := waitFor(10*time.Second, func() bool {
		v, found := familyValue(h.scrape(t), "lightningfeed_upstream_connected")
		return found && v == 1
	})
	if !ok {
		v, found := familyValue(h.scrape(t), "lightningfeed_upstream_connected")
		t.Errorf("lightningfeed_upstream_connected = %v (exported=%v), want 1 on a ready pipeline", v, found)
	}
}

// The store gauge counted rows correctly and was then never registered.
func TestStoreRowsGaugeReportsArchiveDepth(t *testing.T) {
	h := start(t, true)

	ok := waitFor(10*time.Second, func() bool {
		v, found := familyValue(h.scrape(t), "lightningfeed_store_rows")
		return found && v >= 1
	})
	if !ok {
		t.Error("lightningfeed_store_rows never reported a row, though strokes are being archived")
	}
}

// The region info gauge is a GaugeVec with a region label, and a GaugeVec exports
// nothing until that label is observed. It was registered and never labelled.
func TestConfigInfoCarriesTheConfiguredRegion(t *testing.T) {
	h := start(t, true)

	exposition := h.scrape(t)
	if !hasFamily(exposition, `lightningfeed_config_info{region="roussospiti"}`) {
		t.Errorf("lightningfeed_config_info does not carry the configured region; scrape had:\n%s",
			grepFamily(exposition, "lightningfeed_config_info"))
	}
}

// grepFamily returns the sample lines of a family, for failure messages.
func grepFamily(s, name string) string {
	var out []string
	for _, l := range expositionLines(s) {
		if strings.HasPrefix(l, name) {
			out = append(out, "  "+l)
		}
	}
	if len(out) == 0 {
		return "  <no samples>"
	}
	return strings.Join(out, "\n")
}

// A region deployment archives the strokes inside it and nothing else, even
// though the upstream's own filter is loose enough to deliver plenty from
// elsewhere.
func TestRegionDeploymentArchivesOnlyStrokesInsideTheRegion(t *testing.T) {
	h := start(t, true)

	if !waitFor(10*time.Second, func() bool {
		n, err := h.store.Count(context.Background(), time.Time{})
		return err == nil && n > 0
	}) {
		t.Fatal("no strokes were archived")
	}

	strokes, err := h.store.StrokesSince(context.Background(), time.Now().Add(-time.Hour), 1000)
	if err != nil {
		t.Fatalf("reading strokes: %v", err)
	}
	if len(strokes) == 0 {
		t.Fatal("counted rows but read none")
	}
	for _, s := range strokes {
		if !h.cfg.Region.ContainsPoint(s.Lat, s.Lon) {
			t.Errorf("archived a stroke at %v,%v, which is %v km from the region centre",
				s.Lat, s.Lon, geo.HaversineKm(regionLat, regionLon, s.Lat, s.Lon))
		}
	}
}

// Berlin must be nowhere near Roussospiti, so its absence is a real check on the
// filter rather than an artefact of an empty result set.
func TestStrokesFarOutsideTheRegionAreDropped(t *testing.T) {
	h := start(t, true)

	ok := waitFor(10*time.Second, func() bool {
		return seriesTotal(h.scrape(t), `lightningfeed_dropped_total{reason="outside_region"}`) > 0
	})
	if !ok {
		t.Error("no stroke was dropped as outside_region, though the upstream sends one from Berlin")
	}
}

// The upstream replays its history on every connection, so a cold start
// republishes five minutes of already-seen lightning as if it were live. The
// first of the four suppression layers exists for exactly this and was never
// armed: StartConnection was defined, unit-tested in isolation, and called from
// nowhere in the pipeline.
func TestBackfillWindowSuppressesTheStartupReplay(t *testing.T) {
	// A stroke from before the backfill window opens. On a cold start the
	// identity filter has never seen it, so only the backfill window can drop
	// it.
	replayed := model.Stroke{
		Src:      model.SourceLightningMaps,
		StrokeID: 4242,
		Time:     time.Now().Add(-5 * time.Minute),
		Lat:      regionLat,
		Lon:      regionLon,
	}

	up := fakews.Start(t, &fakews.Server{
		Frames: []string{fmt.Sprintf(
			`{"time":%g,"strokes":[{"time":%d,"lat":%v,"lon":%v,"src":2,"id":4242,"del":1800,"dev":2000}]}`,
			float64(time.Now().Add(-5*time.Minute).UnixMilli())/1000,
			replayed.Time.UnixMilli(), regionLat, regionLon)},
		FrameDelay:       20 * time.Millisecond,
		HeartbeatForever: true,
	})
	ns := natsd.Start(t)

	cfg := testConfig(t, ns.URL, up.URL())
	cfg.RegionEnabled = true
	cfg.RegionName = "roussospiti"
	cfg.Region = geo.Circle{Lat: regionLat, Lon: regionLon, RadiusKm: regionRadius}

	h := launch(t, cfg, up)

	// The stroke is inside the region, so it would be published if the backfill
	// window were not suppressing it.
	ok := waitFor(5*time.Second, func() bool {
		return seriesTotal(h.scrape(t), `lightningfeed_dropped_total{reason="replay"}`) > 0
	})
	if !ok {
		t.Error("the replayed history was not dropped; the backfill window is not being armed on connect")
	}
	if got := seriesTotal(h.scrape(t), `lightningfeed_published_total{result="ok"}`); got > 0 {
		t.Errorf("published %v stroke(s) that predated the connection; a cold start must not republish history", got)
	}
}

// The identity filter is the second layer and it only helps once the process has
// seen a stroke. The backfill window has to open per session, not once at
// startup, or a reconnect's replay reaches the filter as its first sight of those
// ids.
func TestBackfillWindowIsRearmedOnEverySession(t *testing.T) {
	// The fake closes each connection after its scripted frames, so the client
	// reconnects and the same batch is replayed. That is the upstream's real
	// behaviour and the only way to exercise a second session.
	up := fakews.Start(t, &fakews.Server{
		FramesPerConnection: true,
		Frames:              []string{batch(2 * time.Second)},
		FrameDelay:          20 * time.Millisecond,
		CloseAfterFrames:    true,
	})
	ns := natsd.Start(t)

	cfg := testConfig(t, ns.URL, up.URL())
	cfg.ReconnectMin = 50 * time.Millisecond
	cfg.RegionEnabled = true
	cfg.RegionName = "roussospiti"
	cfg.Region = geo.Circle{Lat: regionLat, Lon: regionLon, RadiusKm: regionRadius}

	db := openStore(t, cfg.SQLitePath)
	t.Cleanup(func() { db.Close() })

	h := launch(t, cfg, up)
	h.store = db

	// The reconnect floor is deliberately short here rather than the measured
	// fifteen seconds: the floor exists to respect the live upstream's
	// throttling, and this fake has no such limit. Waiting it out would make the
	// test take half a minute to assert nothing.
	if !waitFor(20*time.Second, func() bool { return h.up.ConnectionCount() >= 2 }) {
		t.Fatalf("the pipeline did not reconnect; saw %d connection(s)", h.up.ConnectionCount())
	}

	before := seriesTotal(h.scrape(t), `lightningfeed_published_total{result="ok"}`)
	time.Sleep(2 * time.Second) // let the replay arrive and be judged
	if after := seriesTotal(h.scrape(t), `lightningfeed_published_total{result="ok"}`); after > before {
		t.Errorf("published_total rose from %v to %v across a reconnect that replayed the same strokes",
			before, after)
	}
}

// A cursor is only meaningful against the upstream that issued its ids. The two
// servers run independent sequences — the same minute showed a last id near
// 1.48M on one and near 17.8M on the other — so resuming across that boundary
// republishes history as live data. The stored server must be compared, not just
// logged.
func TestRefusesToResumeACursorFromAnotherUpstream(t *testing.T) {
	up := fakews.Start(t, &fakews.Server{HeartbeatForever: true, FrameDelay: 20 * time.Millisecond})
	ns := natsd.Start(t)

	path := t.TempDir() + "/lightningfeed.db"

	// A cursor recorded against the other upstream. Its ids mean nothing here.
	saveCursor(t, path, "wss://live2.lightningmaps.org:443/",
		map[model.Source]int64{model.SourceLightningMaps: 17836865})

	cfg := testConfig(t, ns.URL, up.URL())
	cfg.SQLitePath = path

	launch(t, cfg, up)

	// The cursor is for live2, so nothing about it may be sent upstream as a
	// resume hint.
	for i, raw := range up.Subscribes() {
		if strings.Contains(string(raw), "17836865") {
			t.Errorf("subscribe frame %d carried another upstream's id: %s", i, raw)
		}
	}
}

// The matching case: a cursor recorded against the upstream being connected to
// is a genuine resume and must be used. Without this the guard above would be
// satisfied by simply never resuming anything.
func TestResumesACursorFromTheSameUpstream(t *testing.T) {
	up := fakews.Start(t, &fakews.Server{HeartbeatForever: true, FrameDelay: 20 * time.Millisecond})
	ns := natsd.Start(t)

	path := t.TempDir() + "/lightningfeed.db"

	// Recorded against this very server, so it is a real resume rather than a
	// cross-server mismatch. Without this the guard above would be satisfied by
	// simply never resuming anything.
	saveCursor(t, path, up.URL(), map[model.Source]int64{model.SourceLightningMaps: 17836865})

	cfg := testConfig(t, ns.URL, up.URL())
	cfg.SQLitePath = path

	launch(t, cfg, up)

	subscribes := up.Subscribes()
	if len(subscribes) == 0 {
		t.Fatal("no subscribe frame reached the upstream")
	}
	if !strings.Contains(string(subscribes[0]), "17836865") {
		t.Errorf("the subscribe frame did not carry the stored cursor: %s", subscribes[0])
	}
}

// saveCursor records a resume cursor against a database at path.
func saveCursor(t *testing.T, path, server string, sources map[model.Source]int64) {
	t.Helper()
	db := openStore(t, path)
	defer db.Close()
	if err := db.SaveCursor(context.Background(), store.Cursor{Sources: sources, Server: server}); err != nil {
		t.Fatalf("saving cursor: %v", err)
	}
}

// The health endpoint reports readiness from the client's connection state. That
// state used to be derived from the last message time, which stayed set through
// every failed reconnect — and a silent dial hang is this upstream's dominant
// failure mode — so readiness held through exactly the outage it exists to catch.
func TestHealthReportsNotReadyWhileDisconnected(t *testing.T) {
	// A pipeline pointed at a server that accepts the socket but never answers
	// the subscribe. The first session then ends and the client sleeps for
	// ReconnectMin, which is where the old implementation kept reporting itself
	// connected.
	dead := fakews.Start(t, &fakews.Server{NoHello: true})
	ns := natsd.Start(t)

	cfg := testConfig(t, ns.URL, dead.URL())
	cfg.IdleTimeout = 500 * time.Millisecond
	cfg.HandshakeTimeout = 400 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runnerCh := make(chan *Runner, 1)
	go func() { _ = RunWith(ctx, cfg, func(r *Runner) { runnerCh <- r }) }()

	var runner *Runner
	select {
	case runner = <-runnerCh:
	case <-time.After(30 * time.Second):
		t.Fatal("the pipeline never reported a metrics address")
	}

	stuck := &harness{base: "http://" + runner.MetricsAddr}

	ok := waitFor(20*time.Second, func() bool {
		body, code, err := stuck.get("/healthz")
		return err == nil && code == http.StatusServiceUnavailable &&
			strings.Contains(body, `"ready":false`)
	})
	if !ok {
		body, code, _ := stuck.get("/healthz")
		t.Errorf("health reported ready (code %d) while the upstream never handshook:\n%s", code, body)
	}
}

// openStore opens a database for inspection.
func openStore(t *testing.T, path string) *store.Store {
	t.Helper()
	db, err := store.Open(path)
	if err != nil {
		t.Fatalf("opening the store at %s: %v", path, err)
	}
	return db
}
