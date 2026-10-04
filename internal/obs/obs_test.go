package obs

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This package had no tests, which is how two separate defects survived a green
// suite: collectors were built and never registered, so they exported nothing at
// all while looking correctly wired in the source, and the counters that report
// upstream totals took a delta but added one per report, understating bursts.
//
// The assertions scrape the real exposition rather than reading fields, because
// "registered" is precisely the property that was broken. A test that inspected the
// struct would have passed against every one of these bugs.

// scrape returns the exposition body.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler returned %d", rec.Code)
	}
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// sample returns the value of a single series, failing if it is absent.
func sample(t *testing.T, exposition, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(exposition, "\n") {
		if !strings.HasPrefix(line, series+" ") {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatalf("parsing %q: %v", line, err)
		}
		return v
	}
	t.Fatalf("series %q absent from the exposition:\n%s", series, exposition)
	return 0
}

// present reports whether a series appears at all.
func present(exposition, series string) bool {
	return strings.Contains(exposition, "\n"+series+" ") ||
		strings.HasPrefix(exposition, series+" ")
}

// TestObserveReconnectAddsTheWholeDelta is the regression.
//
// The upstream reports monotonic totals and the counter wants increments, so the
// caller passes the delta. It used to take no count and increment by one, which
// understated a burst by exactly the factor that matters: reconnects arrive
// several at a time during an outage, which is the only moment anyone reads this.
func TestObserveReconnectAddsTheWholeDelta(t *testing.T) {
	m := NewMetrics("test")
	const series = `lightningfeed_reconnects_total{host="upstream",reason="session_ended"}`

	// The series is pre-initialised so the rate is computable from the first
	// scrape; recording must add to it rather than replace it.
	before := sample(t, scrape(t, m), series)

	m.ObserveReconnect("upstream", "session_ended", 5)
	if got, want := sample(t, scrape(t, m), series), before+5; got != want {
		t.Errorf("after reporting 5 reconnects: %v, want %v", got, want)
	}

	// A second, smaller delta accumulates rather than resetting.
	m.ObserveReconnect("upstream", "session_ended", 2)
	if got, want := sample(t, scrape(t, m), series), before+7; got != want {
		t.Errorf("after a further 2: %v, want %v", got, want)
	}
}

// TestObserveReconnectIgnoresNonPositiveDeltas guards against a caller computing a
// negative difference when a counter is reset, which would decrement a counter.
func TestObserveReconnectIgnoresNonPositiveDeltas(t *testing.T) {
	m := NewMetrics("test")
	const series = `lightningfeed_reconnects_total{host="upstream",reason="session_ended"}`

	before := sample(t, scrape(t, m), series)
	m.ObserveReconnect("upstream", "session_ended", 0)
	m.ObserveReconnect("upstream", "session_ended", -5)
	if got := sample(t, scrape(t, m), series); got != before {
		t.Errorf("counter moved to %v, want %v; a counter must never decrease", got, before)
	}
}

// TestMalformedAndRejectedCountDeltas checks the two counters that were already
// correct, so the reconnect fix cannot quietly change their behaviour.
func TestMalformedAndRejectedCountDeltas(t *testing.T) {
	m := NewMetrics("test")

	m.ObserveMalformedFrames(3)
	m.ObserveMalformedFrames(0)
	if got := sample(t, scrape(t, m), "lightningfeed_malformed_frames_total"); got != 3 {
		t.Errorf("malformed frames = %v, want 3", got)
	}

	m.ObserveRejectedStrokes(4)
	if got := sample(t, scrape(t, m), "lightningfeed_rejected_strokes_total"); got != 4 {
		t.Errorf("rejected strokes = %v, want 4", got)
	}
}

// TestStoreRowsGaugeIsInvisibleUntilRegistered is the defect that made the
// archive-depth gauge vanish from the exposition.
//
// Assigning the collector to a struct field is not enough: an unregistered
// collector exports nothing, which is exactly how a metric could be documented in
// the README and absent from /metrics.
func TestStoreRowsGaugeIsInvisibleUntilRegistered(t *testing.T) {
	m := NewMetrics("test")
	const series = "lightningfeed_store_rows"

	gauge := StoreRowsGauge(func() (int64, error) { return 42, nil })

	if present(scrape(t, m), series) {
		t.Error("store_rows was exported before it was registered")
	}

	m.Register(gauge)
	if got := sample(t, scrape(t, m), series); got != 42 {
		t.Errorf("store_rows = %v, want 42", got)
	}
}

// TestStoreRowsGaugeReportsFailureDistinctly checks that a failing count is not
// reported as zero, which would read as an empty archive rather than a broken one.
func TestStoreRowsGaugeReportsFailureDistinctly(t *testing.T) {
	m := NewMetrics("test")
	m.Register(StoreRowsGauge(func() (int64, error) { return 0, errors.New("disk I/O error") }))

	got := sample(t, scrape(t, m), "lightningfeed_store_rows")
	if got == 0 {
		t.Error("a failing count reported 0, which is indistinguishable from an empty archive")
	}
	if got != countFailedSentinel {
		t.Errorf("store_rows = %v, want the %v sentinel", got, countFailedSentinel)
	}
}

// TestLastMessageAgeGaugeSeparatesQuietFromDead is why connected and
// last-message-age are two signals rather than one.
//
// The gauge reports a sentinel whenever the connection is down or nothing has ever
// arrived. It must never report a growing age in that state, because a growing age
// is exactly what a dead connection looks like — reporting it would let a dead
// socket pass for a quiet region, which is the confusion that made the two signals
// worth separating.
func TestLastMessageAgeGaugeSeparatesQuietFromDead(t *testing.T) {
	const series = "lightningfeed_upstream_last_message_age_seconds"

	sentinelled := []struct {
		name        string
		lastMessage time.Time
		connected   bool
	}{
		{"disconnected and never heard from", time.Time{}, false},
		{"disconnected but a message once arrived", time.Now().Add(-time.Hour), false},
		// Connected but nothing has ever arrived is still unknown, not "0 seconds
		// old", which would look like a frame arriving continuously.
		{"connected but nothing has arrived yet", time.Time{}, true},
	}

	for _, tc := range sentinelled {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMetrics("test")
			m.Register(m.LastMessageAgeGauge(
				func() time.Time { return tc.lastMessage },
				func() bool { return tc.connected },
			))
			if got := sample(t, scrape(t, m), series); got != disconnectedSentinel {
				t.Errorf("age = %v, want the sentinel %v", got, disconnectedSentinel)
			}
		})
	}

	t.Run("connected and quiet reports the real age", func(t *testing.T) {
		m := NewMetrics("test")
		m.Register(m.LastMessageAgeGauge(
			func() time.Time { return time.Now().Add(-30 * time.Second) },
			func() bool { return true },
		))
		got := sample(t, scrape(t, m), series)
		if got < 29 || got > 31 {
			t.Errorf("age = %v, want about 30", got)
		}
	})
}

// TestConnectedAndLeaderGaugesAreSettable covers the point-in-time state that
// cannot be wired once at startup.
func TestConnectedAndLeaderGaugesAreSettable(t *testing.T) {
	m := NewMetrics("test")

	m.SetConnected(false)
	if got := sample(t, scrape(t, m), "lightningfeed_upstream_connected"); got != 0 {
		t.Errorf("connected = %v, want 0", got)
	}
	m.SetConnected(true)
	if got := sample(t, scrape(t, m), "lightningfeed_upstream_connected"); got != 1 {
		t.Errorf("connected = %v, want 1", got)
	}

	m.SetLeader(true)
	if got := sample(t, scrape(t, m), "lightningfeed_leader"); got != 1 {
		t.Errorf("leader = %v, want 1", got)
	}
	m.SetLeader(false)
	if got := sample(t, scrape(t, m), "lightningfeed_leader"); got != 0 {
		t.Errorf("leader = %v, want 0", got)
	}
}

// TestRegionInfoAppearsOnlyOnceSet covers the info metric, which a GaugeVec does
// not export until a label combination is observed.
func TestRegionInfoAppearsOnlyOnceSet(t *testing.T) {
	m := NewMetrics("test")
	const series = `lightningfeed_config_info{region="crete"}`

	if present(scrape(t, m), series) {
		t.Error("config_info exported before the region was set")
	}
	m.SetRegionInfo("crete")
	if got := sample(t, scrape(t, m), series); got != 1 {
		t.Errorf("config_info = %v, want 1", got)
	}
}

// TestHealthProbeChecksEverySignalInPriorityOrder pins the readiness decision,
// including that a standby is deliberately not ready.
func TestHealthProbeChecksEverySignalInPriorityOrder(t *testing.T) {
	yes := func() bool { return true }
	no := func() bool { return false }
	never := func() time.Time { return time.Time{} }

	tests := []struct {
		name       string
		probe      Probe
		wantReady  bool
		wantReason string
	}{
		{
			name:      "everything in order",
			probe:     Probe{Connected: yes, IsLeader: yes, StoreHealthy: yes, LastMessage: func() time.Time { return time.Now() }},
			wantReady: true,
		},
		{
			name:       "a standby is working correctly and must report not ready",
			probe:      Probe{Connected: yes, IsLeader: no, StoreHealthy: yes, LastMessage: func() time.Time { return time.Now() }},
			wantReason: "not the leader",
		},
		{
			// A standby loses the lease question before the store question, because
			// a non-leader does not own a store to be unhealthy.
			name:       "leadership outranks the store",
			probe:      Probe{Connected: yes, IsLeader: no, StoreHealthy: no, LastMessage: func() time.Time { return time.Now() }},
			wantReason: "not the leader",
		},
		{
			name:       "an unreachable store outranks a dropped upstream",
			probe:      Probe{Connected: no, IsLeader: yes, StoreHealthy: no, LastMessage: never},
			wantReason: "store",
		},
		{
			name:       "a disconnected upstream is reported",
			probe:      Probe{Connected: no, IsLeader: yes, StoreHealthy: yes, LastMessage: never},
			wantReason: "upstream connection",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.probe.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

			body := rec.Body.String()
			if tc.wantReady {
				if rec.Code != http.StatusOK {
					t.Errorf("status = %d, want 200; body %s", rec.Code, body)
				}
				return
			}
			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want 503; body %s", rec.Code, body)
			}
			if !strings.Contains(body, tc.wantReason) {
				t.Errorf("body %s does not explain %q", body, tc.wantReason)
			}
		})
	}
}

// TestStartServerReportsTheResolvedPort covers why the listener is opened here
// rather than by ListenAndServe: a bind failure has to reach the caller, and a
// port of 0 has to be resolvable.
func TestStartServerReportsTheResolvedPort(t *testing.T) {
	m := NewMetrics("test")
	probe := Probe{Connected: func() bool { return true }, IsLeader: func() bool { return true }}

	srv, err := StartServer(t.Context(), "127.0.0.1:0", m.Handler(), probe)
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	if srv.Addr() == "127.0.0.1:0" {
		t.Error("Addr() returned the requested port, not the one the kernel chose")
	}
	if !regexp.MustCompile(`^127\.0\.0\.1:\d+$`).MatchString(srv.Addr()) {
		t.Errorf("Addr() = %q, want host:port", srv.Addr())
	}
}

// TestStartServerReportsABindFailure is the reason the listener moved: with
// ListenAndServe in a goroutine a bind failure was logged where nobody watched,
// and the process carried on serving nothing.
func TestStartServerReportsABindFailure(t *testing.T) {
	m := NewMetrics("test")
	probe := Probe{}

	first, err := StartServer(t.Context(), "127.0.0.1:0", m.Handler(), probe)
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer first.Close()

	// The port is genuinely taken now, so the second bind must fail and say so.
	if _, err := StartServer(t.Context(), first.Addr(), m.Handler(), probe); err == nil {
		t.Error("binding an occupied port succeeded")
	}
}
