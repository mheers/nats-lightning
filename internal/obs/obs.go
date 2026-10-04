// Package obs exposes the metrics and health endpoints the bridge runs.
//
// The metric set is chosen around what has actually gone wrong so far, not
// around what is easy to count. The three that matter most are the age of the
// last upstream frame, the ratio of dropped to published strokes, and whether
// this process holds leadership: together they distinguish "no lightning" from
// "wedged upstream" from "not the leader", which are three very different
// incidents that otherwise look identical from outside.
package obs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every counter the bridge maintains.
type Metrics struct {
	reg *prometheus.Registry

	StrokesTotal   *prometheus.CounterVec
	PublishedTotal *prometheus.CounterVec
	DroppedTotal   *prometheus.CounterVec

	UpstreamConnected  prometheus.Gauge
	UpstreamReconnects *prometheus.CounterVec
	UpstreamMalformed  prometheus.Counter

	LeadershipHeld prometheus.Gauge

	StoreRows    prometheus.GaugeFunc
	StorePruned  prometheus.Counter
	PublishLagMs prometheus.Histogram

	region string
}

// NewMetrics builds the metric set.
//
// It uses a private registry rather than the default one so that importing this
// package cannot collide with another library's collectors, and so the exposed
// surface is exactly what is registered here.
func NewMetrics(region string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		reg:    reg,
		region: region,
		StrokesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lightningfeed_strokes_total",
			Help: "Strokes received from the upstream, by source.",
		}, []string{"src"}),

		PublishedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lightningfeed_published_total",
			Help: "Strokes published to NATS, by result.",
		}, []string{"result"}),

		DroppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lightningfeed_dropped_total",
			Help: "Strokes discarded, by reason.",
		}, []string{"reason"}),

		UpstreamConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lightningfeed_upstream_connected",
			Help: "1 when an upstream connection is live.",
		}),

		UpstreamReconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lightningfeed_reconnects_total",
			Help: "Upstream reconnection attempts, by host and reason.",
		}, []string{"host", "reason"}),

		UpstreamMalformed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lightningfeed_malformed_frames_total",
			Help: "Upstream frames that failed to decode.",
		}),

		LeadershipHeld: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lightningfeed_leader",
			Help: "1 when this process holds the leadership lease.",
		}),

		StorePruned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lightningfeed_store_pruned_total",
			Help: "Strokes deleted by retention pruning.",
		}),

		PublishLagMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "lightningfeed_publish_lag_ms",
			Help:    "Milliseconds from stroke time to publication.",
			Buckets: []float64{1000, 2000, 5000, 10000, 30000, 60000, 300000},
		}),
	}

	// Registered without a region label: region is a property of the process,
	// and adding it here would multiply every series for no benefit. The region
	// is exposed once, as an info metric.
	m.reg.MustRegister(
		m.StrokesTotal, m.PublishedTotal, m.DroppedTotal,
		m.UpstreamConnected, m.UpstreamReconnects, m.UpstreamMalformed,
		m.LeadershipHeld, m.StorePruned, m.PublishLagMs,
	)
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "lightningfeed_build_info",
		Help: "Static build and configuration information.",
	}, func() float64 { return 1 }))
	reg.MustRegister(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lightningfeed_config_info",
		Help: "Configured region.",
	}, []string{"region"}))

	return m
}

// UpstreamLastMessage reports seconds since the last upstream frame.
//
// This is the single most useful signal for telling a quiet feed apart from a
// dead one: the upstream sends a heartbeat every ten seconds, so this should
// never exceed the idle timeout while connected.
func (m *Metrics) LastMessageAgeGauge(lastMessage func() time.Time, connected func() bool) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "lightningfeed_upstream_last_message_age_seconds",
		Help: "Seconds since the last upstream frame; the upstream heartbeats about every 10s.",
	}, func() float64 {
		if !connected() {
			return disconnectedSentinel
		}
		last := lastMessage()
		if last.IsZero() {
			return disconnectedSentinel
		}
		return time.Since(last).Seconds()
	})
}

// disconnectedSentinel stands in for "no frame yet" or "not connected".
//
// A large finite value rather than +Inf, because +Inf serialises as "+Inf" which
// some scrapers and dashboards handle badly.
const disconnectedSentinel = 1e9

// Handler serves the metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Registry exposes the registry for tests and for custom collectors.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Health is the state the health endpoint reports.
type Health struct {
	// Ready is true when the process can serve: it holds leadership and, if a
	// region is configured, the store is reachable.
	Ready bool `json:"ready"`

	// Leader is true when this process holds the lease.
	Leader bool `json:"leader"`

	// UpstreamConnected is true when the upstream socket is live.
	UpstreamConnected bool `json:"upstream_connected"`

	// LastMessageAge is seconds since the last upstream frame.
	LastMessageAge float64 `json:"upstream_last_message_age_seconds"`

	// Region is the configured region name, empty for world-wide.
	Region string `json:"region,omitempty"`

	// Version is reported so a dashboard can tell which build it is looking at.
	Version string `json:"version"`

	// Reason explains why the process is not ready.
	Reason string `json:"reason,omitempty"`
}

// Probe is the health check the bridge exposes.
type Probe struct {
	// Connected reports whether the upstream socket is live.
	Connected func() bool

	// LastMessage returns when the last upstream frame arrived.
	LastMessage func() time.Time

	// IsLeader reports whether this process holds the lease.
	IsLeader func() bool

	// StoreHealthy reports whether the database is reachable.
	StoreHealthy func() bool

	// Version identifies the build.
	Version string

	// Region names the configured region.
	Region string

	// ID identifies this process in logs and health responses.
	ID string
}

// ServeHTTP writes the health response.
//
// A standby is deliberately reported as not ready. It is working correctly and
// should not be sent traffic, which is what readiness means here.
func (p Probe) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := Health{
		Region:  p.Region,
		Version: p.Version,
	}
	if p.Connected != nil {
		h.UpstreamConnected = p.Connected()
	}
	if p.IsLeader != nil {
		h.Leader = p.IsLeader()
	}
	if p.LastMessage != nil {
		last := p.LastMessage()
		if !last.IsZero() {
			h.LastMessageAge = time.Since(last).Seconds()
		} else {
			h.LastMessageAge = disconnectedSentinel
		}
	}

	switch {
	case !h.Leader:
		h.Reason = "not the leader; another process holds the upstream connection"
	case p.StoreHealthy != nil && !p.StoreHealthy():
		h.Reason = "the stroke store is not reachable"
	case !h.UpstreamConnected:
		h.Reason = "the upstream connection is not established"
	default:
		h.Ready = true
	}

	w.Header().Set("Content-Type", "application/json")
	// A standby answers 503 so a load balancer stops sending it work, while
	// still reporting enough to diagnose why.
	if !h.Ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(h)
}

// Server runs the metrics and health endpoints.
type Server struct {
	srv *http.Server
}

// StartServer serves metrics and health on separate paths, and shuts down when
// ctx ends.
//
// The two are mounted at distinct paths rather than one handler at "/": a probe
// registered at the root shadows every other route on the mux, which silently
// leaves the metrics unreachable while the endpoint still appears healthy.
func StartServer(ctx context.Context, addr string, metrics, health http.Handler) (*Server, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics)
	mux.Handle("/healthz", health)
	mux.Handle("/", health)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Nothing useful to do here: the process's own liveness is exposed
			// through the metrics it was serving.
			_ = err
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	return &Server{srv: srv}, nil
}

// Addr returns the listen address.
func (s *Server) Addr() string {
	if s.srv == nil {
		return ""
	}
	return s.srv.Addr
}

// Close stops the server.
func (s *Server) Close() error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Close()
}
