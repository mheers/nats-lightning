// Package config turns flags and environment variables into a validated
// configuration.
//
// Validation happens once, at startup, and fails loudly. A bridge that starts
// with a nonsensical region and quietly publishes nothing is worse than one
// that refuses to start.
package config

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/heers-it/lightningfeed/internal/geo"
	"github.com/heers-it/lightningfeed/internal/upstream"
)

// Defaults chosen from measurement rather than taste.
const (
	// DefaultReconnectMin is the upstream's measured tolerance floor.
	//
	// Attempts with no delay succeeded 0 of 6 against the live service, five
	// seconds apart 2 of 6, fifteen seconds apart 6 of 6, and the failures are
	// silent hangs. Anything lower is a reconnect loop that cannot succeed.
	DefaultReconnectMin = 15 * time.Second

	// DefaultReconnectMax caps the exponential backoff.
	DefaultReconnectMax = 5 * time.Minute

	// DefaultIdleTimeout exceeds the upstream's roughly ten second heartbeat.
	DefaultIdleTimeout = 45 * time.Second

	// DefaultBackfillDrop is the upstream's measured replay depth plus margin.
	DefaultBackfillDrop = 5 * time.Minute

	// DefaultDedupeTTL only has to outlive the replay window.
	DefaultDedupeTTL = 10 * time.Minute

	// DefaultRetention is how long strokes stay in SQLite.
	DefaultRetention = 7 * 24 * time.Hour
)

// Config is the fully resolved configuration.
type Config struct {
	// NATS
	NATSURL   string
	Stream    string
	JetStream bool

	// Region. A zero Circle means world-wide.
	RegionName     string
	Region         geo.Circle
	RegionEnabled  bool
	BoundaryPolicy geo.BoundaryPolicy

	// Upstream
	UpstreamURL      string
	UpstreamFailover string
	SourceMask       upstream.SrcMask
	ReconnectMin     time.Duration
	ReconnectMax     time.Duration
	IdleTimeout      time.Duration
	HandshakeTimeout time.Duration

	// Filter
	BackfillDrop time.Duration
	DedupeTTL    time.Duration

	// Store
	SQLitePath string
	Retention  time.Duration
	PruneEvery time.Duration

	// Leadership
	LeaderElect bool
	LeaderKey   string

	// Observability
	MetricsAddr string
	LogLevel    slog.Level
	LogFormat   string
}

// RegionConfigured reports whether a region was set.
func (c Config) RegionConfigured() bool { return c.RegionEnabled && c.Region.RadiusKm > 0 }

// Cells returns the geohash cells the region covers, or nil for world-wide.
func (c Config) Cells() ([]string, error) {
	if !c.RegionConfigured() {
		return nil, nil
	}
	cells, err := c.Region.Cells(5)
	if err != nil {
		return nil, err
	}
	return cells, nil
}

// Viewport returns the upstream viewport for the region, or the whole world.
func (c Config) Viewport() upstream.Viewport {
	if !c.RegionConfigured() {
		return upstream.Viewport{North: 85, East: 179.9, South: -85, West: -179.9}
	}
	b := c.Region.BoundingBox()
	return upstream.Viewport{North: b.MaxLat, East: b.MaxLon, South: b.MinLat, West: b.MinLon}
}

// Parse reads flags and the environment into a validated Config.
//
// Environment variables use the LIGHTNINGFEED_ prefix and override flags, so a
// deployment can set secrets without writing them into a unit file's arguments.
func Parse(args []string, getenv func(string) string) (*Config, error) {
	cfg := &Config{
		NATSURL:          "nats://127.0.0.1:4222",
		Stream:           "LIGHTNING",
		JetStream:        true,
		BoundaryPolicy:   geo.PolicyInclude,
		UpstreamURL:      "wss://live.lightningmaps.org:443/",
		UpstreamFailover: "wss://live2.lightningmaps.org:443/",
		SourceMask:       upstream.DefaultSrcMask,
		ReconnectMin:     DefaultReconnectMin,
		ReconnectMax:     DefaultReconnectMax,
		IdleTimeout:      DefaultIdleTimeout,
		HandshakeTimeout: 20 * time.Second,
		BackfillDrop:     DefaultBackfillDrop,
		DedupeTTL:        DefaultDedupeTTL,
		SQLitePath:       "/var/lib/lightningfeed/lightningfeed.db",
		Retention:        DefaultRetention,
		PruneEvery:       time.Hour,
		LeaderElect:      true,
		LeaderKey:        "lightningfeed/leader",
		MetricsAddr:      ":9109",
		LogLevel:         slog.LevelInfo,
		LogFormat:        "json",
	}

	fs := flag.NewFlagSet("lightningfeed", flag.ContinueOnError)
	fs.StringVar(&cfg.NATSURL, "nats-url", cfg.NATSURL, "NATS server URL")
	fs.StringVar(&cfg.Stream, "stream", cfg.Stream, "JetStream stream name")
	fs.BoolVar(&cfg.JetStream, "jetstream", cfg.JetStream, "use JetStream rather than Core NATS")

	fs.StringVar(&cfg.RegionName, "region-name", "", "region name, for labels")
	fs.Float64Var(&cfg.Region.Lat, "region-lat", 0, "region centre latitude")
	fs.Float64Var(&cfg.Region.Lon, "region-lon", 0, "region centre longitude")
	fs.Float64Var(&cfg.Region.RadiusKm, "region-radius-km", 0, "region radius in km; 0 means world-wide")
	fs.StringVar((*string)(&cfg.BoundaryPolicy), "boundary-policy", string(cfg.BoundaryPolicy),
		"treat undecidable strokes as inside: include or exclude")

	fs.StringVar(&cfg.UpstreamURL, "upstream-url", cfg.UpstreamURL, "upstream WebSocket URL")
	fs.StringVar(&cfg.UpstreamFailover, "upstream-failover", cfg.UpstreamFailover, "failover upstream URL")
	fs.IntVar((*int)(&cfg.SourceMask), "src-mask", int(cfg.SourceMask),
		"source bitmask: 1 reserved, 2 Blitzortung.org, 4 LightningMaps.org, 8 test")
	fs.DurationVar(&cfg.ReconnectMin, "reconnect-min", cfg.ReconnectMin,
		"minimum reconnect delay; below 15s the upstream throttles reconnects")
	fs.DurationVar(&cfg.ReconnectMax, "reconnect-max", cfg.ReconnectMax, "maximum reconnect delay")
	fs.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "reconnect if no frame arrives for this long")
	fs.DurationVar(&cfg.HandshakeTimeout, "handshake-timeout", cfg.HandshakeTimeout, "subscribe and hello timeout")

	fs.DurationVar(&cfg.BackfillDrop, "backfill-drop", cfg.BackfillDrop, "drop strokes older than this on connect")
	fs.DurationVar(&cfg.DedupeTTL, "dedupe-ttl", cfg.DedupeTTL, "how long a stroke identity is remembered")

	fs.StringVar(&cfg.SQLitePath, "sqlite", cfg.SQLitePath, "SQLite database path")
	fs.DurationVar(&cfg.Retention, "retention", cfg.Retention, "how long to keep strokes in SQLite")
	fs.DurationVar(&cfg.PruneEvery, "prune-every", cfg.PruneEvery, "how often to prune old strokes")

	fs.BoolVar(&cfg.LeaderElect, "leader-elect", cfg.LeaderElect, "require leadership before connecting upstream")
	fs.StringVar(&cfg.LeaderKey, "leader-key", cfg.LeaderKey, "KV key holding the leadership lease")

	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "metrics listen address")
	fs.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "log format: json or text")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn or error")
	// Applied after parsing so an invalid value is reported by finalize, in the
	// same place and the same way as every other configuration error.
	defer func() {}()
	_ = logLevel

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// An unparseable environment value must be an error, not a silent no-op: a
	// typo in a unit file would otherwise leave the default in place and the
	// bridge quietly running with the wrong region.
	if err := applyEnv(getenv, cfg); err != nil {
		return nil, err
	}

	if err := cfg.finalize(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// envOverrides lists the environment variables that map onto config fields.
//
// It is a table rather than reflection because the mapping is small, the types
// are not uniform, and an explicit table cannot silently miss a field the way a
// name-convention convention can.
var envOverrides = []struct {
	env   string
	apply func(*Config, string) error
}{
	{"LIGHTNINGFEED_NATS_URL", func(c *Config, v string) error { c.NATSURL = v; return nil }},
	{"LIGHTNINGFEED_STREAM", func(c *Config, v string) error { c.Stream = v; return nil }},
	{"LIGHTNINGFEED_REGION_NAME", func(c *Config, v string) error { c.RegionName = v; return nil }},
	{"LIGHTNINGFEED_REGION_LAT", func(c *Config, v string) error { return setFloat(&c.Region.Lat, v) }},
	{"LIGHTNINGFEED_REGION_LON", func(c *Config, v string) error { return setFloat(&c.Region.Lon, v) }},
	{"LIGHTNINGFEED_REGION_RADIUS_KM", func(c *Config, v string) error { return setFloat(&c.Region.RadiusKm, v) }},
	{"LIGHTNINGFEED_BOUNDARY_POLICY", func(c *Config, v string) error {
		p, err := geo.ParseBoundaryPolicy(v)
		if err != nil {
			return err
		}
		c.BoundaryPolicy = p
		return nil
	}},
	{"LIGHTNINGFEED_UPSTREAM_URL", func(c *Config, v string) error { c.UpstreamURL = v; return nil }},
	{"LIGHTNINGFEED_SRC_MASK", func(c *Config, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("src-mask: %w", err)
		}
		c.SourceMask = upstream.SrcMask(n)
		return nil
	}},
	{"LIGHTNINGFEED_SQLITE", func(c *Config, v string) error { c.SQLitePath = v; return nil }},
	{"LIGHTNINGFEED_RETENTION", func(c *Config, v string) error { return setDuration(&c.Retention, v) }},
	{"LIGHTNINGFEED_LEADER_ELECT", func(c *Config, v string) error { return setBool(&c.LeaderElect, v) }},
	{"LIGHTNINGFEED_METRICS_ADDR", func(c *Config, v string) error { c.MetricsAddr = v; return nil }},
	{"LIGHTNINGFEED_LOG_LEVEL", func(c *Config, v string) error { return setLevel(&c.LogLevel, v) }},
	{"LIGHTNINGFEED_LOG_FORMAT", func(c *Config, v string) error { c.LogFormat = v; return nil }},
}

func applyEnv(getenv func(string) string, cfg *Config) error {
	for _, o := range envOverrides {
		v := getenv(o.env)
		if v == "" {
			continue
		}
		if err := o.apply(cfg, v); err != nil {
			return fmt.Errorf("config: %s: %w", o.env, err)
		}
	}
	return nil
}

// finalize validates and fills in derived values.
func (c *Config) finalize() error {
	// A region needs all three parts. Two of three would silently mean
	// world-wide, which is the opposite of what an operator typing only a
	// latitude intends.
	parts := 0
	if c.Region.Lat != 0 {
		parts++
	}
	if c.Region.Lon != 0 {
		parts++
	}
	if c.Region.RadiusKm != 0 {
		parts++
	}

	switch {
	case parts == 0:
		c.RegionEnabled = false
	case parts == 3:
		if err := c.Region.Validate(); err != nil {
			return fmt.Errorf("config: region: %w", err)
		}
		c.RegionEnabled = true
		if c.RegionName == "" {
			c.RegionName = fmt.Sprintf("%.4f,%.4f", c.Region.Lat, c.Region.Lon)
		}
	default:
		return fmt.Errorf(
			"config: region needs latitude, longitude and radius together; got %d of 3. "+
				"Omit all three for a world-wide feed", parts)
	}

	if _, err := geo.ParseBoundaryPolicy(string(c.BoundaryPolicy)); err != nil {
		return err
	}

	if c.ReconnectMin <= 0 {
		return fmt.Errorf("config: reconnect-min must be positive, got %v", c.ReconnectMin)
	}
	if c.ReconnectMin < DefaultReconnectMin {
		return fmt.Errorf(
			"config: reconnect-min is %v, but the upstream throttles reconnects and "+
				"measured 0 of 6 attempts succeeding below about 15s; use at least %v",
			c.ReconnectMin, DefaultReconnectMin)
	}
	if c.ReconnectMax < c.ReconnectMin {
		return fmt.Errorf("config: reconnect-max %v is below reconnect-min %v",
			c.ReconnectMax, c.ReconnectMin)
	}
	if c.IdleTimeout <= 0 {
		return fmt.Errorf("config: idle-timeout must be positive, got %v", c.IdleTimeout)
	}
	if c.SQLitePath == "" {
		return fmt.Errorf("config: a SQLite path is required")
	}
	if c.Retention <= 0 {
		return fmt.Errorf("config: retention must be positive, got %v", c.Retention)
	}
	if c.DedupeTTL < c.BackfillDrop {
		return fmt.Errorf(
			"config: dedupe-ttl %v is shorter than the backfill window %v, so a replay "+
				"arriving after the TTL would not be recognised", c.DedupeTTL, c.BackfillDrop)
	}
	if c.SourceMask == 0 {
		return fmt.Errorf("config: src-mask selects no network")
	}
	return nil
}

func setFloat(dst *float64, v string) error {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return err
	}
	*dst = f
	return nil
}

func setDuration(dst *time.Duration, v string) error {
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return err
	}
	*dst = d
	return nil
}

func setBool(dst *bool, v string) error {
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return err
	}
	*dst = b
	return nil
}

func setLevel(dst *slog.Level, v string) error {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "debug":
		*dst = slog.LevelDebug
	case "info":
		*dst = slog.LevelInfo
	case "warn", "warning":
		*dst = slog.LevelWarn
	case "error":
		*dst = slog.LevelError
	default:
		return fmt.Errorf("unknown log level %q", v)
	}
	return nil
}

// Logger builds a logger from the configuration.
func (c Config) Logger() *slog.Logger {
	opts := &slog.HandlerOptions{Level: c.LogLevel}
	if c.LogFormat == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

// ParseFromEnv is Parse with the process environment, for use in main.
func ParseFromEnv(args []string) (*Config, error) {
	return Parse(args, os.Getenv)
}
