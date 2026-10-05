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

	"github.com/mheers/nats-lightning/internal/geo"
	"github.com/mheers/nats-lightning/internal/upstream"
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

	// DefaultPublishAttempts is how many times a publish is tried before the
	// process gives up.
	//
	// Three attempts, because the thing being absorbed is a broker hiccup
	// measured in milliseconds and the alternative — exiting on the first
	// failure — costs a reconnect to an upstream that throttles them.
	DefaultPublishAttempts = 3

	// DefaultPublishBackoff is the pause between publish attempts. It is short
	// enough that a brief outage is invisible to a consumer and long enough not
	// to spin.
	DefaultPublishBackoff = 250 * time.Millisecond
)

// Config is the fully resolved configuration.
type Config struct {
	// NATS
	//
	// There is no JetStream toggle. The publisher needs JetStream: it registers a
	// message id per stroke and relies on the broker's duplicate window to absorb
	// a reconnect replay, which Core NATS cannot do. NewPublisher fails outright
	// if JetStream is unavailable, so a flag choosing between them could only ever
	// be a way to fail.
	NATSURL string
	Stream  string

	// Region. A zero Circle means world-wide.
	RegionName     string
	Region         geo.Circle
	RegionEnabled  bool
	BoundaryPolicy geo.BoundaryPolicy

	// Upstream
	//
	// There is no failover URL. The two upstream servers issue independent id
	// sequences, so a cursor carried between them is not a resume; the client
	// warns and starts cold rather than resuming across the boundary. live2 is
	// never dialled automatically. Callers wanting the second server should point
	// UpstreamURL at it deliberately and accept the cold start.
	UpstreamURL      string
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

	// PublishAttempts and PublishBackoff bound the retry applied to a failed
	// publish.
	PublishAttempts int
	PublishBackoff  time.Duration

	// Leadership
	LeaderElect bool
	LeaderKey   string

	// Observability
	MetricsAddr string
	LogLevel    slog.Level
	LogFormat   string

	// Which region components the operator actually supplied.
	//
	// These record presence, not value. A region at latitude 0 or longitude 0
	// is valid, so "was this flag given" cannot be recovered afterwards from the
	// parsed number — only zero means both "unset" and "on the equator".
	regionLatSet    bool
	regionLonSet    bool
	regionRadiusSet bool
}

// markSupplied records which region components were given on the command line.
//
// flag.Visit reports only the flags actually present in the argument list, which
// is exactly the distinction that a value comparison cannot make.
func (c *Config) markSupplied(fs *flag.FlagSet) {
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "region-lat":
			c.regionLatSet = true
		case "region-lon":
			c.regionLonSet = true
		case "region-radius-km":
			c.regionRadiusSet = true
		}
	})
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
		BoundaryPolicy:   geo.PolicyInclude,
		UpstreamURL:      "wss://live.lightningmaps.org:443/",
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
		PublishAttempts:  DefaultPublishAttempts,
		PublishBackoff:   DefaultPublishBackoff,
		LeaderElect:      true,
		LeaderKey:        "lightningfeed/leader",
		MetricsAddr:      ":9109",
		LogLevel:         slog.LevelInfo,
		LogFormat:        "json",
	}

	fs := flag.NewFlagSet("lightningfeed", flag.ContinueOnError)
	fs.StringVar(&cfg.NATSURL, "nats-url", cfg.NATSURL, "NATS server URL")
	fs.StringVar(&cfg.Stream, "stream", cfg.Stream, "JetStream stream name")

	fs.StringVar(&cfg.RegionName, "region-name", "", "region name, for labels")
	fs.Float64Var(&cfg.Region.Lat, "region-lat", 0, "region centre latitude")
	fs.Float64Var(&cfg.Region.Lon, "region-lon", 0, "region centre longitude")
	fs.Float64Var(&cfg.Region.RadiusKm, "region-radius-km", 0, "region radius in km; 0 means world-wide")
	fs.StringVar((*string)(&cfg.BoundaryPolicy), "boundary-policy", string(cfg.BoundaryPolicy),
		"treat undecidable strokes as inside: include or exclude")

	fs.StringVar(&cfg.UpstreamURL, "upstream-url", cfg.UpstreamURL, "upstream WebSocket URL")
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

	fs.IntVar(&cfg.PublishAttempts, "publish-attempts", cfg.PublishAttempts,
		"how many times to try publishing a stroke before failing")
	fs.DurationVar(&cfg.PublishBackoff, "publish-backoff", cfg.PublishBackoff,
		"pause between publish attempts")

	fs.BoolVar(&cfg.LeaderElect, "leader-elect", cfg.LeaderElect, "require leadership before connecting upstream")
	fs.StringVar(&cfg.LeaderKey, "leader-key", cfg.LeaderKey, "KV key holding the leadership lease")

	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "metrics listen address")
	fs.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "log format: json or text")

	// Log level is registered as a string flag and resolved below, for two
	// reasons: slog.Level has no flag.Value implementation, and the environment
	// override has to be able to win. Binding it straight to a string flag whose
	// value was then discarded is what left --log-level silently doing nothing
	// while the environment variable worked.
	var logLevel string
	fs.StringVar(&logLevel, "log-level", cfg.LogLevel.String(), "log level: debug, info, warn or error")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	cfg.markSupplied(fs)

	// A bare word where a flag was expected is a mistake, and the flag package
	// cannot report it: it stops parsing at the first non-flag argument and leaves
	// it in Args, so anything after a forgotten "--" or a mistyped flag name is
	// discarded without a word. A region silently narrowed to the wrong one, or a
	// SQLite path that quietly stayed the default, is much harder to notice than a
	// refusal.
	if fs.NArg() > 0 {
		return nil, fmt.Errorf(
			"config: unexpected argument %q; every setting is a flag, so a value without its "+
				"flag name is a typo. Flags are %s", fs.Arg(0), strings.Join(flagNames(fs), ", "))
	}

	if err := setLevel(&cfg.LogLevel, logLevel); err != nil {
		return nil, fmt.Errorf("config: --log-level: %w", err)
	}

	// An unparseable environment value must be an error, not a silent no-op: a
	// typo in a unit file would otherwise leave the default in place and the
	// bridge quietly running with the wrong region.
	//
	// This runs after the flag is applied and not before, because the environment
	// is meant to win. Applying --log-level again here would overwrite whatever
	// applyEnv just set, which made LIGHTNINGFEED_LOG_LEVEL silently lose to
	// --log-level while every other variable correctly won — the one setting
	// where the documented precedence was a lie.
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
	{"LIGHTNINGFEED_REGION_LAT", func(c *Config, v string) error {
		c.regionLatSet = true
		return setFloat(&c.Region.Lat, v)
	}},
	{"LIGHTNINGFEED_REGION_LON", func(c *Config, v string) error {
		c.regionLonSet = true
		return setFloat(&c.Region.Lon, v)
	}},
	{"LIGHTNINGFEED_REGION_RADIUS_KM", func(c *Config, v string) error {
		c.regionRadiusSet = true
		return setFloat(&c.Region.RadiusKm, v)
	}},
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
	{"LIGHTNINGFEED_PUBLISH_ATTEMPTS", func(c *Config, v string) error {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return err
		}
		c.PublishAttempts = n
		return nil
	}},
	{"LIGHTNINGFEED_PUBLISH_BACKOFF", func(c *Config, v string) error {
		return setDuration(&c.PublishBackoff, v)
	}},
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
	//
	// "Supplied" is tracked explicitly rather than inferred from the value being
	// non-zero. A region on the equator or the prime meridian is perfectly
	// valid, and treating a legitimate 0 as "not supplied" rejected it with a
	// message about a missing part that was in fact present.
	parts := 0
	if c.regionLatSet {
		parts++
	}
	if c.regionLonSet {
		parts++
	}
	if c.regionRadiusSet {
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
	if c.PublishAttempts < 1 {
		return fmt.Errorf("config: publish-attempts must be at least 1, got %d", c.PublishAttempts)
	}
	if c.PublishBackoff < 0 {
		return fmt.Errorf("config: publish-backoff must not be negative, got %v", c.PublishBackoff)
	}
	if c.HandshakeTimeout <= 0 {
		return fmt.Errorf("config: handshake-timeout must be positive, got %v", c.HandshakeTimeout)
	}
	if c.PruneEvery <= 0 {
		return fmt.Errorf("config: prune-every must be positive, got %v", c.PruneEvery)
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

// flagNames lists a flag set's names in the order they were defined, for error
// messages that have to suggest the right flag without naming all of them.
func flagNames(fs *flag.FlagSet) []string {
	var names []string
	fs.VisitAll(func(f *flag.Flag) {
		names = append(names, "--"+f.Name)
	})
	return names
}

// ParseFromEnv is Parse with the process environment, for use in main.
func ParseFromEnv(args []string) (*Config, error) {
	return Parse(args, os.Getenv)
}
