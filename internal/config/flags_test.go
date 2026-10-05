package config_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mheers/nats-lightning/internal/config"
)

// The log level flag was registered, assigned to a local variable, and then
// discarded — so --log-level did nothing at all while the environment variable
// worked. Every level has to survive the round trip through the string flag.
func TestTheLogLevelFlagIsApplied(t *testing.T) {
	for _, tc := range []struct {
		flag string
		want string
	}{
		{"debug", "DEBUG"},
		{"info", "INFO"},
		{"warn", "WARN"},
		{"warning", "WARN"},
		{"error", "ERROR"},
	} {
		t.Run(tc.flag, func(t *testing.T) {
			cfg, err := config.Parse([]string{"--log-level=" + tc.flag}, noEnv)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := cfg.LogLevel.String(); got != tc.want {
				t.Errorf("--log-level=%s gave LogLevel %s, want %s", tc.flag, got, tc.want)
			}
		})
	}
}

// Both spellings the flag package accepts have to reach the same field.
func TestTheLogLevelFlagAcceptsASeparatedValue(t *testing.T) {
	cfg, err := config.Parse([]string{"--log-level", "debug"}, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.LogLevel.String(); got != "DEBUG" {
		t.Errorf("--log-level debug gave LogLevel %s, want DEBUG", got)
	}
}

// An unusable level is a configuration error, reported at startup, rather than
// something discovered at the first log line.
func TestAnUnknownLogLevelIsRejected(t *testing.T) {
	_, err := config.Parse([]string{"--log-level=chatty"}, noEnv)
	if err == nil {
		t.Fatal("Parse accepted an unknown log level")
	}
	if !strings.Contains(err.Error(), "log-level") {
		t.Errorf("the error should name the offending flag, got: %v", err)
	}
}

// Whether a region was supplied cannot be recovered from the parsed values,
// because zero is both "not supplied" and "on the equator". Treating a
// legitimate 0 as missing rejected a valid region with a message about a part
// that was in fact present.
func TestARegionOnTheEquatorAndPrimeMeridianIsAccepted(t *testing.T) {
	cfg, err := config.Parse([]string{
		"--region-lat", "0", "--region-lon", "0", "--region-radius-km", "10",
	}, noEnv)
	if err != nil {
		t.Fatalf("a region at 0,0 was rejected: %v", err)
	}
	if !cfg.RegionConfigured() {
		t.Error("RegionConfigured is false for a fully specified region at 0,0")
	}
}

// The same has to hold when the region comes from the environment, or an
// environment-configured deployment at the equator could not run at all.
func TestAnEquatorialRegionFromTheEnvironmentIsAccepted(t *testing.T) {
	cfg, err := config.Parse(nil, env(map[string]string{
		"LIGHTNINGFEED_REGION_LAT":       "0",
		"LIGHTNINGFEED_REGION_LON":       "0",
		"LIGHTNINGFEED_REGION_RADIUS_KM": "10",
	}))
	if err != nil {
		t.Fatalf("an equatorial region from the environment was rejected: %v", err)
	}
	if !cfg.RegionConfigured() {
		t.Error("RegionConfigured is false for a fully specified environment region")
	}
}

// Each of the three parts is individually load-bearing, and a latitude of 0 has
// to count as present rather than as missing.
func TestEachRegionPartCountsEvenWhenItIsZero(t *testing.T) {
	for _, part := range []string{"--region-lat", "--region-lon", "--region-radius-km"} {
		t.Run(part, func(t *testing.T) {
			// The part is present and zero; the other two are omitted, so this
			// must be rejected as incomplete rather than accepted.
			_, err := config.Parse([]string{part, "0"}, noEnv)
			if err == nil {
				t.Errorf("%s=0 alone was accepted; an incomplete region must be rejected", part)
			}
		})
	}
}

// The environment wins over the flag for the log level too, which is the whole
// reason a deployment can set its verbosity without editing a unit file.
func TestTheEnvironmentOverridesTheLogLevelFlag(t *testing.T) {
	cfg, err := config.Parse([]string{"--log-level=error"}, env(map[string]string{
		"LIGHTNINGFEED_LOG_LEVEL": "debug",
	}))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.LogLevel.String(); got != "DEBUG" {
		t.Errorf("LogLevel = %s, want DEBUG from the environment", got)
	}
}

// The publish retry bounds are validated like every other setting, because zero
// attempts means never publishing, which is not a retry policy.
func TestThePublishRetrySettingsAreValidated(t *testing.T) {
	cfg, err := config.Parse([]string{
		"--publish-attempts", "5", "--publish-backoff", "100ms",
	}, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.PublishAttempts != 5 {
		t.Errorf("PublishAttempts = %d, want 5", cfg.PublishAttempts)
	}
	if cfg.PublishBackoff != 100*time.Millisecond {
		t.Errorf("PublishBackoff = %v, want 100ms", cfg.PublishBackoff)
	}

	for _, args := range [][]string{
		{"--publish-attempts", "0"},
		{"--publish-attempts", "-1"},
		{"--publish-backoff", "-1s"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			if _, err := config.Parse(args, noEnv); err == nil {
				t.Errorf("Parse accepted %v", args)
			}
		})
	}
}

// The defaults exist so that a broker hiccup costs a retry rather than a
// process exit, and the documented defaults have to be what actually applies
// when nothing is configured.
func TestThePublishRetryDefaultsAreUsable(t *testing.T) {
	cfg, err := config.Parse(nil, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.PublishAttempts < 2 {
		t.Errorf("PublishAttempts defaults to %d; a single attempt is no retry at all", cfg.PublishAttempts)
	}
	if cfg.PublishBackoff <= 0 {
		t.Errorf("PublishBackoff defaults to %v, want a positive pause", cfg.PublishBackoff)
	}
}

// Every environment variable documented in the README has to reach its field. A
// mapping that silently misses one leaves the default in place, which for a
// region means quietly publishing the wrong thing.
func TestEveryDocumentedEnvironmentVariableReachesItsField(t *testing.T) {
	// Each case sets one variable to a value of the right shape and reads the
	// field it belongs to. A mapping that silently misses a variable leaves the
	// default in place, which for a region means quietly publishing the wrong
	// thing, so each is checked individually rather than as a batch.
	for _, tc := range []struct {
		env   string
		value string

		// also is any further variables the setting needs. A region part on its
		// own is legitimately refused as incomplete, so those cases supply the
		// rest of the region and read back only their own component.
		also map[string]string

		// read returns the field's value in a form comparable with value.
		read func(*config.Config) string
	}{
		{
			env: "LIGHTNINGFEED_NATS_URL", value: "nats://sentinel:4222",
			read: func(c *config.Config) string { return c.NATSURL },
		},
		{
			env: "LIGHTNINGFEED_STREAM", value: "SENTINEL",
			read: func(c *config.Config) string { return c.Stream },
		},
		{
			env: "LIGHTNINGFEED_REGION_NAME", value: "sentinel",
			read: func(c *config.Config) string { return c.RegionName },
		},
		{
			env: "LIGHTNINGFEED_BOUNDARY_POLICY", value: "exclude",
			read: func(c *config.Config) string { return string(c.BoundaryPolicy) },
		},
		{
			env: "LIGHTNINGFEED_UPSTREAM_URL", value: "wss://sentinel.invalid/",
			read: func(c *config.Config) string { return c.UpstreamURL },
		},
		{
			env: "LIGHTNINGFEED_SQLITE", value: "/tmp/sentinel.db",
			read: func(c *config.Config) string { return c.SQLitePath },
		},
		{
			env: "LIGHTNINGFEED_METRICS_ADDR", value: ":9999",
			read: func(c *config.Config) string { return c.MetricsAddr },
		},
		{
			env: "LIGHTNINGFEED_LOG_FORMAT", value: "text",
			read: func(c *config.Config) string { return c.LogFormat },
		},
		{
			env: "LIGHTNINGFEED_SRC_MASK", value: "2",
			read: func(c *config.Config) string { return fmt.Sprint(int(c.SourceMask)) },
		},
		{
			env: "LIGHTNINGFEED_LEADER_ELECT", value: "false",
			read: func(c *config.Config) string { return fmt.Sprint(c.LeaderElect) },
		},
		{
			env: "LIGHTNINGFEED_PUBLISH_ATTEMPTS", value: "7",
			read: func(c *config.Config) string { return fmt.Sprint(c.PublishAttempts) },
		},
		{
			env: "LIGHTNINGFEED_LOG_LEVEL", value: "debug",
			read: func(c *config.Config) string { return strings.ToLower(c.LogLevel.String()) },
		},
		{
			env: "LIGHTNINGFEED_REGION_LAT", value: "12.5",
			also: regionEnv("12.5", "13.5", "25"),
			read: func(c *config.Config) string { return fmt.Sprint(c.Region.Lat) },
		},
		{
			env: "LIGHTNINGFEED_REGION_LON", value: "13.5",
			also: regionEnv("12.5", "13.5", "25"),
			read: func(c *config.Config) string { return fmt.Sprint(c.Region.Lon) },
		},
		{
			env: "LIGHTNINGFEED_REGION_RADIUS_KM", value: "25",
			also: regionEnv("12.5", "13.5", "25"),
			read: func(c *config.Config) string { return fmt.Sprint(c.Region.RadiusKm) },
		},
	} {
		t.Run(tc.env, func(t *testing.T) {
			vars := map[string]string{tc.env: tc.value}
			for k, v := range tc.also {
				vars[k] = v
			}

			cfg, err := config.Parse(nil, env(vars))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := tc.read(cfg); got != tc.value {
				t.Errorf("%s=%q read back as %q; the variable did not reach its field",
					tc.env, tc.value, got)
			}
		})
	}

	// The two duration settings are compared as parsed values rather than
	// strings, because Go renders a duration canonically and "48h" would never
	// match "48h0m0s" as text.
	for _, tc := range []struct {
		env   string
		value time.Duration
		read  func(*config.Config) time.Duration
	}{
		{"LIGHTNINGFEED_RETENTION", 48 * time.Hour, func(c *config.Config) time.Duration { return c.Retention }},
		{
			"LIGHTNINGFEED_PUBLISH_BACKOFF", 750 * time.Millisecond,
			func(c *config.Config) time.Duration { return c.PublishBackoff },
		},
	} {
		t.Run(tc.env, func(t *testing.T) {
			cfg, err := config.Parse(nil, env(map[string]string{tc.env: tc.value.String()}))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := tc.read(cfg); got != tc.value {
				t.Errorf("%s=%v read back as %v; the variable did not reach its field",
					tc.env, tc.value, got)
			}
		})
	}
}

// A bare word where a flag was expected is discarded by the flag package without a
// word, so the parser has to notice it itself. A dropped argument is the quietest
// misconfiguration there is: a region narrowed to the wrong circle, or a SQLite
// path that stayed the default, with every log line looking healthy.
func TestUnexpectedPositionalArgumentsAreRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"stray word", []string{"cells", "strayarg"}},
		{"value whose flag name was forgotten", []string{"--region-lat=48", "48.14"}},
		{"trailing word", []string{"--log-level=info", "ingest"}},
		{"positional help", []string{"help"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(tc.args, env(nil))
			if err == nil {
				t.Fatalf("Parse(%v) succeeded, want an error naming the stray argument", tc.args)
			}
			if !strings.Contains(err.Error(), "unexpected argument") {
				t.Errorf("error does not explain the problem: %v", err)
			}
		})
	}
}

// The error has to be actionable: a refusal that does not say which flags exist
// leaves the reader to guess which word was meant to be one.
func TestUnexpectedArgumentErrorListsTheFlags(t *testing.T) {
	_, err := config.Parse([]string{"--log-level=info", "strayarg"}, env(nil))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, flag := range []string{"--region-lat", "--nats-url", "--sqlite"} {
		if !strings.Contains(err.Error(), flag) {
			t.Errorf("error does not mention %s: %v", flag, err)
		}
	}
}

// regionEnv completes a region so a single component can be checked in isolation
// without tripping the incomplete-region guard.
func regionEnv(lat, lon, radiusKm string) map[string]string {
	return map[string]string{
		"LIGHTNINGFEED_REGION_LAT":       lat,
		"LIGHTNINGFEED_REGION_LON":       lon,
		"LIGHTNINGFEED_REGION_RADIUS_KM": radiusKm,
	}
}

// formatConfig is not needed: each case reads its own field directly, which
// localises a failure to the one variable that broke.
