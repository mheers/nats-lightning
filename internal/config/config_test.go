package config_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/mheers/nats-lightning/internal/config"
	"github.com/mheers/nats-lightning/internal/geo"
	"github.com/mheers/nats-lightning/internal/upstream"
)

// Slice 13: configuration.
//
// Validation happens once at startup and fails loudly. A bridge that starts with
// a nonsensical region and quietly publishes nothing is worse than one that
// refuses to start, so the tests here are mostly about what must be rejected.

func env(pairs map[string]string) func(string) string {
	return func(k string) string { return pairs[k] }
}

func noEnv(string) string { return "" }

// The deployment this project is configured for.
var regionFlags = []string{
	"--region-name", "roussospiti",
	"--region-lat", "35.3340688",
	"--region-lon", "24.4944483",
	"--region-radius-km", "10",
}

func TestTheConfiguredRegionParses(t *testing.T) {
	cfg, err := config.Parse(regionFlags, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !cfg.RegionConfigured() {
		t.Fatal("region should be configured")
	}
	if cfg.RegionName != "roussospiti" {
		t.Errorf("name = %q, want roussospiti", cfg.RegionName)
	}
	if cfg.Region.RadiusKm != 10 {
		t.Errorf("radius = %v, want 10", cfg.Region.RadiusKm)
	}
}

// Omitting the region must give a world-wide feed, which is a legitimate mode.
func TestOmittingTheRegionGivesAWorldWideFeed(t *testing.T) {
	cfg, err := config.Parse(nil, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.RegionConfigured() {
		t.Error("region should not be configured")
	}

	v := cfg.Viewport()
	if v.North <= 0 || v.South >= 0 {
		t.Errorf("world viewport should span the equator, got %+v", v)
	}
	cells, err := cfg.Cells()
	if err != nil {
		t.Fatalf("Cells: %v", err)
	}
	if cells != nil {
		t.Errorf("world-wide should have no cell restriction, got %v", cells)
	}
}

// A partially specified region is the dangerous case: an operator who supplies
// only a latitude almost certainly did not mean world-wide.
func TestAPartialRegionIsRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"latitude only", []string{"--region-lat", "35.3"}},
		{"latitude and longitude but no radius", []string{"--region-lat", "35.3", "--region-lon", "24.5"}},
		{"radius only", []string{"--region-radius-km", "10"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(tc.args, noEnv)
			if err == nil {
				t.Fatalf("a partial region was accepted; it would silently mean world-wide")
			}
			if !strings.Contains(err.Error(), "region") {
				t.Errorf("error should mention the region, got: %v", err)
			}
		})
	}
}

func TestAnImpossibleRegionIsRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"latitude past the pole", []string{"--region-lat", "95", "--region-lon", "0", "--region-radius-km", "10"}},
		{"negative radius", []string{"--region-lat", "35", "--region-lon", "24", "--region-radius-km", "-5"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := config.Parse(tc.args, noEnv); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

// The reconnect floor is a measured constraint, not a preference. Letting it be
// configured below 15s would produce a bridge that can never reconnect.
func TestAReconnectFloorBelowTheMeasuredMinimumIsRejected(t *testing.T) {
	_, err := config.Parse([]string{"--reconnect-min", "1s"}, noEnv)
	if err == nil {
		t.Fatal("a one second reconnect floor was accepted")
	}
	if !strings.Contains(err.Error(), "throttl") {
		t.Errorf("the error should explain why, got: %v", err)
	}

	if _, err := config.Parse([]string{"--reconnect-min", "15s"}, noEnv); err != nil {
		t.Errorf("15s should be accepted: %v", err)
	}
}

func TestReconnectMaxMustNotBeBelowReconnectMin(t *testing.T) {
	if _, err := config.Parse([]string{"--reconnect-min", "30s", "--reconnect-max", "10s"}, noEnv); err == nil {
		t.Error("expected an error")
	}
}

// A dedupe window shorter than the replay window would let a replay through,
// which is the exact failure the layers exist to prevent.
func TestADedupeWindowShorterThanTheReplayWindowIsRejected(t *testing.T) {
	_, err := config.Parse([]string{"--dedupe-ttl", "1m"}, noEnv)
	if err == nil {
		t.Fatal("a 1 minute dedupe window was accepted against a 5 minute replay window")
	}
	if !strings.Contains(err.Error(), "replay") {
		t.Errorf("the error should mention the replay, got: %v", err)
	}
}

func TestAMaskSelectingNoNetworkIsRejected(t *testing.T) {
	if _, err := config.Parse([]string{"--src-mask", "0"}, noEnv); err == nil {
		t.Error("a mask of zero was accepted")
	}
}

// The default mask must be LightningMaps.org. Getting this wrong once delivered
// Blitzortung strokes under the belief that they were LightningMaps'.
func TestTheDefaultSourceMaskIsLightningMaps(t *testing.T) {
	cfg, err := config.Parse(nil, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.SourceMask != upstream.MaskLightningMaps {
		t.Errorf("default src-mask = %d, want %d", cfg.SourceMask, upstream.MaskLightningMaps)
	}
}

// Environment variables must be able to set the region, so a secret does not end
// up in a unit file's argument list.
func TestTheEnvironmentCanConfigureTheRegion(t *testing.T) {
	cfg, err := config.Parse(nil, env(map[string]string{
		"LIGHTNINGFEED_REGION_NAME":      "from-env",
		"LIGHTNINGFEED_REGION_LAT":       "35.3340688",
		"LIGHTNINGFEED_REGION_LON":       "24.4944483",
		"LIGHTNINGFEED_REGION_RADIUS_KM": "10",
		"LIGHTNINGFEED_BOUNDARY_POLICY":  "exclude",
	}))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !cfg.RegionConfigured() {
		t.Fatal("region should be configured from the environment")
	}
	if cfg.RegionName != "from-env" {
		t.Errorf("name = %q, want from-env", cfg.RegionName)
	}
	if cfg.BoundaryPolicy != geo.PolicyExclude {
		t.Errorf("policy = %q, want exclude", cfg.BoundaryPolicy)
	}
}

// The environment overrides flags, so a deployment can replace a baked-in value
// without editing the unit file.
func TestTheEnvironmentOverridesFlags(t *testing.T) {
	cfg, err := config.Parse([]string{"--nats-url", "nats://from-flag:4222"}, env(map[string]string{
		"LIGHTNINGFEED_NATS_URL": "nats://from-env:4222",
	}))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.NATSURL != "nats://from-env:4222" {
		t.Errorf("NATSURL = %q, want the environment value", cfg.NATSURL)
	}
}

func TestAnUnparseableEnvironmentValueIsRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"latitude is text", map[string]string{
			"LIGHTNINGFEED_REGION_LAT": "north",
			"LIGHTNINGFEED_REGION_LON": "24", "LIGHTNINGFEED_REGION_RADIUS_KM": "10",
		}, "LIGHTNINGFEED_REGION_LAT"},
		{"unknown policy", map[string]string{
			"LIGHTNINGFEED_BOUNDARY_POLICY": "maybe",
		}, "LIGHTNINGFEED_BOUNDARY_POLICY"},
		{"unknown log level", map[string]string{
			"LIGHTNINGFEED_LOG_LEVEL": "chatty",
		}, "LIGHTNINGFEED_LOG_LEVEL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(nil, env(tc.env))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should name the offending variable %s, got: %v", tc.want, err)
			}
		})
	}
}

// A region without a name gets a derived one, so metrics are never labelled with
// an empty string.
func TestAnUnnamedRegionGetsADerivedName(t *testing.T) {
	cfg, err := config.Parse([]string{
		"--region-lat", "35.3340688",
		"--region-lon", "24.4944483",
		"--region-radius-km", "10",
	}, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.RegionName == "" {
		t.Error("expected a derived region name")
	}
}

// The cells a region publishes to must be the verified set.
func TestTheRegionsCellsAreTheVerifiedSet(t *testing.T) {
	cfg, err := config.Parse(regionFlags, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cells, err := cfg.Cells()
	if err != nil {
		t.Fatalf("Cells: %v", err)
	}
	if len(cells) != 30 {
		t.Errorf("got %d cells, want 30 for a 10 km radius at precision 5", len(cells))
	}

	// Every point inside the circle must fall in one of them, or strokes would
	// be dropped without any error being reported.
	for deg := range 360 {
		angle := float64(deg) * 3.141592653589793 / 180
		lat := cfg.Region.Lat + 10*0.017453293/111.32*cosAngle(angle)
		lon := cfg.Region.Lon + 10*0.017453293/111.32/cosLatitude(cfg.Region.Lat)*sinAngle(angle)
		cell, err := geo.Encode(lat, lon, 5)
		if err != nil {
			continue
		}
		found := false
		for _, c := range cells {
			if c == cell {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("point at %d degrees maps to cell %q, which the region does not publish", deg, cell)
		}
	}
}

func TestViewportCoversTheWholeRegion(t *testing.T) {
	cfg, err := config.Parse(regionFlags, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	v := cfg.Viewport()

	if v.North < 35.4238 || v.North > 35.4240 {
		t.Errorf("north edge = %v, want 35.4239", v.North)
	}
	// The box must extend a full radius beyond the centre in every direction.
	if v.West > 24.3844 || v.West < 24.3842 {
		t.Errorf("west edge = %v, want 24.3843", v.West)
	}
	if v.East < 24.6045 || v.East > 24.6047 {
		t.Errorf("east edge = %v, want 24.6046", v.East)
	}
	if v.South > 35.2443 || v.South < 35.2441 {
		t.Errorf("south edge = %v, want 35.2442", v.South)
	}
}

func TestDurationsParseFromFlags(t *testing.T) {
	cfg, err := config.Parse([]string{
		"--retention", "48h",
		"--prune-every", "30m",
		"--idle-timeout", "1m",
	}, noEnv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Retention != 48*time.Hour {
		t.Errorf("retention = %v, want 48h", cfg.Retention)
	}
	if cfg.PruneEvery != 30*time.Minute {
		t.Errorf("prune-every = %v, want 30m", cfg.PruneEvery)
	}
	if cfg.IdleTimeout != time.Minute {
		t.Errorf("idle-timeout = %v, want 1m", cfg.IdleTimeout)
	}
}

// cosAngle and sinAngle keep the cell-coverage loop readable without importing
// math into every line of it.
func cosAngle(rad float64) float64 { return math.Cos(rad) }
func sinAngle(rad float64) float64 { return math.Sin(rad) }

func cosLatitude(lat float64) float64 { return math.Cos(lat * math.Pi / 180) }
