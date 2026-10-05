package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/mheers/nats-lightning/internal/upstream"
)

// clearEnv neutralises every LIGHTNINGFEED_ variable for the duration of a test.
//
// The cells command resolves its configuration through config.ParseFromEnv, which
// reads the real process environment. A developer with any of these exported —
// which is exactly what a deployment does — would otherwise get output this test
// did not ask for. applyEnv skips empty values, so setting them to "" is enough to
// make them invisible while still restoring whatever was there afterwards.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, o := range []string{
		"NATS_URL", "STREAM", "REGION_NAME", "REGION_LAT", "REGION_LON",
		"REGION_RADIUS_KM", "BOUNDARY_POLICY", "UPSTREAM_URL", "SRC_MASK",
		"SQLITE", "RETENTION", "PUBLISH_ATTEMPTS", "PUBLISH_BACKOFF",
		"LEADER_ELECT", "METRICS_ADDR", "LOG_LEVEL", "LOG_FORMAT",
	} {
		t.Setenv("LIGHTNINGFEED_"+o, "")
	}
	// A stray value in the ambient environment would still be inherited by the
	// process, and t.Setenv above cannot unset what it does not know about.
	os.Unsetenv("LIGHTNINGFEED_UNKNOWN")
}

// run executes the root command with args and returns everything it wrote.
func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	clearEnv(t)

	var out bytes.Buffer
	root := newRoot()
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)

	err := root.Execute()
	return out.String(), err
}

// cellsArgs is a region small enough to land in a single geohash-5 cell, which
// keeps the expected output readable.
var cellsArgs = []string{
	"cells",
	"--region-name=munich",
	"--region-lat=48.0",
	"--region-lon=11.0",
	"--region-radius-km=1",
}

// TestCellsCommandEmitsEverySelectedSource is the regression test for the
// multi-source fix.
//
// The subject carries the source code and the two networks issue independent id
// sequences, so a mask selecting both networks needs subjects for both. The
// command used to let each matching bit overwrite the last, printing only one
// network's subjects — handing a subscriber a silently incomplete subscription
// list, and doing so more confidently the wider the mask.
func TestCellsCommandEmitsEverySelectedSource(t *testing.T) {
	tests := []struct {
		name string
		mask string
		want string
	}{
		{
			name: "one network prints one section",
			mask: "4",
			want: "munich: 1 cells, radius 1 km around 48.0000,11.0000\n" +
				"sources: src.2\n" +
				"\nlightningmaps.org\n" +
				"lightning.v1.src.2.cell.u0xc4\n",
		},
		{
			// This is the case that was broken. Both sections must appear.
			name: "two networks print both sections",
			mask: "6",
			want: "munich: 1 cells, radius 1 km around 48.0000,11.0000\n" +
				"sources: src.1, src.2\n" +
				"\nblitzortung.org\n" +
				"lightning.v1.src.1.cell.u0xc4\n" +
				"\nlightningmaps.org\n" +
				"lightning.v1.src.2.cell.u0xc4\n",
		},
		{
			// Guards the regression from the other side: the fix must not invent
			// sources the mask did not select.
			name: "a narrower mask narrows the output",
			mask: "2",
			want: "munich: 1 cells, radius 1 km around 48.0000,11.0000\n" +
				"sources: src.1\n" +
				"\nblitzortung.org\n" +
				"lightning.v1.src.1.cell.u0xc4\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := run(t, append(append([]string{}, cellsArgs...), "--src-mask="+tc.mask)...)
			if err != nil {
				t.Fatalf("cells --src-mask=%s: %v", tc.mask, err)
			}
			if got != tc.want {
				t.Errorf("cells --src-mask=%s output:\n%q\nwant:\n%q", tc.mask, got, tc.want)
			}
		})
	}
}

func TestCellsCommandWithoutRegion(t *testing.T) {
	const want = "no region configured; every subject is published\n"

	got, err := run(t, "cells")
	if err != nil {
		t.Fatalf("cells: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestCellsCommandMaskOfOnlyEmptyNetworks covers the distinct message, because
// "your region is fine but nothing will ever be published to it" and "your region
// produced no cells" are different problems with different fixes.
func TestCellsCommandMaskOfOnlyEmptyNetworks(t *testing.T) {
	want := "munich: 1 cells, radius 1 km around 48.0000,11.0000\n" +
		"\nno source carries data; the mask selects only networks observed to be empty\n"

	for _, mask := range []string{"1", "8"} {
		got, err := run(t, append(append([]string{}, cellsArgs...), "--src-mask="+mask)...)
		if err != nil {
			t.Fatalf("cells --src-mask=%s: %v", mask, err)
		}
		if got != want {
			t.Errorf("--src-mask=%s output:\n%q\nwant:\n%q", mask, got, want)
		}
		if strings.Contains(got, "cell.u0xc4") {
			t.Errorf("--src-mask=%s published a subject for a network that carries nothing", mask)
		}
	}
}

// TestCellsCommandRejectsPartialRegion checks the message a user actually sees
// when they supply only a latitude, which is the common typo.
func TestCellsCommandRejectsPartialRegion(t *testing.T) {
	_, err := run(t, "cells", "--region-lat=48.0")
	if err == nil {
		t.Fatal("a region with only a latitude was accepted")
	}
	if !strings.Contains(err.Error(), "region") {
		t.Errorf("error %q does not mention the region", err)
	}
}

func TestSelectedSources(t *testing.T) {
	tests := []struct {
		name string
		mask upstream.SrcMask
		want []int
	}{
		{"empty mask selects nothing", 0, nil},
		{"lightningmaps.org", upstream.MaskLightningMaps, []int{2}},
		{"blitzortung.org", upstream.MaskBlitzortung, []int{1}},
		{"both networks, in bit order", upstream.MaskBlitzortung | upstream.MaskLightningMaps, []int{1, 2}},
		// The reserved and testing bits were observed to carry no data at all, so
		// printing them would list subjects nothing is ever published to.
		{"reserved bit is skipped", upstream.MaskReserved, nil},
		{"testing bit is skipped", upstream.MaskTesting, nil},
		{"empty bits are skipped, real ones kept", upstream.MaskReserved | upstream.MaskTesting | upstream.MaskLightningMaps, []int{2}},
		{"default mask", upstream.DefaultSrcMask, []int{2}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := selectedSources(tc.mask)
			if len(got) != len(tc.want) {
				t.Fatalf("selectedSources(%d) = %v, want %v", tc.mask, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("selectedSources(%d) = %v, want %v", tc.mask, got, tc.want)
				}
			}
		})
	}
}

func TestJoinSources(t *testing.T) {
	tests := []struct {
		name string
		in   []int
		want string
	}{
		{"none", nil, ""},
		{"one", []int{2}, "src.2"},
		{"two", []int{1, 2}, "src.1, src.2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinSources(tc.in); got != tc.want {
				t.Errorf("joinSources(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestExtractHistoryFlags(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantRest  []string
		wantSince string
		wantLimit int
	}{
		{
			name:     "no history flags",
			args:     []string{"--sqlite=/tmp/x.db"},
			wantRest: []string{"--sqlite=/tmp/x.db"},
		},
		{
			name: "both spellings of both flags",
			args: []string{"--since", "2h", "--limit=50"},
			// The standard flag package accepts both spellings, so this must too.
			wantRest:  nil,
			wantSince: "2h",
			wantLimit: 50,
		},
		{
			name:      "inline since",
			args:      []string{"--since=1h"},
			wantRest:  nil,
			wantSince: "1h",
		},
		{
			name:     "unknown flags pass through for the config parser to judge",
			args:     []string{"--not-a-history-flag", "x", "--since", "1h"},
			wantRest: []string{"--not-a-history-flag", "x"},
			// The pass-through must not swallow the next argument, which is the
			// failure mode of stripping by name only.
			wantSince: "1h",
		},
		{
			// A value is never mistaken for the next flag, which is the failure
			// mode of stripping by name without consuming the value. It means
			// "--since --limit=5" sets since to the literal "--limit=5" and
			// leaves limit alone — the same thing the standard flag package does
			// with a non-boolean flag, and deliberately so, because the two
			// parsers have to agree on every argument list a caller may pass.
			name:      "a value that looks like a flag is still the value",
			args:      []string{"--since", "--limit=5"},
			wantRest:  nil,
			wantSince: "--limit=5",
			wantLimit: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var since string
			limit := 0
			rest, err := extractHistoryFlags(tc.args, &since, &limit)
			if err != nil {
				t.Fatalf("extractHistoryFlags(%v): %v", tc.args, err)
			}
			if since != tc.wantSince {
				t.Errorf("since = %q, want %q", since, tc.wantSince)
			}
			if limit != tc.wantLimit {
				t.Errorf("limit = %d, want %d", limit, tc.wantLimit)
			}
			if len(rest) != len(tc.wantRest) {
				t.Fatalf("rest = %v, want %v", rest, tc.wantRest)
			}
			for i := range rest {
				if rest[i] != tc.wantRest[i] {
					t.Fatalf("rest = %v, want %v", rest, tc.wantRest)
				}
			}
		})
	}
}

func TestExtractHistoryFlagsErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"missing since value", []string{"--since"}, "needs a value"},
		{"missing limit value", []string{"--limit"}, "needs a value"},
		{"limit is not a number", []string{"--limit=many"}, "--limit"},
		// A negative limit used to reach PrintHistory, which sliced
		// strokes[len+1:] and panicked. Rejecting it here also stops a typo from
		// being answered with the entire archive.
		{"negative limit", []string{"--limit=-1"}, "must not be negative"},
		{"large negative limit", []string{"--limit=-99999"}, "must not be negative"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var since string
			limit := 0
			_, err := extractHistoryFlags(tc.args, &since, &limit)
			if err == nil {
				t.Fatalf("extractHistoryFlags(%v) accepted it", tc.args)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}
