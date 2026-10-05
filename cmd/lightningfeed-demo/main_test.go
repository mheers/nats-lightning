package main

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/mheers/nats-lightning/internal/config"
	"github.com/mheers/nats-lightning/internal/feed"
	"github.com/mheers/nats-lightning/internal/geo"
)

// roussospiti is the region the README documents, so a test that draws it is
// drawing the same thing a reader will.
var roussospiti = geo.Circle{Lat: 35.3340688, Lon: 24.4944483, RadiusKm: 10}

// regionConfig builds the configuration the bridge would have for a circle, with
// the source mask a test asks for.
//
// It goes through the real parser rather than assembling a Config literal,
// because what is under test is the subject list a *parsed* configuration
// produces: a hand-built one would skip the validation that decides whether a
// region is configured at all.
func regionConfig(t *testing.T, region geo.Circle, mask int) *config.Config {
	t.Helper()

	args := []string{fmt.Sprintf("--src-mask=%d", mask)}
	if region.RadiusKm > 0 {
		args = append(args,
			fmt.Sprintf("--region-lat=%v", region.Lat),
			fmt.Sprintf("--region-lon=%v", region.Lon),
			fmt.Sprintf("--region-radius-km=%v", region.RadiusKm),
		)
	}

	cfg, err := config.Parse(args, func(string) string { return "" })
	if err != nil {
		t.Fatalf("config.Parse(%v): %v", args, err)
	}
	return cfg
}

// cosLatOf is the cosine the projection uses, including its clamp.
//
// Duplicated rather than imported so that a change to the clamp is a change to
// this test too, rather than both moving together and agreeing on a wrong
// window.
func cosLatOf(lat float64) float64 {
	c := math.Cos(lat * math.Pi / 180)
	if c < 0.05 {
		return 0.05
	}
	return c
}

func regionName(c geo.Circle) string {
	return fmt.Sprintf("%.2f_%.2f", c.Lat, c.Lon)
}

// A region window must be square in kilometres, or a circle is drawn as an
// ellipse and the map misreports the only geometry on it. This is the test that
// catches a change to the longitude correction.
func TestProjectionWindowIsSquareInKilometres(t *testing.T) {
	for _, region := range []geo.Circle{
		roussospiti,
		{Lat: 0, Lon: 0, RadiusKm: 50},           // on the equator: the widest case
		{Lat: 60, Lon: 5, RadiusKm: 50},          // high latitude: where the factor bites
		{Lat: -33.87, Lon: 151.21, RadiusKm: 20}, // southern hemisphere
	} {
		t.Run(regionName(region), func(t *testing.T) {
			p := newProjection(region, mapCols)

			// The corner distances are the diagonal of the window in each axis's own
			// units, so comparing the full spans is the direct statement of the
			// property: same kilometres north to south as east to west.
			kmNS := p.latSpan * kmPerDegLat
			kmEW := p.lonSpan * kmPerDegLat * cosLatOf(region.Lat)

			if diff := kmNS - kmEW; diff > 0.01 || diff < -0.01 {
				t.Errorf("window is %.1f km north-south and %.1f km east-west, want equal", kmNS, kmEW)
			}
		})
	}
}

func cosLatOfUnused() {}

func regionNameUnused() {}

// The centre must land in the middle of the grid. An even number of columns has
// two equally central ones, and either is correct — what would be wrong is the
// centre landing near an edge, which would put the whole region off-centre.
func TestProjectionPlacesTheCentreInTheMiddle(t *testing.T) {
	p := newProjection(roussospiti, mapCols)

	col, row, ok := p.project(roussospiti.Lat, roussospiti.Lon)
	if !ok {
		t.Fatal("the region centre is outside its own window")
	}
	lo, hi := (mapCols-1)/2, mapCols/2
	if col < lo || col > hi {
		t.Errorf("centre column = %d, want one of the central columns %d or %d", col, lo, hi)
	}
	if want := (mapRows - 1) / 2; row != want {
		t.Errorf("centre row = %d, want %d", row, want)
	}
}

// A point in every direction from the centre has to land in the window, and one
// far outside it has to be rejected rather than clamped onto an edge. Clamping
// would pile up strokes from another continent along the border of the map.
func TestProjectionCoversTheRegionAndRejectsTheRest(t *testing.T) {
	p := newProjection(roussospiti, mapCols)

	// The four compass points at the radius, in degrees.
	for _, tc := range []struct {
		name string
		lat  float64
		lon  float64
	}{
		{"north", roussospiti.Lat + roussospiti.RadiusKm/kmPerDegLat, roussospiti.Lon},
		{"south", roussospiti.Lat - roussospiti.RadiusKm/kmPerDegLat, roussospiti.Lon},
		{"east", roussospiti.Lat, roussospiti.Lon + roussospiti.RadiusKm/(kmPerDegLat*cosLatOf(roussospiti.Lat))},
		{"west", roussospiti.Lat, roussospiti.Lon - roussospiti.RadiusKm/(kmPerDegLat*cosLatOf(roussospiti.Lat))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, ok := p.project(tc.lat, tc.lon); !ok {
				t.Errorf("a point %s of the radius falls outside the window", tc.name)
			}
		})
	}

	for _, tc := range []struct {
		name string
		lat  float64
		lon  float64
	}{
		{"Cape Town", -33.92, 18.42},
		{"Reykjavik", 64.15, -21.94},
		{"null island", 0, 0},
	} {
		t.Run("outside "+tc.name, func(t *testing.T) {
			if _, _, ok := p.project(tc.lat, tc.lon); ok {
				t.Errorf("%s is reported as inside the window", tc.name)
			}
		})
	}
}

// A world-wide demo has no region to draw, so it draws the whole planet. The
// window must still be finite — a division by a cosine of zero would make the
// longitude span infinite — and it must reject the poles, because no stroke is
// ever reported within five degrees of either and two permanently empty rows tell
// the reader nothing.
func TestWorldWideProjectionIsFiniteAndCoversTheHabitableBands(t *testing.T) {
	p := newProjection(geo.Circle{}, mapCols)

	if math.IsInf(p.latSpan, 0) || math.IsInf(p.lonSpan, 0) {
		t.Fatalf("world-wide span is not finite: lat %v lon %v", p.latSpan, p.lonSpan)
	}

	for _, tc := range []struct {
		name string
		lat  float64
		lon  float64
	}{
		{"the default region", 35.3340688, 24.4944483},
		{"Cape Town", -33.92, 18.42},
		{"Reykjavik", 64.15, -21.94},
		{"just inside the north cut-off", 84.9, 179.9},
		{"just inside the south cut-off", -84.9, -179.9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, ok := p.project(tc.lat, tc.lon); !ok {
				t.Errorf("%v,%v is outside a world-wide window", tc.lat, tc.lon)
			}
		})
	}

	for _, tc := range []struct {
		name string
		lat  float64
		lon  float64
	}{
		{"north pole", 89.9, 0},
		{"south pole", -89.9, 0},
	} {
		t.Run(tc.name+" is excluded", func(t *testing.T) {
			if _, _, ok := p.project(tc.lat, tc.lon); ok {
				t.Errorf("%v,%v is inside the window, but the poles are meant to be cut off", tc.lat, tc.lon)
			}
		})
	}
}

// The newest stroke wins a shared cell, whichever order the markers arrive in.
// Deciding this by iteration order would make the map's picture depend on a
// detail of the consumer that has nothing to do with the weather.
func TestNewestMarkerWinsRegardlessOfOrder(t *testing.T) {
	now := time.Now()
	p := newProjection(roussospiti, mapCols)

	newest := marker{lat: roussospiti.Lat, lon: roussospiti.Lon, at: now}
	older := marker{lat: roussospiti.Lat, lon: roussospiti.Lon, at: now.Add(-time.Minute)}

	for _, tc := range []struct {
		name     string
		markers  []marker
		wantRune rune
	}{
		{"newest first", []marker{newest, older}, '*'},
		{"newest last", []marker{older, newest}, '*'},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines := renderMap(p, tc.markers, now, defaultFade)
			if !strings.Contains(lines[(mapRows-1)/2], string(tc.wantRune)) {
				t.Errorf("cell holds %q, want the newest marker %q",
					lines[(mapRows-1)/2], string(tc.wantRune))
			}
		})
	}
}

// Age has to be visible. A map that draws a stroke from three minutes ago the
// same way as one from three seconds ago cannot tell an active cell from a dead
// one, which is the thing the map is for.
func TestMarkersFadeWithAge(t *testing.T) {
	fade := defaultFade

	for _, tc := range []struct {
		name string
		age  time.Duration
		want rune
	}{
		{"just now", 0, '*'},
		{"a few seconds", 5 * time.Second, '*'},
		{"half way", fade / 2, '+'},
		{"nearly gone", fade - time.Second, ':'},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := faded(tc.age, fade); got != tc.want {
				t.Errorf("faded(%v) = %q, want %q", tc.age, string(got), string(tc.want))
			}
		})
	}
}

// The region itself must be visible under the strokes, or the map is a
// scatter plot with no scale and no way to tell inside from outside.
func TestRegionIsDrawnAndStrokesLandInsideIt(t *testing.T) {
	p := newProjection(roussospiti, mapCols)

	// Just inside the radius on each side, which is where a stroke that matters
	// most sits: close enough to be interesting, close enough to the edge that a
	// mis-drawn region would put it outside.
	inside := roussospiti.RadiusKm * 0.9
	north := roussospiti.Lat + inside/kmPerDegLat
	col, row, ok := p.project(north, roussospiti.Lon)
	if !ok {
		t.Fatal("a stroke just inside the region has no cell")
	}

	canvas := p.canvas()
	if canvas[row][col] != '·' {
		t.Errorf("cell holds %q, want the region fill %q", string(canvas[row][col]), "·")
	}
}

// Every frame must be the same height, because the redraw moves the cursor up by
// exactly as many lines as the previous frame printed. A frame that changes
// height leaves stale rows behind and slowly corrupts the display.
func TestMapHeightIsStable(t *testing.T) {
	now := time.Now()
	p := newProjection(roussospiti, mapCols)

	empty := renderMap(p, nil, now, defaultFade)

	markers := make([]marker, 0, 200)
	for i := range 200 {
		markers = append(markers, marker{
			lat: roussospiti.Lat + float64(i%20)*0.01 - 0.1,
			lon: roussospiti.Lon + float64(i/20)*0.01 - 0.1,
			at:  now.Add(-time.Duration(i) * time.Second),
		})
	}
	full := renderMap(p, markers, now, defaultFade)

	if len(empty) != len(full) {
		t.Errorf("frame height changed from %d to %d lines", len(empty), len(full))
	}
	// The map itself, a row of longitude labels, and a legend.
	if want := mapRows + 2; len(empty) != want {
		t.Errorf("frame is %d lines, want %d: %d map rows, longitude labels and a legend",
			len(empty), want, mapRows)
	}
}

// The map has to say where it is. Without coordinates on the edges a world-wide view
// is an unlabelled grid, and there is no way to tell a cluster over the Atlantic
// from one over Africa — which is the only question a world-wide map exists to
// answer.
func TestMapIsLabelledWithCoordinates(t *testing.T) {
	world := newProjection(geo.Circle{}, mapCols)
	lines := renderMap(world, nil, time.Now(), defaultFade)

	// Every line, gutters aside, has to fit the grid, or the strokes will not sit
	// above the coordinates printed beneath them.
	widest := mapCols + world.gutter + 1
	for i, line := range lines {
		if n := len([]rune(line)); n > widest {
			t.Errorf("line %d is %d wide, wider than the map's %d: %q", i, n, widest, line)
		}
	}

	// The world-wide window runs from 85N to 85S, so the top row's label is the
	// northern edge of it and the bottom row's is the southern.
	if lat := lines[0]; !strings.Contains(lat, "85N") {
		t.Errorf("the first row carries no latitude label: %q", lat)
	}
	if lat := lines[mapRows-1]; !strings.Contains(lat, "85S") {
		t.Errorf("the last row carries no latitude label: %q", lat)
	}

	// Labels go on round meridians, because that is the only kind a reader can place
	// a feature against.
	lon := lines[mapRows]
	for _, want := range []string{"180W", "90W", "90E", "180E"} {
		if !strings.Contains(lon, want) {
			t.Errorf("longitude row %q does not mention %q", lon, want)
		}
	}
}

// Labels have to be distinguishable. A 25 km region spans about a third of a degree,
// so whole-degree labels all read "35N" and the grid conveys nothing — and the
// longitude row must line up with the map above it, which means sharing its gutter.
func TestRegionalLabelsAreDistinguishableAndAligned(t *testing.T) {
	region := geo.Circle{Lat: 35.3340688, Lon: 24.4944483, RadiusKm: 25}
	p := newProjection(region, mapCols)
	lines := renderMap(p, nil, time.Now(), defaultFade)

	var lats []string
	for row := 0; row < mapRows; row += labelEvery {
		fields := strings.Fields(lines[row])
		if len(fields) == 0 {
			t.Fatalf("row %d has no latitude label: %q", row, lines[row])
		}
		lats = append(lats, fields[0])
	}
	seen := map[string]bool{}
	for _, l := range lats {
		if seen[l] {
			t.Errorf("latitude label %q appears twice, so the grid says nothing", l)
		}
		seen[l] = true
	}
	if len(lats) < 3 {
		t.Errorf("only %d latitude labels for a %d row grid", len(lats), mapRows)
	}

	// The coordinate row and the legend both start under the grid, not under the
	// gutter, or they appear to label the labels.
	indent := len(lats[0]) + 1
	for _, line := range []string{lines[mapRows], lines[mapRows+1]} {
		if n := len(line) - len(strings.TrimLeft(line, " ")); n != indent {
			t.Errorf("row starts at column %d, want %d: %q", n, indent, line)
		}
	}
	if !strings.Contains(lines[mapRows+1], "inside radius") {
		t.Errorf("the legend is not where the coordinates are: %q", lines[mapRows+1])
	}
}

// A world-wide view has no region, so the legend must not send the reader looking
// for a circle that was never drawn.
func TestLegendOnlyMentionsWhatIsDrawn(t *testing.T) {
	world := newProjection(geo.Circle{}, mapCols).legend()
	if strings.Contains(world, "inside radius") || strings.Contains(world, "region centre") {
		t.Errorf("the world-wide legend describes a region: %q", world)
	}
	if !strings.Contains(world, "now") {
		t.Errorf("the world-wide legend does not explain the stroke glyphs: %q", world)
	}

	region := newProjection(roussospiti, mapCols).legend()
	if !strings.Contains(region, "inside radius") || !strings.Contains(region, "region centre") {
		t.Errorf("the regional legend omits the region: %q", region)
	}
}

// The sparkline has to show shape. A row that renders every bucket identically
// is not reporting a rate.
func TestSparklineShowsRelativeHeights(t *testing.T) {
	got := sparkline([]int{0, 1, 2, 8})
	if n := len([]rune(got)); n != 4 {
		t.Fatalf("sparkline is %d glyphs wide, want 4", n)
	}
	runes := []rune(got)
	if runes[0] != '▁' {
		t.Errorf("an empty bucket drew %q, want the lowest block", string(runes[0]))
	}
	if runes[3] != '█' {
		t.Errorf("the busiest bucket drew %q, want a full block", string(runes[3]))
	}
	if runes[1] == runes[3] {
		t.Errorf("a bucket of 1 drew the same as the peak of 8: %q", string(runes[1]))
	}
}

// A single stroke must be visible rather than rounding down to nothing, and an
// all-zero row must not divide by zero.
func TestSparklineHandlesFlatAndEmptyRows(t *testing.T) {
	if got := sparkline(nil); got != "" {
		t.Errorf("sparkline(nil) = %q, want empty", got)
	}
	if got := sparkline([]int{0, 0, 0}); len([]rune(got)) != 3 {
		t.Errorf("sparkline of zeros = %q, want three glyphs", got)
	}
	// A peak of 1 is the division-by-zero case: every bucket is equal to the peak.
	if got := sparkline([]int{1, 1}); len([]rune(got)) != 2 {
		t.Errorf("sparkline of ones = %q, want two glyphs", got)
	}
}

// The demo's flags are parsed before the bridge's. A demo flag must be consumed
// by the demo, and everything else must survive untouched: a region flag the demo
// swallowed would be silently ignored, and the demo would then watch a region
// other than the one that was asked for.
func TestDemoFlagParsing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		want     options
		wantRest []string
		wantErr  bool
	}{
		{name: "duration", args: []string{"--duration", "5m"}, want: options{duration: 5 * time.Minute}},
		{name: "duration inline", args: []string{"--duration=90s"}, want: options{duration: 90 * time.Second}},
		{name: "fade", args: []string{"--fade", "30s"}, want: options{fade: 30 * time.Second}},
		{name: "limit", args: []string{"--limit", "5"}, want: options{limit: 5}},
		{name: "durable", args: []string{"--durable", "mine"}, want: options{durable: "mine"}},
		{name: "view", args: []string{"--view", "log"}, want: options{view: "log"}},
		{name: "replay bare", args: []string{"--replay"}, want: options{replay: true}},
		{name: "replay explicit", args: []string{"--replay=false"}, want: options{replay: false}},
		{
			name:     "bridge flags pass through",
			args:     []string{"--view", "log", "--region-lat=48", "--region-radius-km=10"},
			want:     options{view: "log"},
			wantRest: []string{"--region-lat=48", "--region-radius-km=10"},
		},
		{
			name:     "order does not matter",
			args:     []string{"--region-lat=48", "--duration", "2m", "--region-radius-km=10"},
			want:     options{duration: 2 * time.Minute},
			wantRest: []string{"--region-lat=48", "--region-radius-km=10"},
		},
		{name: "negative limit", args: []string{"--limit", "-1"}, wantErr: true},
		{name: "unknown view", args: []string{"--view", "grid"}, wantErr: true},
		{name: "zero duration", args: []string{"--duration", "0s"}, wantErr: true},
		{name: "missing value", args: []string{"--limit"}, wantErr: true},
		{name: "unparseable duration", args: []string{"--duration", "soon"}, wantErr: true},
		{name: "unparseable replay", args: []string{"--replay=perhaps"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got options
			rest, err := extractDemoFlags(tc.args, &got)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("no error for %v", tc.args)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("options = %+v, want %+v", got, tc.want)
			}
			if len(rest) != len(tc.wantRest) {
				t.Fatalf("passed through %v, want %v", rest, tc.wantRest)
			}
			for i := range rest {
				if rest[i] != tc.wantRest[i] {
					t.Errorf("passed through %v, want %v", rest, tc.wantRest)
					break
				}
			}
		})
	}
}

// The demo's help has to reach the reader. Flag parsing is disabled, so cobra
// cannot see --help and the flag package would otherwise print the bridge's usage
// instead: no demo flags at all, and nothing about the map.
func TestHelpIsTheDemOsOwn(t *testing.T) {
	for _, arg := range []string{"-h", "--help", "help"} {
		if !wantsHelp([]string{arg}) {
			t.Errorf("%q does not ask for help", arg)
		}
	}

	for _, arg := range []string{"--view", "--duration", "--region-lat=48", "--replay"} {
		if wantsHelp([]string{arg}) {
			t.Errorf("%q is treated as a request for help", arg)
		}
	}

	// Help wins over everything else: a reader who asked for it is not trying to
	// start a subscription and should not end up connected to one.
	if !wantsHelp([]string{"--view", "map", "--help"}) {
		t.Error("help alongside other flags was not recognised")
	}

	// The text has to answer the questions the bridge's usage would have.
	for _, want := range []string{"--view map|log", "lightningfeed cells", "LIGHTNINGFEED_REGION_LAT"} {
		if !strings.Contains(helpText, want) {
			t.Errorf("the help text does not mention %q", want)
		}
	}

	// The cleanup command has to be the non-interactive one. Left without --force
	// the nats CLI stops to ask for confirmation, so the command as documented works
	// only by hand and hangs in a script — which is where somebody reading this after
	// a wrong region will be running it.
	if !strings.Contains(helpText, "consumer rm LIGHTNING lightningfeed-demo --force") {
		t.Error("the help text does not give a non-interactive consumer rm command")
	}
}

// --version has to be intercepted for the same reason --help is: cobra cannot see
// it, and the flag package would print the bridge's whole flag list and exit
// non-zero instead of answering the question.
func TestVersionIsReported(t *testing.T) {
	for _, arg := range []string{"-v", "--version", "version"} {
		if !wantsVersion([]string{arg}) {
			t.Errorf("%q does not ask for the version", arg)
		}
	}
	for _, arg := range []string{"--view", "--region-lat=48", "--limit", "5"} {
		if wantsVersion([]string{arg}) {
			t.Errorf("%q is treated as a request for the version", arg)
		}
	}
	// Help takes precedence, since it is the longer answer.
	if !wantsHelp([]string{"--version", "--help"}) {
		t.Error("--help alongside --version is not recognised as a help request")
	}

	if got := buildVersion(); got == "" {
		t.Error("buildVersion() is empty, so --version would print nothing")
	}
}

// The elapsed time and the rate are measured from the demo's own start, never from
// the first stroke. A demo watching a quiet region reports "0s elapsed" and a rate of
// hundreds a minute on the stroke that finally arrives, and neither number is about
// anything.
func TestElapsedTimeIsMeasuredFromTheStartNotTheFirstStroke(t *testing.T) {
	started := time.Now().Add(-10 * time.Minute)
	r := &reader{
		startedAt: started,
		buckets:   make([]int, bucketCount),
		bucketAt:  started,
		inside:    1,
		lastAt:    started.Add(10 * time.Minute),
		byBand:    map[geo.Certainty]int{geo.CertaintyIn: 1},
		byNet:     map[string]int{"lightningmaps.org": 1},
	}

	lines := r.statusLines(started.Add(10*time.Minute + time.Second))
	head := lines[0]

	if !strings.Contains(head, "10m elapsed") {
		t.Errorf("status line does not report ten minutes of watching: %q", head)
	}
	if strings.Contains(head, "0s elapsed") {
		t.Errorf("status line reports no elapsed time: %q", head)
	}
	// One stroke over ten minutes is 0.1/min. A rate computed over the gap since the
	// stroke arrived would be a large number, and would look like a storm.
	if !strings.Contains(head, "0.1/min") {
		t.Errorf("rate is not measured over the demo's own runtime: %q", head)
	}
}

// The subject list is the one thing a consumer has to get right, and getting it
// wrong is silent: a subscriber to the wrong subjects simply sees nothing.
func TestSubjectsCoverEverySourceAndCell(t *testing.T) {
	cfg := regionConfig(t, roussospiti, 6) // both networks

	subjects, err := subjectsFor(cfg)
	if err != nil {
		t.Fatalf("subjectsFor: %v", err)
	}

	cells, err := cfg.Cells()
	if err != nil {
		t.Fatalf("cells: %v", err)
	}
	want := len(cells) * 2
	if len(subjects) != want {
		t.Errorf("got %d subjects, want %d for %d cells and 2 networks", len(subjects), want, len(cells))
	}

	// Each network needs its own subjects. A mask selecting two networks has to
	// yield both prefixes, or a subscriber silently misses one of them.
	for _, src := range []string{"src.1.", "src.2."} {
		found := false
		for _, s := range subjects {
			if strings.Contains(s, src) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no subject for %s: %v", src, subjects)
		}
	}

	for _, s := range subjects {
		if !strings.HasPrefix(s, feed.SubjectPrefix+".src.") {
			t.Errorf("subject %q is outside the namespace", s)
		}
	}
}

// With no region there is no cell list, so the whole namespace is subscribed to.
// Anything narrower would silently miss strokes for a world-wide deployment.
func TestSubjectsAreWorldWideWithoutARegion(t *testing.T) {
	cfg := regionConfig(t, geo.Circle{}, 4)

	subjects, err := subjectsFor(cfg)
	if err != nil {
		t.Fatalf("subjectsFor: %v", err)
	}
	if len(subjects) != 1 || subjects[0] != feed.SubjectPrefix+".>" {
		t.Errorf("subjects = %v, want [%s.>]", subjects, feed.SubjectPrefix)
	}
}

// A mask selecting nothing that carries data has no subjects at all, and
// subscribing to none would look like a quiet region rather than a mistake.
func TestSubjectsRejectAMaskThatCarriesNothing(t *testing.T) {
	cfg := regionConfig(t, roussospiti, 1) // the reserved bit

	if _, err := subjectsFor(cfg); err == nil {
		t.Error("a mask selecting no network produced subjects instead of an error")
	}
}
