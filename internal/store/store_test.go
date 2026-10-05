package store_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mheers/nats-lightning/internal/geo"
	"github.com/mheers/nats-lightning/internal/model"
	"github.com/mheers/nats-lightning/internal/store"
)

// Slice 11: persistence.
//
// The seam is a real SQLite file in a temporary directory. The behaviours that
// matter here are all properties of the database rather than of this package: a
// unique constraint that makes a duplicate insert a no-op, a cursor that
// survives reopening, and an indexed query that answers "every stroke near the
// village since yesterday" quickly. A mock could only prove the code calls the
// methods the mock expected.
//
// SQLite earns its place for two things NATS cannot do. The upstream resume
// cursor is upstream state, so no broker can hold it; and "all strokes within
// 10 km over the last N hours" is a radius query, which subject filtering cannot
// express at all.

// testRegion is the configured region.
var testRegion = geo.Circle{Lat: 35.3340688, Lon: 24.4944483, RadiusKm: 10}

func openStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lightningfeed.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

func testStroke(id int64, lat, lon float64, at time.Time, dev int) model.Stroke {
	d := dev
	return model.Stroke{
		Src:        model.SourceLightningMaps,
		StrokeID:   id,
		Time:       at,
		Lat:        lat,
		Lon:        lon,
		DeviationM: &d,
	}
}

// A stroke written must be retrievable by the radius query, with the judgement
// the publisher would have made. This is the question the whole store exists to
// answer.
func TestStrokesAreRetrievableByRadius(t *testing.T) {
	s, _ := openStore(t)
	base := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

	want := []model.Stroke{
		testStroke(1, testRegion.Lat, testRegion.Lon, base, 500),                       // at the centre
		testStroke(2, testRegion.Lat+0.05, testRegion.Lon, base.Add(time.Minute), 900), // a few km north
	}
	// One well outside the region, which must not come back.
	outside := testStroke(3, testRegion.Lat+0.5, testRegion.Lon, base, 1000)

	for _, stroke := range append(append([]model.Stroke{}, want...), outside) {
		if err := s.Record(ctxBackground(), stroke, cellOf(stroke), certaintyOf(stroke)); err != nil {
			t.Fatalf("Record(%s): %v", stroke.Key(), err)
		}
	}

	got, err := s.StrokesNear(ctxBackground(), testRegion, base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("StrokesNear: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d strokes, want %d", len(got), len(want))
	}
	byID := map[int64]store.StoredStroke{}
	for _, s := range got {
		byID[s.StrokeID] = s
	}
	for _, w := range want {
		if _, ok := byID[w.StrokeID]; !ok {
			t.Errorf("stroke %d is missing from the radius query", w.StrokeID)
		}
	}
	if _, ok := byID[outside.StrokeID]; ok {
		t.Errorf("stroke %d is 55 km away but came back from a 10 km query", outside.StrokeID)
	}

	// The certainty band must survive the round trip, so a consumer can ask the
	// strict question later without the information having been thrown away.
	for _, r := range got {
		if r.Certainty == "" {
			t.Errorf("stroke %d came back with no certainty band", r.StrokeID)
		}
	}
	if got[0].DeviationM == nil || *got[0].DeviationM != 500 {
		t.Errorf("deviation did not survive the round trip: %v", got[0].DeviationM)
	}
}

// The upstream replay means the same stroke arrives again after every reconnect.
// A unique constraint turns a replay into a no-op rather than a duplicate row.
func TestReplayingAStrokeDoesNotDuplicateIt(t *testing.T) {
	s, _ := openStore(t)
	now := time.Now().UTC()
	stroke := testStroke(1949808, 35.4, 24.5, now, 1500)

	for i := range 5 {
		if err := s.Record(ctxBackground(), stroke, cellOf(stroke), certaintyOf(stroke)); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}

	n, err := s.Count(ctxBackground(), time.Time{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 1 {
		t.Errorf("table holds %d rows after five identical records, want 1", n)
	}
}

// The same id from two networks is two different strokes and both must persist.
func TestTheSameIDFromTwoNetworksPersistsBoth(t *testing.T) {
	s, _ := openStore(t)
	now := time.Now().UTC()

	a := testStroke(42, 35.4, 24.5, now, 1000)
	b := testStroke(42, 35.4, 24.5, now, 1000)
	b.Src = model.SourceBlitzortung

	for _, stroke := range []model.Stroke{a, b} {
		if err := s.Record(ctxBackground(), stroke, cellOf(stroke), certaintyOf(stroke)); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	n, _ := s.Count(ctxBackground(), time.Time{})
	if n != 2 {
		t.Errorf("table holds %d rows, want 2", n)
	}
}

// The upstream's resume hint is upstream state, so no broker can hold it. Losing
// it across a restart means replaying five minutes of history every time.
func TestTheResumeCursorSurvivesAReopen(t *testing.T) {
	s, path := openStore(t)

	seen := map[model.Source]int64{
		model.SourceLightningMaps: 1951299,
		model.SourceBlitzortung:   826840,
	}
	if err := s.SaveCursor(ctxBackground(), store.Cursor{Sources: seen, Server: "live"}); err != nil {
		t.Fatalf("SaveCursor: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := store.Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.LoadCursor(ctxBackground())
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}

	if got.Server != "live" {
		t.Errorf("server = %q, want live", got.Server)
	}
	for src, want := range seen {
		if got.Sources[src] != want {
			t.Errorf("cursor for src %v = %d, want %d", int(src), got.Sources[src], want)
		}
	}
}

// A cold start has no cursor, and that must not be an error.
func TestLoadingACursorFromAFreshDatabaseYieldsNothing(t *testing.T) {
	s, _ := openStore(t)
	got, err := s.LoadCursor(ctxBackground())
	if err != nil {
		t.Fatalf("LoadCursor on a fresh database: %v", err)
	}
	if len(got.Sources) != 0 || got.Server != "" {
		t.Errorf("got %+v from a fresh database, want empty", got)
	}
}

// The two upstream servers have independent id spaces, so which server the
// cursor belongs to has to be recorded alongside it.
func TestTheCursorRecordsWhichServerItCameFrom(t *testing.T) {
	s, _ := openStore(t)

	for _, server := range []string{"live", "live2"} {
		if err := s.SaveCursor(ctxBackground(), store.Cursor{
			Sources: map[model.Source]int64{model.SourceLightningMaps: 100},
			Server:  server,
		}); err != nil {
			t.Fatalf("SaveCursor(%s): %v", server, err)
		}
		got, err := s.LoadCursor(ctxBackground())
		if err != nil {
			t.Fatalf("LoadCursor: %v", err)
		}
		if got.Server != server {
			t.Errorf("server = %q, want %q", got.Server, server)
		}
	}
}

// Retention must actually delete. A file-backed store that only grows would
// eventually fill the disk on whatever machine runs the bridge.
func TestRetentionDeletesOldStrokes(t *testing.T) {
	s, _ := openStore(t)
	now := time.Now().UTC()

	old := testStroke(1, 35.4, 24.5, now.Add(-10*24*time.Hour), 1000)
	recent := testStroke(2, 35.4, 24.5, now, 1000)
	for _, stroke := range []model.Stroke{old, recent} {
		if err := s.Record(ctxBackground(), stroke, cellOf(stroke), certaintyOf(stroke)); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	removed, err := s.Prune(ctxBackground(), now.Add(-7*24*time.Hour))
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if removed != 1 {
		t.Errorf("Prune removed %d rows, want 1", removed)
	}

	n, _ := s.Count(ctxBackground(), time.Time{})
	if n != 1 {
		t.Errorf("table holds %d rows after pruning, want 1", n)
	}
}

// The radius query must respect its time window, or "since yesterday" quietly
// returns everything ever recorded.
func TestTheRadiusQueryRespectsItsTimeWindow(t *testing.T) {
	s, _ := openStore(t)
	base := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

	old := testStroke(1, testRegion.Lat, testRegion.Lon, base.Add(-48*time.Hour), 500)
	recent := testStroke(2, testRegion.Lat, testRegion.Lon, base, 500)
	for _, stroke := range []model.Stroke{old, recent} {
		if err := s.Record(ctxBackground(), stroke, cellOf(stroke), certaintyOf(stroke)); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	got, err := s.StrokesNear(ctxBackground(), testRegion, base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("StrokesNear: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d strokes in the last hour, want 1", len(got))
	}
	if got[0].StrokeID != recent.StrokeID {
		t.Errorf("returned stroke %d, want %d", got[0].StrokeID, recent.StrokeID)
	}
}

// A world-wide deployment has no circle, and must still be able to read history.
func TestHistoryCanBeReadWithoutARegion(t *testing.T) {
	s, _ := openStore(t)
	base := time.Now().UTC()

	for i := range 3 {
		stroke := testStroke(int64(i), 35.4+float64(i)*0.01, 24.5, base, 800)
		if err := s.Record(ctxBackground(), stroke, cellOf(stroke), certaintyOf(stroke)); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	got, err := s.StrokesSince(ctxBackground(), base.Add(-time.Hour), 100)
	if err != nil {
		t.Fatalf("StrokesSince: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("got %d strokes, want 3", len(got))
	}
}

// An empty cell would be archived happily and then match no radius query, so the
// archive would look populated while being spatially unsearchable.
//
// This happened for real: the ingest stage shadowed its cell variable with a
// short-lived declaration inside a conditional, so every row was written with an
// empty cell. Nothing errored, NATS publishing was unaffected, and the radius
// query returned nothing. The store now refuses rather than storing a row it
// could never find again.
func TestRecordingWithoutACellIsRejected(t *testing.T) {
	s, _ := openStore(t)
	stroke := testStroke(1, testRegion.Lat, testRegion.Lon, time.Now().UTC(), 900)

	err := s.Record(ctxBackground(), stroke, "", geo.CertaintyIn)
	if err == nil {
		t.Fatal("recording a stroke with no cell was accepted")
	}
	if !strings.Contains(err.Error(), "cell") {
		t.Errorf("the error should mention the cell, got: %v", err)
	}

	// And nothing should have been written.
	n, _ := s.Count(ctxBackground(), time.Time{})
	if n != 0 {
		t.Errorf("a rejected stroke left %d rows behind", n)
	}
}

// Every archived stroke must be findable by the radius query that covers it.
// This is the property the shadowing bug broke.
func TestEveryArchivedStrokeIsFindableAgain(t *testing.T) {
	s, _ := openStore(t)
	now := time.Now().UTC()

	var want []model.Stroke
	for i := range 20 {
		// Keep every stroke inside the radius: 0.004 degrees is about 445 m, so
		// the furthest is 8.5 km north and still within 10 km.
		stroke := testStroke(int64(i),
			testRegion.Lat+float64(i)*0.004, testRegion.Lon, now, 1200)
		cell := cellOf(stroke)
		if err := s.Record(ctxBackground(), stroke, cell, certaintyOf(stroke)); err != nil {
			t.Fatalf("Record: %v", err)
		}
		want = append(want, stroke)
	}

	got, err := s.StrokesNear(ctxBackground(), testRegion, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("StrokesNear: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("archived %d strokes but the radius query returned %d; "+
			"every archived stroke must be findable", len(want), len(got))
	}
}
