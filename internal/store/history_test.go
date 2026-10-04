package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/heers-it/lightningfeed/internal/store"
)

// StrokesSince is the world-wide read behind `lightningfeed history` when no
// region is configured, and its LIMIT semantics were quietly inverted.
//
// The query scanned ascending and applied LIMIT to that scan, which keeps the
// *oldest* rows in the window. Asking for the ten most recent strokes since
// yesterday therefore returned the first ten of that window — timestamps hours
// stale — while still looking like a working query, because it returned rows.

// seedStrokes writes n strokes one minute apart, so stroke id n is the newest.
func seedStrokes(t *testing.T, s *store.Store, n int) time.Time {
	t.Helper()
	base := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= n; i++ {
		st := testStroke(int64(i), testRegion.Lat, testRegion.Lon, base.Add(time.Duration(i)*time.Minute), 0)
		if err := s.Record(context.Background(), st, "u0xc4", "in"); err != nil {
			t.Fatalf("Record(%d): %v", i, err)
		}
	}
	return base
}

func strokeIDs(ss []store.StoredStroke) []int64 {
	out := make([]int64, len(ss))
	for i, s := range ss {
		out[i] = s.StrokeID
	}
	return out
}

func TestStrokesSinceReturnsTheMostRecent(t *testing.T) {
	s, _ := openStore(t)
	base := seedStrokes(t, s, 10)

	got, err := s.StrokesSince(context.Background(), base.Add(-time.Minute), 3)
	if err != nil {
		t.Fatalf("StrokesSince: %v", err)
	}
	if want := []int64{8, 9, 10}; !equalIDs(got, want) {
		t.Errorf("StrokesSince(limit=3) = %v, want %v (the three newest)", strokeIDs(got), want)
	}
}

// TestStrokesSinceIsNewestLast pins the documented ordering, which the DESC scan
// used to invert.
func TestStrokesSinceIsNewestLast(t *testing.T) {
	s, _ := openStore(t)
	base := seedStrokes(t, s, 5)

	got, err := s.StrokesSince(context.Background(), base.Add(-time.Minute), 5)
	if err != nil {
		t.Fatalf("StrokesSince: %v", err)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].Time.After(got[i].Time) {
			t.Fatalf("result is not ascending in time: %v then %v", got[i-1].Time, got[i].Time)
		}
	}
	if got[len(got)-1].StrokeID != 5 {
		t.Errorf("last stroke id = %d, want 5 (the newest)", got[len(got)-1].StrokeID)
	}
}

// TestStrokesSinceHonoursTheWindow checks the limit did not come at the cost of the
// since bound, which is the other half of the same query.
func TestStrokesSinceHonoursTheWindow(t *testing.T) {
	s, _ := openStore(t)
	base := seedStrokes(t, s, 10)

	// Stroke i is stamped base+i minutes, so since=base+7m includes stroke 7:
	// the bound is inclusive.
	got, err := s.StrokesSince(context.Background(), base.Add(7*time.Minute), 100)
	if err != nil {
		t.Fatalf("StrokesSince: %v", err)
	}
	if want := []int64{7, 8, 9, 10}; !equalIDs(got, want) {
		t.Errorf("StrokesSince(since=base+7m) = %v, want %v", strokeIDs(got), want)
	}
}

func TestStrokesSinceLimitZeroMeansNoLimit(t *testing.T) {
	s, _ := openStore(t)
	base := seedStrokes(t, s, 10)

	// The CLI passes 0 when --limit is omitted, so this is the default path and it
	// must return the window rather than nothing.
	got, err := s.StrokesSince(context.Background(), base.Add(-time.Minute), 0)
	if err != nil {
		t.Fatalf("StrokesSince: %v", err)
	}
	if len(got) != 10 {
		t.Errorf("limit=0 returned %d strokes, want all 10", len(got))
	}
}

// TestStrokesNearIsAlsoNewestLast keeps the two reads consistent, because the
// history command truncates the radius query in process and would otherwise keep
// the wrong end of it.
//
// It also pins *which* rows the read budget is spent on. The limit here bounds rows
// read rather than rows returned, and a LIMIT over an ascending scan keeps the
// oldest — so a busy region answered a question about the last 24 hours with the
// first 10000 rows of it, and every timestamp it printed was a day stale.
func TestStrokesNearIsAlsoNewestLast(t *testing.T) {
	s, _ := openStore(t)
	base := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= 5; i++ {
		st := testStroke(int64(i), testRegion.Lat, testRegion.Lon, base.Add(time.Duration(i)*time.Minute), 0)
		// cellOf, not a literal: StrokesNear selects by the region's own cells, so
		// a stroke filed under any other cell is invisible to it by construction.
		if err := s.Record(context.Background(), st, cellOf(st), "in"); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.StrokesNear(context.Background(), testRegion, base.Add(-time.Minute))
	if err != nil {
		t.Fatalf("StrokesNear: %v", err)
	}
	if len(got) < 2 {
		t.Fatalf("got %d strokes, want at least 2", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].Time.After(got[i].Time) {
			t.Fatalf("StrokesNear result is not ascending in time at %d", i)
		}
	}
	if got[len(got)-1].StrokeID != 5 {
		t.Errorf("last stroke id = %d, want 5 (the newest)", got[len(got)-1].StrokeID)
	}
}

func equalIDs(got []store.StoredStroke, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].StrokeID != want[i] {
			return false
		}
	}
	return true
}
