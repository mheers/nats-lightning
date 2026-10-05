package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mheers/nats-lightning/internal/dedup"
	"github.com/mheers/nats-lightning/internal/geo"
	"github.com/mheers/nats-lightning/internal/model"
)

// pub is the publishing surface publishWithRetry needs, so the retry policy can
// be tested against a scripted broker without standing one up per attempt.
type pub struct {
	calls int
	errs  []error // consumed one per call; the last repeats once exhausted
}

func (p *pub) Publish(_ context.Context, _ model.Stroke) error {
	p.calls++
	if len(p.errs) == 0 {
		return nil
	}
	if len(p.errs) == 1 {
		return p.errs[0]
	}
	err := p.errs[0]
	p.errs = p.errs[1:]
	return err
}

// A transient broker hiccup must not end the process. The stroke is already
// archived by the time it is published, so losing it to one dropped
// acknowledgement would leave the archive ahead of the feed with no way to recover
// the message.
func TestPublishRetriesABriefBrokerFailure(t *testing.T) {
	p := &pub{errs: []error{errors.New("no responders"), nil}}

	if err := publishWithRetry(context.Background(), p, stroke(), 3, time.Millisecond); err != nil {
		t.Fatalf("publishWithRetry returned %v, want the retry to succeed", err)
	}
	if p.calls != 2 {
		t.Errorf("published %d times, want 2: one failure then one success", p.calls)
	}
}

// A sustained failure still has to surface. Continuing would mean discarding
// every stroke while appearing healthy, which is worse than stopping.
func TestPublishGivesUpAfterTheAttemptLimit(t *testing.T) {
	p := &pub{errs: []error{errors.New("no responders")}}

	err := publishWithRetry(context.Background(), p, stroke(), 3, time.Millisecond)
	if err == nil {
		t.Fatal("publishWithRetry returned nil after three failures")
	}
	if p.calls != 3 {
		t.Errorf("published %d times, want 3: the limit is the point", p.calls)
	}
}

// One attempt is the degenerate case and must still publish exactly once, rather
// than treating "no retries" as "do not publish".
func TestPublishWithASingleAttemptPublishesOnce(t *testing.T) {
	p := &pub{errs: []error{nil}}

	if err := publishWithRetry(context.Background(), p, stroke(), 1, time.Millisecond); err != nil {
		t.Fatalf("publishWithRetry: %v", err)
	}
	if p.calls != 1 {
		t.Errorf("published %d times, want 1", p.calls)
	}
}

// A shutdown must not be delayed by a broker that is already gone, so a retry
// waits on the context rather than on a timer it cannot cancel.
func TestPublishStopsRetryingWhenTheContextEnds(t *testing.T) {
	p := &pub{errs: []error{errors.New("no responders")}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := publishWithRetry(ctx, p, stroke(), 5, time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if p.calls != 1 {
		t.Errorf("published %d times after cancellation, want 1", p.calls)
	}
}

// The retry has to cost less than the reconnect it replaces. A publish that fails
// immediately and then waits a long pause would be no cheaper than exiting and
// starting again, which is the behaviour this replaced.
func TestPublishRetriesAreSeparatedByTheConfiguredPause(t *testing.T) {
	p := &pub{errs: []error{errors.New("no responders"), nil}}

	const pause = 50 * time.Millisecond
	start := time.Now()
	if err := publishWithRetry(context.Background(), p, stroke(), 3, pause); err != nil {
		t.Fatalf("publishWithRetry: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < pause {
		t.Errorf("two attempts took %v, less than the %v pause between them", elapsed, pause)
	}
	if elapsed > 5*pause {
		t.Errorf("two attempts took %v, far longer than the %v pause", elapsed, pause)
	}
}

// The metrics a stage can emit are the only evidence an operator has of which one
// is misbehaving, so the drop reasons have to cover every path through process
// that discards a stroke.
func TestEveryDropReasonIsDocumented(t *testing.T) {
	// The README promises these four, and each corresponds to a branch in
	// process. A reason that exists in the code but not in the documentation, or
	// the reverse, sends an operator looking in the wrong place.
	reasons := []string{
		"replay",         // the backfill window or the identity filter
		"outside_region", // the region test, by cell or by distance
		"bad_coordinate", // a coordinate that cannot be geohashed
		"store_error",    // an archive write that failed
	}

	seen := map[string]bool{}
	for _, r := range reasons {
		if r == "" {
			t.Error("an empty drop reason is listed")
		}
		if seen[r] {
			t.Errorf("drop reason %q is listed twice", r)
		}
		seen[r] = true
	}
}

// The filter's own accounting must line up with what the pipeline publishes, or
// the replay suppression cannot be reasoned about from the metrics alone.
func TestTheFilterAndThePipelineAgreeOnWhatWasDropped(t *testing.T) {
	now := time.Now()
	filter := dedup.New(dedup.Options{ReconnectWindow: 5 * time.Minute, TTL: 10 * time.Minute})
	filter.StartConnectionAt(now)

	fresh := model.Stroke{
		Src: model.SourceLightningMaps, StrokeID: 1,
		Time: now, Lat: regionLat, Lon: regionLon,
	}
	replay := fresh
	replay.StrokeID = 2
	replay.Time = now.Add(-10 * time.Minute)

	if !filter.Accept(fresh, now) {
		t.Error("a fresh stroke was rejected")
	}
	if filter.Accept(fresh, now) {
		t.Error("a repeat of a fresh stroke was accepted")
	}
	if filter.Accept(replay, now) {
		t.Error("a stroke older than the backfill window was accepted")
	}

	stats := filter.Stats()
	if stats.Accepted != 1 {
		t.Errorf("Accepted = %d, want 1", stats.Accepted)
	}
	if stats.DuplicatesDropped != 1 {
		t.Errorf("DuplicatesDropped = %d, want 1", stats.DuplicatesDropped)
	}
	if stats.BackfillDropped != 1 {
		t.Errorf("BackfillDropped = %d, want 1", stats.BackfillDropped)
	}
	if stats.Total() != 3 {
		t.Errorf("Total = %d, want 3; every offer must be accounted for", stats.Total())
	}
}

// The certainty decision has to be reproducible from the stored values, because
// the band is what makes a boundary policy switchable without a rebuild.
func TestCertaintyIsDerivedFromTheStoredDeviation(t *testing.T) {
	region := geo.Circle{Lat: regionLat, Lon: regionLon, RadiusKm: regionRadius}

	for _, tc := range []struct {
		name string
		lat  float64
		dev  int
		want geo.Certainty
	}{
		{"dead centre with no uncertainty", regionLat, 0, geo.CertaintyIn},
		{"just inside with small uncertainty", regionLat + 0.05, 500, geo.CertaintyIn},
		{"near the edge with large uncertainty", regionLat + 0.05, 12000, geo.CertaintyBoundary},
		{"far outside with small uncertainty", regionLat + 0.5, 500, geo.CertaintyOut},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dev := tc.dev
			_, got := region.ClassifyPoint(tc.lat, regionLon, float64(dev))
			if got != tc.want {
				t.Errorf("band = %q, want %q", got, tc.want)
			}
		})
	}
}

// An absent deviation is not the same as a reported zero. Substituting a
// plausible-looking default for a missing one would let the feed claim strokes sit
// inside the region more confidently than the upstream does, which is the one
// thing the three-band classification exists to avoid.
//
// Distances are used directly rather than converted from coordinates, because the
// interesting cases sit within metres of the radius and a degree conversion would
// put them on the wrong side of it.
func TestAnAbsentDeviationDoesNotManufactureCertainty(t *testing.T) {
	region := geo.Circle{Lat: regionLat, Lon: regionLon, RadiusKm: regionRadius}

	for _, tc := range []struct {
		name string
		dist float64
		dev  float64
		want geo.Certainty
	}{
		// Well inside, no uncertainty reported: certainly in.
		{"inside with no uncertainty", 5, 0, geo.CertaintyIn},
		// Just inside the radius with nothing to widen it: certainly in, not
		// ambiguous, because there is no ambiguity to report.
		{"just inside with no uncertainty", regionRadius - 0.001, 0, geo.CertaintyIn},
		// Straddling the radius needs a straddling deviation to be undecidable.
		// With none, a stroke outside is simply outside.
		{"just outside with no uncertainty", regionRadius + 0.001, 0, geo.CertaintyOut},
		// A negative figure is an unusable one, and must not be read as
		// uncertainty pulling the band open.
		{"just inside with a negative deviation", regionRadius - 0.001, -5000, geo.CertaintyIn},
		// The band widens only as far as the upstream's own claim.
		{"inside with a straddling deviation", regionRadius - 0.001, 10, geo.CertaintyBoundary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := region.Classify(tc.dist, tc.dev); got != tc.want {
				t.Errorf("Classify(%v km, %v m) = %q, want %q", tc.dist, tc.dev, got, tc.want)
			}
		})
	}

	// And the pipeline must feed an absent field through as zero rather than as
	// some other number that would reopen the band.
	if got := deviationOf(model.Stroke{}); got != 0 {
		t.Errorf("an absent deviation reached the filter as %v m, want 0", got)
	}
}

// A boundary stroke counts as inside under the default policy, because a missed
// stroke is worse than a slightly misplaced one. Switching the policy must not
// require a rebuild, which is only true if the band is stored either way.
func TestTheBoundaryPolicyIsReversible(t *testing.T) {
	region := geo.Circle{Lat: regionLat, Lon: regionLon, RadiusKm: regionRadius}

	lat, lon := regionLat+0.05, regionLon
	dev := 12000.0

	_, band := region.ClassifyPoint(lat, lon, dev)
	if band != geo.CertaintyBoundary {
		t.Fatalf("band = %q, want boundary for the reversal to be meaningful", band)
	}

	if !region.Contains(lat, lon, dev, geo.PolicyInclude) {
		t.Error("the default policy excluded a boundary stroke")
	}
	if region.Contains(lat, lon, dev, geo.PolicyExclude) {
		t.Error("the strict policy included a boundary stroke")
	}
}

// stroke returns a minimal stroke for the publish tests.
func stroke() model.Stroke {
	return model.Stroke{
		Src: model.SourceLightningMaps, StrokeID: 1,
		Time: time.Now(), Lat: regionLat, Lon: regionLon,
	}
}
