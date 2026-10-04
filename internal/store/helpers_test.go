package store_test

import (
	"context"

	"github.com/heers-it/lightningfeed/internal/geo"
	"github.com/heers-it/lightningfeed/internal/model"
)

// ctxBackground is a short-lived context for tests that do not exercise
// cancellation.
func ctxBackground() context.Context { return context.Background() }

// cellOf is the subject cell for a stroke, matching what the publisher uses.
func cellOf(s model.Stroke) string {
	return geo.MustEncode(s.Lat, s.Lon, 5)
}

// certaintyOf is the band a stroke falls into for the configured region. Tests
// pass this explicitly rather than having the store infer it, because the store
// persists a judgement made at ingest time.
func certaintyOf(s model.Stroke) geo.Certainty {
	_, c := testRegion.ClassifyPoint(s.Lat, s.Lon, deviationOf(s))
	return c
}

func deviationOf(s model.Stroke) float64 {
	if s.DeviationM == nil {
		return 0
	}
	return float64(*s.DeviationM)
}
