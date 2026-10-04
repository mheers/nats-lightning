package obs

import "github.com/prometheus/client_golang/prometheus"

// StoreRowsGauge builds the archived-row-count gauge from a counting function.
//
// It returns an unregistered collector: the caller must pass it to
// Metrics.Register. That is not a trap but a deliberate two-step, because the
// function closes over the store, which is opened after the metrics are built.
// Assigning the result to a struct field is not enough — an unregistered
// collector exports nothing, which is exactly how the archive-depth gauge went
// missing from the exposition while appearing correctly wired in the source.
func StoreRowsGauge(count func() (int64, error)) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "lightningfeed_store_rows",
		Help: "Strokes currently archived.",
	}, func() float64 {
		n, err := count()
		if err != nil {
			// Reporting zero would be a lie that reads as "the archive is
			// empty" rather than "the count failed". The sentinel is the same
			// one the last-message gauge uses for not-connected.
			return float64(countFailedSentinel)
		}
		return float64(n)
	})
}

// countFailedSentinel stands in for "the count could not be obtained".
//
// It matches disconnectedSentinel so a dashboard alerts on one recognisable
// "not a real number" value rather than having to learn two.
const countFailedSentinel = 1e9
