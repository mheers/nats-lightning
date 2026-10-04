package obs

import "github.com/prometheus/client_golang/prometheus"

// prometheusGauge is the type NewGaugeFunc returns, named locally so callers in
// this module do not each import prometheus for one assignment.
// GaugeFunc builds a gauge from a function, so callers do not each import
// prometheus for a single assignment.
func GaugeFunc(f func() float64) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "lightningfeed_store_rows",
		Help: "Strokes currently archived.",
	}, f)
}
