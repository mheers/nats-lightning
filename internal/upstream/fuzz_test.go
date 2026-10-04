package upstream

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
)

// FuzzParseFrame throws arbitrary bytes at the decoder.
//
// This is the only place in the process where input arrives from the internet and
// is trusted to a degree, so the properties asserted here are the ones a panic or a
// silently wrong value would violate:
//
//   - it never panics, whatever the bytes are;
//   - a stroke it accepts has coordinates that are actually coordinates: finite,
//     and inside the real ranges. A stroke at 0,0 that was really a null or an
//     absent field would sail through every other check, which is precisely the bug
//     that shipped once already;
//   - its accounting is self-consistent, so the caller can trust Rejected and
//     RejectReason when it reports a loss.
func FuzzParseFrame(f *testing.F) {
	// Seeds: the real shapes, plus every spelling of a bad coordinate that has
	// mattered, plus the frame kinds the upstream sends.
	seeds := []string{
		`{"time":1700000000,"strokes":[{"time":1700000000000,"lat":48.1,"lon":11.1,"src":2,"id":1}]}`,
		`{"time":1700000000,"strokes":[{"time":1700000000000,"lat":"48.1","lon":"11.1","src":2,"id":1}]}`,
		`{"time":1700000000,"strokes":[{"time":1700000000000,"lat":null,"lon":11.1,"src":2,"id":1}]}`,
		`{"time":1700000000,"strokes":[{"time":1700000000000,"lat":"","lon":11.1,"src":2,"id":1}]}`,
		`{"time":1700000000,"strokes":[{"time":1700000000000,"lon":11.1,"src":2,"id":1}]}`,
		`{"time":1700000000,"strokes":[{"time":1700000000000,"lat":"NaN","lon":"Inf","src":2,"id":1}]}`,
		`{"time":1700000000,"strokes":[]}`,
		`{"time":1700000000}`,
		`{"cid":1,"con":2,"k":3,"port":"80","time":1700000000}`,
		`{"time":1700000000,"flags":{"s":2}}`,
		`{}`,
		`[]`,
		`null`,
		``,
		`{"strokes":[{"time":1,"lat":1e400,"lon":1e400,"src":2,"id":1}]}`,
		`{"strokes":[{"time":-1,"lat":0,"lon":0,"src":2,"id":1}]}`,
		`{"strokes":[{"time":1,"lat":0,"lon":0,"src":99,"id":-7,"dev":-1,"del":-1,"srv":-1,"alt":-1}]}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		frame, err := ParseFrame(raw)
		if err != nil {
			// A frame that will not decode is a legitimate outcome; the caller is
			// documented to count it and keep reading.
			return
		}

		if frame.Rejected < 0 {
			t.Fatalf("Rejected = %d, want non-negative", frame.Rejected)
		}
		if frame.Rejected > 0 && frame.RejectReason == "" {
			t.Errorf("Rejected = %d but RejectReason is empty, so the loss is unattributable: %s",
				frame.Rejected, raw)
		}
		if frame.Rejected == 0 && frame.RejectReason != "" {
			t.Errorf("RejectReason = %q but Rejected = 0; the two must agree", frame.RejectReason)
		}

		for _, s := range frame.Strokes {
			if math.IsNaN(s.Lat) || math.IsNaN(s.Lon) {
				t.Errorf("accepted a NaN coordinate: lat=%v lon=%v from %s", s.Lat, s.Lon, raw)
			}
			if math.IsInf(s.Lat, 0) || math.IsInf(s.Lon, 0) {
				t.Errorf("accepted an infinite coordinate: lat=%v lon=%v from %s", s.Lat, s.Lon, raw)
			}
			if s.Lat < -90 || s.Lat > 90 {
				t.Errorf("accepted latitude %v outside [-90,90] from %s", s.Lat, raw)
			}
			if s.Lon < -180 || s.Lon > 180 {
				t.Errorf("accepted longitude %v outside [-180,180] from %s", s.Lon, raw)
			}
			if s.Time.IsZero() {
				t.Errorf("accepted a stroke with no time from %s", raw)
			}
		}

		// Determinism: the same bytes must always decode the same way, or the
		// golden-fixture guarantee that protocol drift fails a test is a fiction.
		again, err2 := ParseFrame(raw)
		if (err == nil) != (err2 == nil) {
			t.Fatalf("ParseFrame is not deterministic: first err=%v second err=%v", err, err2)
		}
		if err == nil {
			if len(again.Strokes) != len(frame.Strokes) || again.Rejected != frame.Rejected {
				t.Errorf("ParseFrame is not deterministic for %s: %d/%d then %d/%d",
					raw, len(frame.Strokes), frame.Rejected, len(again.Strokes), again.Rejected)
			}
		}
	})
}

// FuzzStrokeCoordinate decides accept-or-reject from the coordinate's spelling.
//
// Unlike FuzzParseFrame this states the expected verdict, so it can catch the
// decoder being too lenient *and* too strict. Too lenient publishes lightning that
// is not there; too strict discards a real stroke and loses data during an upstream
// format change — which is the situation the flexible decoding exists for.
//
// The expectation is derived from the literal itself rather than carried alongside
// it in a parallel "kind" value. A fuzzer mutates those independently, so a carried
// kind produces mismatches that are artefacts of the test rather than defects in the
// decoder — which is the fastest way to make a fuzz target worthless.
func FuzzStrokeCoordinate(f *testing.F) {
	seeds := []string{
		`48.1`, `11.1`, // numbers
		`"48.1"`, `"11.1"`, // numeric strings
		`" 48.1 "`, `" 11.1 "`, // padded strings
		`null`, `""`, `"   "`, // missing values
		`"abc"`, `"NaN"`, `"Inf"`, // non-numeric strings
		`999`, `-999`, `1e308`, // out of range
		`0`, `-0`, `0.0`, // genuine zero
		`-90`, `90`, `-180`, `180`, // boundaries
		`89.999`, `179.999`, // large but in range
		`1e-300`, `true`, `false`, `[]`, `{}`,
	}
	for _, s := range seeds {
		f.Add(s, s)
	}

	f.Fuzz(func(t *testing.T, latLit, lonLit string) {
		body := `{"time":1700000000,"strokes":[{"time":1700000000000,` +
			`"lat":` + latLit + `,"lon":` + lonLit + `,"src":2,"id":1}]}`

		frame, err := ParseFrame([]byte(body))
		if err != nil {
			// The literal was not valid JSON at all, so the frame legitimately
			// failed to decode and there is no verdict to check.
			return
		}

		accepted := len(frame.Strokes) == 1
		want := coordinateShouldDecode(latLit, 90) && coordinateShouldDecode(lonLit, 180)

		if accepted != want {
			if accepted {
				s := frame.Strokes[0]
				t.Errorf("accepted lat=%q lon=%q as lat=%v lon=%v; wanted rejection",
					latLit, lonLit, s.Lat, s.Lon)
			} else {
				t.Errorf("rejected a usable coordinate pair lat=%q lon=%q: %s",
					latLit, lonLit, frame.RejectReason)
			}
		}
	})
}

// coordinateShouldDecode reports whether a coordinate literal is one this decoder
// ought to turn into a usable number.
//
// It restates the documented rules — a number, or a string holding one, finite and
// inside the range — independently of the implementation, so that agreement means
// something. An empty or blank string is a missing value rather than a zero, which
// is the exact distinction the null-island bug turned on.
func coordinateShouldDecode(lit string, limit float64) bool {
	trimmed := strings.TrimSpace(lit)
	if trimmed == "null" || trimmed == "" {
		return false
	}

	if strings.HasPrefix(trimmed, `"`) {
		var s string
		if err := json.Unmarshal([]byte(trimmed), &s); err != nil {
			return false
		}
		if strings.TrimSpace(s) == "" {
			return false
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
		return v >= -limit && v <= limit
	}

	var v float64
	if err := json.Unmarshal([]byte(trimmed), &v); err != nil {
		return false
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return false
	}
	return v >= -limit && v <= limit
}
