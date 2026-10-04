package upstream

import (
	"strings"
	"testing"
)

// flexFloat exists so the two transports' disagreement about whether a coordinate
// is a number or a string cannot place a stroke at the null island. Three other
// routes to 0.0 got through it: an explicit null, an empty string, and a field that
// was not there at all.
//
// The absent case is the one to worry about, because it needs no upstream bug — only
// a renamed field, which this undocumented feed is free to do at any time. Every one
// of these produced an *accepted* stroke at latitude or longitude 0, published and
// archived as real lightning in the Gulf of Guinea, with bad_coordinate still at
// zero and nothing in the logs.
func TestCoordinatesThatAreNotNumbers(t *testing.T) {
	tests := []struct {
		name string
		lat  string
		lon  string
	}{
		{"null latitude", `"lat":null,`, `"lon":11.1,`},
		{"empty string latitude", `"lat":"",`, `"lon":11.1,`},
		{"blank string latitude", `"lat":"   ",`, `"lon":11.1,`},
		{"absent latitude", ``, `"lon":11.1,`},
		{"null longitude", `"lat":48.1,`, `"lon":null,`},
		{"empty string longitude", `"lat":48.1,`, `"lon":"",`},
		{"absent longitude", `"lat":48.1,`, ``},
		{"both absent", ``, ``},
		{"not a number at all", `"lat":"north",`, `"lon":11.1,`},
		{"NaN as a string", `"lat":"NaN",`, `"lon":11.1,`},
		{"out of range", `"lat":999,`, `"lon":11.1,`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"time":1700000000,"strokes":[{"time":1700000000000,` +
				tc.lat + tc.lon + `"src":2,"id":1}]}`

			f, err := ParseFrame([]byte(body))
			if err != nil {
				return // rejecting the frame outright is also acceptable
			}
			if len(f.Strokes) != 0 {
				s := f.Strokes[0]
				t.Fatalf("accepted a stroke with no usable coordinate: lat=%v lon=%v", s.Lat, s.Lon)
			}
			if f.Rejected != 1 {
				t.Errorf("Rejected = %d, want 1; an unusable stroke must be counted", f.Rejected)
			}
			if f.RejectReason == "" {
				t.Error("RejectReason is empty, so the drop would be unattributable")
			}
		})
	}
}

// TestValidCoordinatesStillDecode is the control. Tightening the decoder is only
// safe if the forms the upstream actually sends still work, so both transports'
// spellings are asserted alongside the rejections above.
func TestValidCoordinatesStillDecode(t *testing.T) {
	tests := []struct {
		name string
		lat  string
		lon  string
	}{
		{"numbers", `"lat":48.1,`, `"lon":11.1,`},
		{"strings, as the HTTP transport sends them", `"lat":"48.1",`, `"lon":"11.1",`},
		{"strings with surrounding whitespace", `"lat":" 48.1 ",`, `"lon":" 11.1 ",`},
		{"negative values", `"lat":-33.9,`, `"lon":151.2,`},
		// Zero is a legal coordinate and must not be confused with an absent one.
		{"genuine zero latitude", `"lat":0,`, `"lon":11.1,`},
		{"genuine zero longitude", `"lat":48.1,`, `"lon":0,`},
		{"both genuinely zero", `"lat":0,`, `"lon":0,`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"time":1700000000,"strokes":[{"time":1700000000000,` +
				tc.lat + tc.lon + `"src":2,"id":7}]}`

			f, err := ParseFrame([]byte(body))
			if err != nil {
				t.Fatalf("ParseFrame: %v", err)
			}
			if f.Rejected != 0 {
				t.Fatalf("Rejected = %d (%s), want 0", f.Rejected, f.RejectReason)
			}
			if len(f.Strokes) != 1 {
				t.Fatalf("got %d strokes, want 1", len(f.Strokes))
			}
			if got := f.Strokes[0].StrokeID; got != 7 {
				t.Errorf("StrokeID = %d, want 7", got)
			}
		})
	}
}

// TestOneUnusableCoordinateStillDoesNotDiscardTheBatch re-asserts the property the
// flexFloat rework exists for, now that the rejection rules are stricter. Widening
// what counts as unusable must not widen what gets thrown away.
func TestOneUnusableCoordinateStillDoesNotDiscardTheBatch(t *testing.T) {
	const batch = `{"time":1700000000,"strokes":[` +
		`{"time":1700000000000,"lat":48.1,"lon":11.1,"src":2,"id":1},` +
		`{"time":1700000001000,"lat":null,"lon":11.2,"src":2,"id":2},` +
		`{"time":1700000002000,"lat":48.3,"lon":11.3,"src":2,"id":3},` +
		`{"time":1700000003000,"lon":11.4,"src":2,"id":4},` +
		`{"time":1700000004000,"lat":48.5,"lon":11.5,"src":2,"id":5}]}`

	f, err := ParseFrame([]byte(batch))
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if f.Rejected != 2 {
		t.Errorf("Rejected = %d, want 2", f.Rejected)
	}
	if len(f.Strokes) != 3 {
		t.Fatalf("kept %d strokes, want 3: one bad coordinate must cost only itself", len(f.Strokes))
	}
	for _, id := range []int64{1, 3, 5} {
		found := false
		for _, s := range f.Strokes {
			if s.StrokeID == id {
				found = true
			}
		}
		if !found {
			t.Errorf("stroke %d was discarded along with the bad ones", id)
		}
	}
	if !strings.Contains(f.RejectReason, "stroke 1") {
		t.Errorf("RejectReason = %q, want it to name the first offending stroke", f.RejectReason)
	}
}
