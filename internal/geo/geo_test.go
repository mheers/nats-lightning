package geo

import "testing"

// Slice 1: Encode produces the geohash the specification says it should.
//
// The expected value is the canonical worked example from the geohash
// specification (Wikipedia, "Geohash"): latitude 57.64911, longitude 10.40739
// encodes to "u4pru". This is an independent source of truth — not something
// this package computed.
func TestEncodeMatchesSpecificationExample(t *testing.T) {
	got, err := Encode(57.64911, 10.40739, 5)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if got != "u4pru" {
		t.Errorf("Encode(57.64911, 10.40739, 5) = %q, want %q", got, "u4pru")
	}
}

func TestEncodeHonoursPrecision(t *testing.T) {
	// Lower precision is a prefix of higher precision: each extra character
	// refines the same cell by a factor of 32.
	for precision := 1; precision <= 5; precision++ {
		got, err := Encode(57.64911, 10.40739, precision)
		if err != nil {
			t.Fatalf("precision %d: %v", precision, err)
		}
		if want := "u4pru"[:precision]; got != want {
			t.Errorf("precision %d: got %q, want %q", precision, got, want)
		}
	}
}

func TestEncodeRejectsImpossibleInput(t *testing.T) {
	// Silently encoding latitude 91 would produce a hash that decodes to a
	// cell somewhere else entirely, so out-of-range input is an error.
	for _, tc := range []struct {
		name      string
		lat, lon  float64
		precision int
	}{
		{"latitude above 90", 90.1, 0, 5},
		{"latitude below -90", -90.1, 0, 5},
		{"longitude above 180", 0, 180.1, 5},
		{"longitude below -180", 0, -180.1, 5},
		{"precision zero", 0, 0, 0},
		{"precision above 12", 0, 0, 13},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Encode(tc.lat, tc.lon, tc.precision); err == nil {
				t.Errorf("Encode(%v, %v, %d) = no error, want error",
					tc.lat, tc.lon, tc.precision)
			}
		})
	}
}

func TestMustEncodePanicsOnBadInput(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustEncode did not panic on invalid input")
		}
	}()
	MustEncode(91, 0, 5)
}
