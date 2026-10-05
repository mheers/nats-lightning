package ingest

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mheers/nats-lightning/internal/config"
	"github.com/mheers/nats-lightning/internal/geo"
	"github.com/mheers/nats-lightning/internal/model"
	"github.com/mheers/nats-lightning/internal/store"
)

// The history command had two ways to report nothing about a populated archive.
//
// PrintHistory truncated its result with strokes[len(strokes)-limit:] whenever
// len(strokes) > limit, but the CLI passes 0 when --limit is omitted and the README
// documents an invocation without it. strokes[len:] is empty, so every documented
// run printed "no strokes recorded" against a full archive. The same expression with
// a negative limit sliced past the end and panicked, which is the shape a typo at a
// shell prompt takes.

// seedArchive writes n strokes and returns a config pointing at the database.
func seedArchive(t *testing.T, n int) *config.Config {
	t.Helper()
	cfg := &config.Config{SQLitePath: t.TempDir() + "/history.db"}

	db, err := store.Open(cfg.SQLitePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 1; i <= n; i++ {
		s := model.Stroke{
			StrokeID: int64(i),
			Src:      model.SourceLightningMaps,
			Time:     time.Now().Add(-time.Duration(n-i) * time.Second),
			Lat:      regionLat,
			Lon:      regionLon,
		}
		if err := db.Record(context.Background(), s, "u0xc4", geo.CertaintyIn); err != nil {
			t.Fatal(err)
		}
	}
	return cfg
}

// captureStdout runs fn with os.Stdout redirected, because PrintHistory writes to it
// directly rather than through an injected writer.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w

	read := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		read <- string(b)
	}()

	fnErr := fn()
	w.Close()
	os.Stdout = orig
	out := <-read
	r.Close()
	return out, fnErr
}

func TestHistoryWithoutALimitReturnsTheArchive(t *testing.T) {
	cfg := seedArchive(t, 25)

	out, err := captureStdout(t, func() error {
		return PrintHistory(context.Background(), cfg, "24h", 0)
	})
	if err != nil {
		t.Fatalf("PrintHistory: %v", err)
	}
	if strings.Contains(out, "no strokes recorded") {
		t.Errorf("25 strokes archived, but the documented no---limit invocation reported none:\n%s", out)
	}
	if got := strings.Count(out, "src=2"); got != 25 {
		t.Errorf("printed %d strokes, want 25", got)
	}
}

func TestHistoryLimitKeepsTheMostRecent(t *testing.T) {
	cfg := seedArchive(t, 25)

	out, err := captureStdout(t, func() error {
		return PrintHistory(context.Background(), cfg, "24h", 3)
	})
	if err != nil {
		t.Fatalf("PrintHistory: %v", err)
	}
	if got := strings.Count(out, "src=2"); got != 3 {
		t.Errorf("--limit=3 printed %d strokes, want 3", got)
	}
	// Strokes are seeded oldest-id-first, so the three newest are the last three
	// ids. Truncating from the front rather than the back would show id=1,2,3.
	if !strings.Contains(out, "id=25") || !strings.Contains(out, "id=23") {
		t.Errorf("--limit=3 did not keep the three newest:\n%s", out)
	}
	if strings.Contains(out, "id=1 ") {
		t.Errorf("--limit=3 kept the oldest strokes:\n%s", out)
	}
}

// TestHistoryNeverPanicsOnAnyLimit guards the arithmetic rather than one value,
// because the panic was a slice bound and any out-of-range input reaches it.
func TestHistoryNeverPanicsOnAnyLimit(t *testing.T) {
	cfg := seedArchive(t, 5)

	for _, limit := range []int{-1000, -1, 0, 1, 5, 6, 1 << 20} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("PrintHistory panicked with limit=%d: %v", limit, r)
				}
			}()
			_, _ = captureStdout(t, func() error {
				return PrintHistory(context.Background(), cfg, "24h", limit)
			})
		})
	}
}
