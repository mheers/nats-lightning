package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// streamOutcome encodes the exit status a supervisor sees.
//
// Losing the lease cancels the pipeline, so the stream returns context.Canceled and
// nothing else distinguishes that from a deliberate shutdown. Reporting it as
// success made the process exit 0 after another process had become the single
// upstream connection, so Restart=on-failure brought nothing back.
func TestStreamOutcomeReportsALostLeaseAsFailure(t *testing.T) {
	lost := errors.New("lease is held by \"other/1\", not \"this/1\"")

	tests := []struct {
		name        string
		streamErr   error
		leadership  error
		wantErr     bool
		wantMessage string
	}{
		{
			name:      "clean shutdown",
			streamErr: context.Canceled,
		},
		{
			// The regression: cancellation plus a lost lease is a failure, and the
			// old code returned nil here because it only ever looked at streamErr.
			name:        "lost lease surfaces through the cancellation",
			streamErr:   context.Canceled,
			leadership:  lost,
			wantErr:     true,
			wantMessage: "leadership lost",
		},
		{
			name:        "a real streaming failure wins over the lease error",
			streamErr:   errors.New("upstream handshake failed"),
			leadership:  lost,
			wantErr:     true,
			wantMessage: "streaming",
		},
		{
			name:        "a streaming failure alone is still a failure",
			streamErr:   errors.New("upstream handshake failed"),
			wantErr:     true,
			wantMessage: "streaming",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := streamOutcome(tc.streamErr, tc.leadership)
			if tc.wantErr != (err != nil) {
				t.Fatalf("streamOutcome(%v, %v) = %v, wantErr=%v", tc.streamErr, tc.leadership, err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), tc.wantMessage) {
				t.Errorf("error %q does not mention %q", err, tc.wantMessage)
			}
		})
	}
}

// TestStreamOutcomeIsNotNilOnEveryFailure guards the invariant that matters to a
// restart policy: anything that went wrong must produce a non-nil error.
func TestStreamOutcomeIsNotNilOnEveryFailure(t *testing.T) {
	failures := []error{
		errors.New("anything at all"),
		context.Canceled,
		context.DeadlineExceeded,
	}
	for _, f := range failures {
		if streamOutcome(f, f) == nil {
			t.Errorf("streamOutcome(%v, %v) reported success", f, f)
		}
	}
}

// storeHealth backs the readiness probe.
//
// Nothing else reports the archive's condition: a failed Record is counted and
// logged and then deliberately tolerated so a broken archive costs a query rather
// than a stroke, so a probe wired to a value that is never reassigned answers
// "healthy" about a disk that has been full since startup.
func TestStoreHealthFollowsStoreOutcomes(t *testing.T) {
	h := newStoreHealth()
	if !h.ok() {
		t.Error("a store that has never failed must start healthy")
	}

	h.observe(errors.New("disk I/O error"))
	if h.ok() {
		t.Error("health still reported OK after a store failure")
	}

	h.observe(nil)
	if !h.ok() {
		t.Error("health did not recover after a successful store operation")
	}
}

// TestStoreHealthIsRaceFree matters because the probe is served on an HTTP
// goroutine while the pipeline writes from its own.
func TestStoreHealthIsRaceFree(t *testing.T) {
	h := newStoreHealth()
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			if i%2 == 0 {
				h.observe(errors.New("transient"))
			} else {
				h.observe(nil)
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		_ = h.ok()
	}
	<-done
}
