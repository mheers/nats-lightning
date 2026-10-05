package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/mheers/nats-lightning/internal/model"
	"github.com/mheers/nats-lightning/internal/store"
)

// The pipeline, run as a program.
//
// Each test drives the compiled binary against a real nats-server with JetStream and
// a real WebSocket upstream, and then checks the three places the result can be
// observed: the broker, the SQLite archive, and the process's own metrics and health
// endpoints. Nothing is stubbed inside the bridge, so a defect anywhere between the
// socket and the archive has to show up here.

// TestTheWholePipelineRuns is the baseline: real upstream, real broker, real archive.
//
// It asserts all four observable outcomes together, because each on its own is
// satisfiable by a bridge that is broken in a different place. The outside stroke is
// the discriminator — it must appear in neither the stream nor the archive, so a
// bridge that published everything, or that published nothing, cannot pass.
func TestTheWholePipelineRuns(t *testing.T) {
	f := newFixture(t, defaultUpstream(3))
	b := f.startIngest(t, true)

	// Three inside the region plus one outside it.
	f.waitForStream(t, 3, 20*time.Second)

	// The archive holds the inside strokes and not the outside one.
	storeRows(t, b, 3)

	// And the exposition agrees. The connected and leader gauges are copied across
	// on a timer, so they are waited for rather than read instantaneously.
	b.waitForSample(t, `lightningfeed_upstream_connected`, 1, 15*time.Second)
	b.waitForSample(t, "lightningfeed_leader", 1, 15*time.Second)
	b.waitForSample(t, `lightningfeed_dropped_total{reason="outside_region"}`, 1, 15*time.Second)

	m := b.metrics(t)
	if got := sampleMetrics(t, m, "lightningfeed_store_rows"); got < 3 {
		t.Errorf("store_rows = %v, want at least 3", got)
	}

	// The region is reported, because a bridge that silently ran world-wide would
	// otherwise satisfy every assertion above.
	if !strings.Contains(m, `lightningfeed_config_info{region="roussospiti"}`) {
		t.Error("the configured region is absent from the exposition")
	}

	// Exactly one upstream connection: the leader-only invariant that makes the
	// deployment possible at all.
	if n := f.upstream.ConnectionCount(); n != 1 {
		t.Errorf("the upstream saw %d connections, want 1", n)
	}
}

// TestTheProcessPublishesCloudEvents checks the wire contract rather than a count.
//
// A count proves something arrived; it does not prove a consumer can read it. The
// subject, the CloudEvent envelope and the attribution string are the interface, and
// this asserts them by reading a message off the stream exactly as a subscriber would.
func TestTheProcessPublishesReadableCloudEvents(t *testing.T) {
	f := newFixture(t, defaultUpstream(1))
	b := f.startIngest(t, true)
	f.waitForStream(t, 1, 20*time.Second)

	nc := f.nats.Connect(t)
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Read through a durable consumer bound to the stream, which is the documented
	// way to consume: deduplication applies to the stream, not to the subject, so a
	// plain Core subscription would see replays a consumer does not.
	sub, err := js.SubscribeSync("", nats.BindStream("E2E"), nats.Durable("e2e-reader"))
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatalf("no message on the stream: %v", err)
	}

	body := string(msg.Data)
	for _, want := range []string{
		`"specversion":"1.0"`,
		`"type":"org.blitzortung.lightning.stroke.v1"`,
		`"datacontenttype":"application/json"`,
		`"src":2`,
		`"network":"lightningmaps.org"`,
		`Blitzortung.org contributors`,
		`"certainty":"in"`,
		`"attribution"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the published event does not contain %s\n%s", want, body)
		}
	}

	if !strings.HasPrefix(msg.Subject, "lightning.v1.src.2.cell.") {
		t.Errorf("subject = %q, want lightning.v1.src.2.cell.<geohash>", msg.Subject)
	}

	// The message id is what lets the broker drop a reconnect replay.
	if len(msg.Header.Get("Nats-Msg-Id")) == 0 {
		t.Error("the published message carries no Nats-Msg-Id, so a replay would duplicate")
	}

	_ = b
}

// TestHistoryCommandReadsTheArchiveTheBridgeWrote is the regression for the worst
// bug this project has had, asserted the way a user meets it.
//
// One process ingests; a second, entirely separate process is then asked what it
// recorded. Nothing is shared but the database file. The invocation is the one the
// README documents, with no --limit, which is exactly the shape that used to report
// an empty archive against a full one.
func TestHistoryCommandReadsTheArchiveTheBridgeWrote(t *testing.T) {
	f := newFixture(t, defaultUpstream(4))
	b := f.startIngest(t, true)
	f.waitForStream(t, 4, 20*time.Second)
	storeRows(t, b, 4)

	// The documented invocation, verbatim except for the database path.
	stdout, stderr, code := run(t, append([]string{
		"history",
		"--sqlite=" + b.sqlitePath,
		"--since=24h",
	}, regionFlag...)...)
	if code != 0 {
		t.Fatalf("history exited %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	if strings.Contains(stdout, "no strokes recorded") {
		t.Errorf("history reported an empty archive after %d strokes were recorded:\n%s", 4, stdout)
	}
	if got := strings.Count(stdout, "src=2"); got != 4 {
		t.Errorf("history printed %d strokes, want 4:\n%s", got, stdout)
	}
	if !strings.Contains(stdout, "certainty=in") {
		t.Errorf("history did not report the certainty band:\n%s", stdout)
	}
	// The far stroke was never archived, so it must not appear.
	if strings.Contains(stdout, fmt.Sprintf("%.4f", outsideLat)) {
		t.Errorf("history returned a stroke outside the region:\n%s", stdout)
	}

	// --limit keeps the most recent, which was the other half of that bug.
	stdout, _, code = run(t, append([]string{
		"history",
		"--sqlite=" + b.sqlitePath,
		"--since=24h",
		"--limit=2",
	}, regionFlag...)...)
	if code != 0 {
		t.Fatalf("history --limit exited %d", code)
	}
	if got := strings.Count(stdout, "src=2"); got != 2 {
		t.Errorf("--limit=2 printed %d strokes, want 2:\n%s", got, stdout)
	}
}

// TestTheCursorSurvivesARestart is the property the cursor exists for, measured
// across two process lifetimes.
//
// The upstream replays its recent history on every connection, so a bridge that
// forgot its cursor republishes it. The JetStream duplicate window would hide that
// from the broker, which is why the check is on what the *archive* accumulated and on
// the resume the second process reports in its own log — not merely on the count.
func TestTheCursorSurvivesARestart(t *testing.T) {
	up := defaultUpstream(3)
	f := newFixture(t, up)

	first := f.startIngest(t, false) // no leadership, so the SQLite file is free to reopen
	f.waitForStream(t, 3, 20*time.Second)
	storeRows(t, first, 3)

	// Give the cursor saver time to write. It saves immediately on start and then on
	// a timer, so a stroke has to have been seen for there to be anything to save.
	waitForCursor(t, f, 2, 10*time.Second)

	// Stop it the way a supervisor would.
	first.signal(t, syscall.SIGTERM)
	if code := first.waitExit(t, 30*time.Second); code != 0 {
		t.Fatalf("a signalled shutdown exited %d, want 0\nstderr:\n%s", code, first.stderr)
	}

	before := storeRowCount(t, first.sqlitePath)

	second := f.startIngest(t, false)
	defer func() {
		second.signal(t, syscall.SIGTERM)
		_ = second.waitExit(t, 30*time.Second)
	}()

	// The second process must say it resumed rather than started cold.
	if !strings.Contains(second.stderr.String(), "resuming from the stored cursor") {
		t.Errorf("the restarted bridge did not resume:\n%s", second.stderr)
	}
	// Give it long enough to have replayed, had it been going to.
	time.Sleep(2 * time.Second)

	if after := storeRowCount(t, second.sqlitePath); after != before {
		t.Errorf("the archive grew from %d to %d rows across a restart with a saved "+
			"cursor, so the resume did not suppress the replay", before, after)
	}
}

// TestOnlyTheLeaderConnectsUpstream is the invariant the whole deployment rests on:
// the upstream throttles connections, so a standby must not dial it.
//
// Two instances are started against one upstream. The standby must report not-ready
// and must never appear as a second connection, and after the leader is told to stop,
// the standby must take over — which is only possible if the lease was released
// cleanly on shutdown.
func TestOnlyTheLeaderConnectsUpstream(t *testing.T) {
	f := newFixture(t, defaultUpstream(1))

	leader := f.startIngest(t, true)
	leader.waitReady(t, 30*time.Second)
	f.waitForStream(t, 1, 20*time.Second)

	// A second instance with the same leader key waits rather than connecting.
	standby := f.startIngestNoWait(t, true)
	defer func() {
		if standby.cmd.ProcessState == nil {
			_ = standby.cmd.Process.Kill()
		}
		_ = standby.cmd.Wait()
	}()

	// Give it long enough to have connected if it were going to.
	time.Sleep(3 * time.Second)
	if n := f.upstream.ConnectionCount(); n != 1 {
		t.Errorf("the upstream saw %d connections with two instances running, want 1: "+
			"a standby must not dial the upstream", n)
	}

	// The standby does not serve /healthz at all.
	//
	// This is a known gap rather than the intended behaviour, and the test records it
	// instead of asserting it as correct. RunWith acquires leadership at step 1 and
	// only binds the metrics server at step 4, so a process waiting for the lease has
	// no endpoint to answer on: an operator gets connection-refused where both the
	// README and RunWith's own step ordering promise a 503 explaining "not the
	// leader". Connection-refused reads as a crash, which is the opposite of the
	// message a correct standby should be sending.
	//
	// The fix is to bind the server before acquiring the lease, which needs the
	// probe's late-bound dependencies (client, store) behind something concurrency-safe
	// because the handler would start serving before they exist. That is a real
	// change to the most delicate function in the codebase and is left as a decision
	// rather than done here.
	if body, code, err := standby.get("/healthz"); err == nil {
		t.Logf("the standby served /healthz with %d: %s", code, body)
	} else {
		t.Logf("KNOWN GAP: the standby cannot report readiness because it blocks "+
			"before binding the metrics server: %v", err)
	}

	// Now the leader goes away, and the standby must take over.
	leader.signal(t, syscall.SIGTERM)
	if code := leader.waitExit(t, 30*time.Second); code != 0 {
		t.Fatalf("the leader exited %d on SIGTERM, want 0", code)
	}

	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		if f.upstream.ConnectionCount() >= 2 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n := f.upstream.ConnectionCount(); n < 2 {
		t.Errorf("after the leader stopped, the upstream saw %d connections; the "+
			"standby never took over, so the lease was not released cleanly:\n%s",
			n, standby.stderr)
	}
}

// TestLosingTheLeaseExitsNonZero is the regression for the exit-status bug, asserted
// where it actually matters: at the process boundary, against a supervisor.
//
// The lease is stolen out from under a running bridge. The bridge's only lever is to
// cancel itself, and a cancelled pipeline is indistinguishable from a deliberate
// shutdown unless the exit status says otherwise. Reporting success here would leave
// the deployment down with every signal saying it had been planned.
func TestLosingTheLeaseExitsNonZero(t *testing.T) {
	f := newFixture(t, defaultUpstream(1))
	b := f.startIngest(t, true)
	f.waitForStream(t, 1, 20*time.Second)

	stealLease(t, f, b.leaderKey)

	code := b.waitExit(t, 40*time.Second)
	if code == 0 {
		t.Errorf("the bridge exited 0 after losing the lease to another process; a "+
			"supervisor's Restart=on-failure would leave it down and looking deliberate\n%s",
			b.stderr)
	}
	if !strings.Contains(b.stderr.String(), "leadership lost") {
		t.Errorf("the bridge did not say it lost leadership:\n%s", b.stderr)
	}
}

// TestSIGTERMReleasesTheLeaseAndExitsCleanly is the counterpart: an operator stopping
// the service must get exit 0 and a released lease, or every deploy ends in a
// handover fight.
func TestSIGTERMReleasesTheLeaseAndExitsCleanly(t *testing.T) {
	f := newFixture(t, defaultUpstream(1))
	b := f.startIngest(t, true)
	f.waitForStream(t, 1, 20*time.Second)

	b.signal(t, syscall.SIGTERM)
	if code := b.waitExit(t, 30*time.Second); code != 0 {
		t.Errorf("SIGTERM produced exit %d, want 0\nstderr:\n%s", code, b.stderr)
	}

	// The lease is gone, so a replacement can take it immediately rather than
	// waiting out the TTL.
	if owner := leaseOwner(t, f, b.leaderKey); owner != "" {
		t.Errorf("the lease is still held by %q after a clean shutdown", owner)
	}
}

// TestTheBridgeRefusesToStartWhenItCannotWork covers the failures an operator will
// actually hit. Each must be a clear message and a non-zero exit, not a process that
// lingers reporting health it does not deserve.
func TestTheBridgeRefusesToStartWhenItCannotWork(t *testing.T) {
	deadPort, err := freePort()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		args    []string
		wantMsg string
	}{
		{
			name: "an incomplete region",
			args: []string{
				"ingest",
				"--region-lat=48.0",
				"--region-lon=11.0",
			},
			wantMsg: "region",
		},
		{
			name: "an unreachable broker",
			args: []string{
				"ingest",
				"--nats-url=nats://127.0.0.1:" + deadPort,
				"--region-name=x",
				"--region-lat=35.3340688",
				"--region-lon=24.4944483",
				"--region-radius-km=10",
			},
			wantMsg: "NATS",
		},
		{
			name: "a region too large to enumerate",
			args: []string{
				"ingest",
				"--nats-url=nats://127.0.0.1:" + deadPort,
				"--region-name=big",
				"--region-lat=60.0",
				"--region-lon=0.0",
				"--region-radius-km=2500",
			},
			wantMsg: "radius",
		},
		{
			name:    "an unknown flag",
			args:    []string{"ingest", "--jetstream=false"},
			wantMsg: "not defined",
		},
		{
			name: "a negative history limit",
			args: []string{"history", "--since=1h", "--limit=-1"},
			// The limit is caught while stripping, before anything is opened.
			wantMsg: "limit",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, code := run(t, tc.args...)
			if code == 0 {
				t.Errorf("exited 0 for %v; it should have refused to start\nstdout:\n%s", tc.args, stdout)
			}
			combined := stdout + stderr
			if !strings.Contains(strings.ToLower(combined), strings.ToLower(tc.wantMsg)) {
				t.Errorf("the error does not mention %q:\n%s", tc.wantMsg, combined)
			}
		})
	}
}

// TestCellsCommandListsEverySelectedSource is the CLI regression, run as a program.
//
// The subject carries the source code, so a mask selecting both networks needs
// subjects for both. The command used to print one network's worth, and the wider the
// mask the more confident the wrong output looked.
func TestCellsCommandListsEverySelectedSource(t *testing.T) {
	stdout, stderr, code := run(t, "cells", "--src-mask=6",
		"--region-name=munich", "--region-lat=48", "--region-lon=11", "--region-radius-km=1")
	if code != 0 {
		t.Fatalf("cells exited %d\n%s", code, stderr)
	}

	for _, want := range []string{
		"sources: src.1, src.2",
		"blitzortung.org",
		"lightningmaps.org",
		"lightning.v1.src.1.cell.",
		"lightning.v1.src.2.cell.",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("cells output does not contain %q:\n%s", want, stdout)
		}
	}

	// The reserved and testing networks carry no data, so listing them would be
	// listing subjects nothing is ever published to.
	if strings.Contains(stdout, "src.8") {
		t.Errorf("cells listed the testing network, which carries nothing:\n%s", stdout)
	}
}

// TestVersionAndHelpAreUsable is trivial and worth keeping: a binary that cannot
// print its own version is a binary nobody can identify in a bug report.
func TestVersionAndHelpAreUsable(t *testing.T) {
	stdout, stderr, code := run(t, "--version")
	if code != 0 {
		t.Errorf("--version exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "dev") && !strings.Contains(stdout, "v") {
		t.Errorf("--version printed %q", stdout)
	}

	stdout, _, code = run(t, "--help")
	if code != 0 {
		t.Errorf("--help exited %d", code)
	}
	for _, want := range []string{"ingest", "history", "cells"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("--help does not mention the %q command", want)
		}
	}
}

// --- helpers -------------------------------------------------------------

// storeRows waits for the archive to hold exactly the inside strokes and no others.
func storeRows(t *testing.T, b *bridge, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var got int64
	for time.Now().Before(deadline) {
		got = storeRowCount(t, b.sqlitePath)
		if got == int64(want) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("the archive holds %d rows, want %d", got, want)
}

func storeRowCount(t *testing.T, path string) int64 {
	t.Helper()
	db, err := store.Open(path)
	if err != nil {
		t.Fatalf("opening the archive %s: %v", path, err)
	}
	defer db.Close()
	n, err := db.Count(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	return n
}

// waitForCursor waits until the resume cursor names source 2 at or beyond id.
func waitForCursor(t *testing.T, f *fixture, source int, within time.Duration) {
	t.Helper()
	db := openArchive(t, filepathOf(f))
	defer db.Close()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		c, err := db.LoadCursor(context.Background())
		if err == nil && c.Sources[model.Source(source)] > 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no cursor was written within %v", within)
}

func openArchive(t *testing.T, path string) *store.Store {
	t.Helper()
	db, err := store.Open(path)
	if err != nil {
		t.Fatalf("opening the archive: %v", err)
	}
	return db
}

func filepathOf(f *fixture) string { return filepath.Join(f.dir, "lightningfeed.db") }

// leadershipBucket opens the KV bucket the bridge stores its lease in.
func leadershipBucket(nc *nats.Conn) (nats.KeyValue, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	return js.KeyValue("lightningfeed")
}

// stealLease overwrites the lease with a different owner, which is what another
// process taking over looks like from here.
func stealLease(t *testing.T, f *fixture, key string) {
	t.Helper()
	nc := f.nats.Connect(t)
	defer nc.Close()

	kv, err := leadershipBucket(nc)
	if err != nil {
		t.Fatalf("opening the leadership bucket: %v", err)
	}
	entry, err := kv.Get(key)
	if err != nil {
		t.Fatalf("reading the lease: %v", err)
	}
	if _, err := kv.Update(key, []byte("somebody-else/999"), entry.Revision()); err != nil {
		t.Fatalf("stealing the lease: %v", err)
	}
}

// leaseOwner returns the current owner of the lease, or "" if it is unheld.
func leaseOwner(t *testing.T, f *fixture, key string) string {
	t.Helper()
	nc := f.nats.Connect(t)
	defer nc.Close()

	kv, err := leadershipBucket(nc)
	if err != nil {
		return ""
	}
	entry, err := kv.Get(key)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(entry.Value()))
}
