package e2e

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/heers-it/lightningfeed/internal/testsupport/fakews"
	"github.com/heers-it/lightningfeed/internal/testsupport/natsd"
)

// This file, and the tests beside it, run the compiled binary as a real process.
//
// Nothing else in the suite does. The pipeline's own packages are exercised through
// ingest.RunWith in-process, which covers the wiring but not the things a process is
// actually for: the command line, the exit status, signal handling, the metrics port
// as configured rather than as returned, and leadership handover between two
// instances. Those are precisely the properties that were wrong at least once — a
// lost lease exiting 0, and a documented `history` invocation reporting an empty
// archive — and neither was visible from inside the process.
//
// None of this needs the live upstream. The fake is a real WebSocket server and the
// broker is a real nats-server, so the whole pipeline runs for real without spending
// the connection tolerance that only live_test.go is allowed to spend.

// binary is the compiled bridge, built once for the whole package.
var binary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "lightningfeed-e2e-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: temp dir:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	// The package's own directory is two levels below the module root.
	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))

	binary = filepath.Join(dir, "lightningfeed")
	build := exec.Command("go", "build", "-o", binary, "./cmd/lightningfeed")
	build.Dir = root
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: building the bridge: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// syncBuffer collects a subprocess's output safely.
//
// os/exec copies a child's stdout and stderr from its own goroutines, so a plain
// strings.Builder is being written while the test reads it. That is a real data race
// and the race detector finds it every time, which is exactly what it is for: the
// symptom was a test that passed without -race and failed with it.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// bridge is a running lightningfeed process under test.
type bridge struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdout *syncBuffer
	stderr *syncBuffer

	metricsAddr string
	sqlitePath  string
	leaderKey   string
	natsURL     string
	upstreamURL string

	waitOnce sync.Once
	waitErr  error
	waitCode int
}

// regionFlag is the region the deployment is configured for: Roussospiti, Crete.
// Every stroke these tests publish is inside it, and one deliberately outside, so
// that "published" and "filtered" are both observable.
var regionFlag = []string{
	"--region-name=roussospiti",
	"--region-lat=35.3340688",
	"--region-lon=24.4944483",
	"--region-radius-km=10",
}

const (
	regionLat    = 35.3340688
	regionLon    = 24.4944483
	insideLat    = 35.3341
	insideLon    = 24.4945
	outsideLat   = 52.5200
	outsideLon   = 13.4050
	defaultLease = "lightningfeed/leader"
)

// strokeFrame renders an upstream frame carrying `n` strokes inside the region and
// one far outside it, all stamped `age` in the past.
//
// The outside stroke is what makes the region filter observable: it must reach
// neither NATS nor the archive.
func strokeFrame(age time.Duration, n int) string {
	now := time.Now().Add(-age)
	var b strings.Builder
	fmt.Fprintf(&b, `{"time":%g,"strokes":[`, float64(now.UnixMilli())/1000)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		// Distinct ids so the identity filter does not collapse them.
		fmt.Fprintf(&b, `{"time":%d,"lat":%v,"lon":%v,"src":2,"id":%d,"del":1800,"dev":500}`,
			now.UnixMilli(), insideLat, insideLon, i+1)
	}
	fmt.Fprintf(&b, `,{"time":%d,"lat":%v,"lon":%v,"src":2,"id":9999,"del":1800,"dev":500}]}`,
		now.UnixMilli(), outsideLat, outsideLon)
	return b.String()
}

// fixture is the whole environment one test needs: a broker, an upstream, and a
// place on disk.
type fixture struct {
	nats     *natsd.Server
	upstream *fakews.Server
	dir      string
}

func newFixture(t *testing.T, up *fakews.Server) *fixture {
	t.Helper()
	return &fixture{
		nats:     natsd.Start(t),
		upstream: fakews.Start(t, up),
		dir:      t.TempDir(),
	}
}

// defaultUpstream is a fake that keeps streaming heartbeats forever and never
// closes, so the bridge holds one connection and the tests can assert on it.
func defaultUpstream(n int) *fakews.Server {
	return &fakews.Server{
		Frames:              []string{strokeFrame(0, n)},
		FrameDelay:          50 * time.Millisecond,
		HeartbeatForever:    true,
		FramesPerConnection: true,
		HonourSourceMask:    true,
	}
}

// startIngest launches `lightningfeed ingest` and waits for it to report ready.
func (f *fixture) startIngest(t *testing.T, leaderElect bool, extra ...string) *bridge {
	t.Helper()
	b := f.launchIngest(t, leaderElect, extra...)
	b.waitReady(t, 30*time.Second)
	return b
}

// startIngestNoWait launches the bridge without requiring readiness.
//
// A standby is *expected* never to become ready while another instance holds the
// lease, so waiting for readiness would hang on exactly the case a test wants to
// observe. Readiness is asserted separately, and negatively.
func (f *fixture) startIngestNoWait(t *testing.T, leaderElect bool, extra ...string) *bridge {
	t.Helper()
	return f.launchIngest(t, leaderElect, extra...)
}

func (f *fixture) launchIngest(t *testing.T, leaderElect bool, extra ...string) *bridge {
	t.Helper()

	port, err := freePort()
	if err != nil {
		t.Fatalf("reserving a metrics port: %v", err)
	}

	args := append([]string{
		"ingest",
		"--nats-url=" + f.nats.URL,
		"--upstream-url=" + f.upstream.URL(),
		"--stream=E2E",
		"--sqlite=" + filepath.Join(f.dir, "lightningfeed.db"),
		"--metrics-addr=127.0.0.1:" + port,
		"--log-level=debug",
	}, regionFlag...)
	if !leaderElect {
		args = append(args, "--leader-elect=false")
	}
	args = append(args, extra...)

	b := &bridge{
		t:           t,
		metricsAddr: "127.0.0.1:" + port,
		sqlitePath:  filepath.Join(f.dir, "lightningfeed.db"),
		leaderKey:   defaultLease,
		natsURL:     f.nats.URL,
		upstreamURL: f.upstream.URL(),
		stdout:      &syncBuffer{},
		stderr:      &syncBuffer{},
	}

	b.cmd = exec.Command(binary, args...)
	b.cmd.Stdout = b.stdout
	b.cmd.Stderr = b.stderr
	b.cmd.Env = append(os.Environ(), "LIGHTNINGFEED_LOG_FORMAT=text")
	if err := b.cmd.Start(); err != nil {
		t.Fatalf("starting ingest: %v", err)
	}

	t.Cleanup(func() {
		if b.cmd.ProcessState == nil {
			_ = b.cmd.Process.Kill()
		}
		_ = b.cmd.Wait()
		if t.Failed() {
			t.Logf("stdout:\n%s\nstderr:\n%s", b.stdout, b.stderr)
		}
	})

	return b
}

// run executes the binary with args and returns stdout, stderr and the exit code.
// It is the entry point for the CLI tests that are about the exit status.
func run(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(binary, args...)
	var out, errb syncBuffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("running %v: %v", args, err)
		}
		code = ee.ExitCode()
	}
	return out.String(), errb.String(), code
}

// waitReady blocks until /healthz reports ready.
func (b *bridge) waitReady(t *testing.T, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last string
	for time.Now().Before(deadline) {
		body, code, err := b.get("/healthz")
		if err == nil && code == http.StatusOK && strings.Contains(body, `"ready":true`) {
			return
		}
		if err == nil {
			last = body
		}
		if b.exited() {
			t.Fatalf("the bridge exited before becoming ready:\nstdout:\n%s\nstderr:\n%s",
				b.stdout, b.stderr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the bridge did not become ready within %v (last health: %q)\nstdout:\n%s\nstderr:\n%s",
		within, last, b.stdout, b.stderr)
}

func (b *bridge) get(path string) (body string, code int, err error) {
	resp, err := http.Get("http://" + b.metricsAddr + path)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return string(raw), resp.StatusCode, err
}

// metrics scrapes the exposition.
func (b *bridge) metrics(t *testing.T) string {
	t.Helper()
	body, code, err := b.get("/metrics")
	if err != nil {
		t.Fatalf("scraping metrics: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("GET /metrics returned %d: %s", code, body)
	}
	return body
}

// exited reports whether the process has already terminated.
func (b *bridge) exited() bool {
	if b.cmd.ProcessState != nil {
		return true
	}
	// Signal 0 probes without disturbing: a live process answers, a reaped one
	// does not. This is a hint, not a synchronisation point.
	return b.cmd.Process.Signal(syscall.Signal(0)) != nil
}

// waitExit blocks until the process terminates and returns its exit code.
func (b *bridge) waitExit(t *testing.T, within time.Duration) int {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- b.cmd.Wait() }()

	select {
	case err := <-done:
		b.waitOnce.Do(func() {
			b.waitErr = err
			if err != nil {
				var ee *exec.ExitError
				if errors.As(err, &ee) {
					b.waitCode = ee.ExitCode()
				} else {
					b.waitCode = -1
				}
			}
		})
		return b.waitCode
	case <-time.After(within):
		t.Fatalf("the bridge did not exit within %v\nstdout:\n%s\nstderr:\n%s",
			within, b.stdout, b.stderr)
		return 0
	}
}

// signal sends sig to the process.
func (b *bridge) signal(t *testing.T, sig os.Signal) {
	t.Helper()
	if err := b.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("signalling the bridge: %v", err)
	}
}

// streamCount reads the message count straight from the broker.
func (f *fixture) streamCount(t *testing.T) uint64 {
	t.Helper()
	return f.nats.StreamMessageCount(t, "E2E")
}

// waitForStream blocks until the stream holds at least n messages.
func (f *fixture) waitForStream(t *testing.T, n uint64, within time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(within)
	var last uint64
	for time.Now().Before(deadline) {
		last = f.streamCount(t)
		if last >= n {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the stream held %d messages after %v, want at least %d\n%s",
		last, within, n, "")
	return last
}

// sampleMetrics extracts a single Prometheus sample value, failing if absent.
func sampleMetrics(t *testing.T, exposition, series string) float64 {
	t.Helper()
	v, ok := trySample(exposition, series)
	if !ok {
		t.Fatalf("series %q is absent from the exposition", series)
	}
	return v
}

// trySample is sampleMetrics without the failure, for polling loops.
func trySample(exposition, series string) (float64, bool) {
	for _, line := range strings.Split(exposition, "\n") {
		if !strings.HasPrefix(line, series+" ") {
			continue
		}
		fields := strings.Fields(line)
		var v float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &v); err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

// waitForSample blocks until a series reports exactly want.
//
// Several gauges are deliberately polled rather than pushed — connection state and
// leadership are copied across on a timer — so asserting on them the instant a
// downstream effect appears is a race with that timer rather than a real signal. A
// gauge that never reaches the value is a genuine failure and still reported.
func (b *bridge) waitForSample(t *testing.T, series string, want float64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last float64
	var seen bool
	for time.Now().Before(deadline) {
		m := b.metrics(t)
		if v, ok := trySample(m, series); ok {
			last, seen = v, true
			if v == want {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !seen {
		t.Fatalf("series %q never appeared in the exposition", series)
	}
	t.Errorf("%s = %v after %v, want %v", series, last, within, want)
}

// freePort reserves and releases a port, the same way the metrics server does when
// given port 0. Racy in principle; in practice the window is microseconds and the
// alternative is a fixed port that collides with a parallel package.
func freePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		l.Close()
		return "", err
	}
	if err := l.Close(); err != nil {
		return "", err
	}
	return port, nil
}

// scanLines is a small helper for assertions that read the bridge's own log output.
func scanLines(s string) []string {
	var out []string
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			out = append(out, line)
		}
	}
	return out
}
