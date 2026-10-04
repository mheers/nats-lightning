// Package natsd starts a real NATS server inside a test process.
//
// The point is to test against a real broker. JetStream's duplicate window,
// stream storage and subject routing are the behaviours this project's
// correctness depends on, and none of them can be demonstrated against a fake
// publisher that merely agrees with itself.
package natsd

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Server is a running NATS server with JetStream enabled and a client
// connection.
type Server struct {
	*server.Server

	// URL is the client connection string.
	URL string
}

// Start launches a server on a random free port with JetStream storage backed by
// a temporary directory, and shuts it down when the test ends.
func Start(t *testing.T) *Server {
	t.Helper()

	port, err := freePort()
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	dir := t.TempDir()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      port,
		JetStream: true,
		StoreDir:  filepath.Join(dir, "jetstream"),

		// Keep the server from writing noise into the test output.
		NoLog:  true,
		NoSigs: true,
	}

	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}

	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatalf("server did not become ready")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
		_ = os.RemoveAll(dir)
	})

	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	return &Server{Server: srv, URL: url}
}

// Connect returns a fresh client connection, closed when the test ends.
func (s *Server) Connect(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.URL)
	if err != nil {
		t.Fatalf("connecting to %s: %v", s.URL, err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// StreamInfo returns a stream's configuration and state.
//
// It opens its own short-lived connection rather than sharing one held by the
// Server, because a connection cached on a long-lived test helper is a trap: the
// moment anything closes it, every later caller fails with a bare
// "connection closed".
func (s *Server) StreamInfo(t *testing.T, stream string) (*nats.StreamInfo, error) {
	t.Helper()
	nc, err := nats.Connect(s.URL)
	if err != nil {
		t.Fatalf("connecting to %s: %v", s.URL, err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js.StreamInfo(stream)
}

// StreamMessageCount returns how many messages a stream holds.
func (s *Server) StreamMessageCount(t *testing.T, stream string) uint64 {
	t.Helper()
	info, err := s.StreamInfo(t, stream)
	if err != nil {
		t.Fatalf("stream info for %q: %v", stream, err)
	}
	return info.State.Msgs
}

// freePort reserves an ephemeral port and releases it, so the server can bind
// it. There is a small race here, which is why the server also reports its own
// listen errors via ReadyForConnections.
func freePort() (int, error) {
	l, err := listenAny()
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return portOf(l), nil
}
func listenAny() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}

func portOf(l net.Listener) int {
	return l.Addr().(*net.TCPAddr).Port
}
