package ingest

import (
	"bytes"
	"net/http"
	"net/http/httptest"
)

// recorder captures a handler's response, so a health assertion can inspect the
// status and body without opening a socket.
type recorder struct {
	code int
	body *bytes.Buffer
}

func newRecorder() *recorder {
	return &recorder{code: http.StatusOK, body: &bytes.Buffer{}}
}

func (r *recorder) Header() http.Header { return http.Header{} }

func (r *recorder) Write(b []byte) (int, error) { return r.body.Write(b) }

func (r *recorder) WriteHeader(code int) { r.code = code }

func newRequest() *http.Request { return httptest.NewRequest(http.MethodGet, "/healthz", nil) }
