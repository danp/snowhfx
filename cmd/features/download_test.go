package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testGeoJSON = `{"type":"FeatureCollection","features":[]}`

// fakeHub serves the Hub download API at /items/{id}/geojson and the export
// at /result. Each handler is called with the 1-based request count.
type fakeHub struct {
	polls   atomic.Int32
	results atomic.Int32
	poll    func(w http.ResponseWriter, n int, resultURL string)
	result  func(w http.ResponseWriter, n int)
}

func newFakeHub(t *testing.T, timeout time.Duration) *fakeHub {
	t.Helper()
	h := &fakeHub{
		result: func(w http.ResponseWriter, n int) { io.WriteString(w, testGeoJSON) },
	}
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/items/"):
			h.poll(w, int(h.polls.Add(1)), srv.URL+"/result")
		case r.URL.Path == "/result":
			h.result(w, int(h.results.Add(1)))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	oldURL, oldTimeout, oldPoll, oldBackoff := hubDownloadURL, downloadTimeout, downloadPollInterval, downloadMaxBackoff
	t.Cleanup(func() {
		hubDownloadURL, downloadTimeout, downloadPollInterval, downloadMaxBackoff = oldURL, oldTimeout, oldPoll, oldBackoff
	})
	hubDownloadURL = srv.URL + "/items/%s/geojson"
	downloadTimeout = timeout
	downloadPollInterval = time.Millisecond
	downloadMaxBackoff = 5 * time.Millisecond

	return h
}

func completed(w http.ResponseWriter, resultURL string) {
	io.WriteString(w, `{"message":"Download is available at the result url.","status":"Completed","resultUrl":"`+resultURL+`"}`)
}

func exporting(w http.ResponseWriter) {
	io.WriteString(w, `{"message":"Up to date download file is being generated. Please check back again later.","status":"ExportingData"}`)
}

func readDownload(t *testing.T) (string, error) {
	t.Helper()
	rc, err := download(context.Background(), "item")
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading download: %v", err)
	}
	return string(b), nil
}

func TestDownloadFirstTry(t *testing.T) {
	h := newFakeHub(t, 5*time.Second)
	h.poll = func(w http.ResponseWriter, n int, resultURL string) { completed(w, resultURL) }

	got, err := readDownload(t)
	if err != nil {
		t.Fatal(err)
	}
	if got != testGeoJSON {
		t.Fatalf("got %q, want %q", got, testGeoJSON)
	}
	if n := h.polls.Load(); n != 1 {
		t.Fatalf("got %d polls, want 1", n)
	}
}

func TestDownloadRetriesHubError(t *testing.T) {
	h := newFakeHub(t, 5*time.Second)
	h.poll = func(w http.ResponseWriter, n int, resultURL string) {
		switch n {
		case 1:
			http.Error(w, `{"message":"bad request"}`, http.StatusBadRequest)
		case 2:
			exporting(w)
		default:
			completed(w, resultURL)
		}
	}

	got, err := readDownload(t)
	if err != nil {
		t.Fatal(err)
	}
	if got != testGeoJSON {
		t.Fatalf("got %q, want %q", got, testGeoJSON)
	}
	if n := h.polls.Load(); n != 3 {
		t.Fatalf("got %d polls, want 3", n)
	}
}

func TestDownloadRetriesResultError(t *testing.T) {
	h := newFakeHub(t, 5*time.Second)
	h.poll = func(w http.ResponseWriter, n int, resultURL string) { completed(w, resultURL) }
	h.result = func(w http.ResponseWriter, n int) {
		if n == 1 {
			http.Error(w, "<Error>unavailable</Error>", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, testGeoJSON)
	}

	got, err := readDownload(t)
	if err != nil {
		t.Fatal(err)
	}
	if got != testGeoJSON {
		t.Fatalf("got %q, want %q", got, testGeoJSON)
	}
	if n := h.results.Load(); n != 2 {
		t.Fatalf("got %d result requests, want 2", n)
	}
}

func TestDownloadTimesOut(t *testing.T) {
	h := newFakeHub(t, 100*time.Millisecond)
	h.poll = func(w http.ResponseWriter, n int, resultURL string) {
		http.Error(w, `{"message":"still broken"}`, http.StatusBadRequest)
	}

	start := time.Now()
	_, err := readDownload(t)
	if err == nil {
		t.Fatal("got no error, want timeout")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %v, want about 100ms", elapsed)
	}
	for _, want := range []string{"timed out waiting for item export", "400", "still broken"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if n := h.polls.Load(); n < 2 {
		t.Errorf("got %d polls, want retries", n)
	}
}

func TestDownloadFailedExport(t *testing.T) {
	h := newFakeHub(t, 5*time.Second)
	h.poll = func(w http.ResponseWriter, n int, resultURL string) {
		io.WriteString(w, `{"message":"Export failed.","status":"Failed"}`)
	}

	_, err := readDownload(t)
	if err == nil {
		t.Fatal("got no error, want export failure")
	}
	if !strings.Contains(err.Error(), "Failed") || !strings.Contains(err.Error(), "Export failed.") {
		t.Errorf("error %q does not mention the failed status", err)
	}
	if n := h.polls.Load(); n != 1 {
		t.Errorf("got %d polls, want 1", n)
	}
	if n := h.results.Load(); n != 0 {
		t.Errorf("got %d result requests, want 0", n)
	}
}
