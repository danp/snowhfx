package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

type discardResponseWriter struct {
	header http.Header
}

func (w *discardResponseWriter) Header() http.Header         { return w.header }
func (w *discardResponseWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardResponseWriter) WriteHeader(int)             {}

func BenchmarkServeFeature(b *testing.B) {
	for _, size := range []int{1 << 20, 16 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			dir := b.TempDir()
			store, err := newFeatureStore(dir, featureBundle(1))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { store.Close() })
			data := bytes.Repeat([]byte{1}, size)
			hash := fmt.Sprintf("%x", sha256.Sum256(data))
			if err := os.WriteFile(filepath.Join(dir, hash+".bin"), data, 0600); err != nil {
				b.Fatal(err)
			}
			handler := newAPIHandler(nil, store)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/features/"+hash+".bin", nil)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				handler.ServeHTTP(&discardResponseWriter{header: make(http.Header)}, req)
			}
		})
	}
}
