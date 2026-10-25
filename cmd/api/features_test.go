package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
)

func featureFixture(priority byte) []byte {
	data := append([]byte("SHFX\x04\x01"), make([]byte, 16)...)
	data = append(data, 0, 0)          // no routes or string pieces
	data = append(data, 0, 0, 0, 0, 1) // segment bounds, one feature
	return append(data, 0, 0, priority, 0, 0, 0)
}

func featureBundle(priority byte) fstest.MapFS {
	return fstest.MapFS{
		"featuredata/features.bin":         {Data: featureFixture(priority)},
		"featuredata/features_cycling.bin": {Data: featureFixture(2)},
	}
}

func featureRequest(handler http.Handler, method, path, etag string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func TestDefaultFeaturesDir(t *testing.T) {
	t.Run("XDG directory", func(t *testing.T) {
		base := t.TempDir()
		t.Setenv("XDG_DATA_HOME", base)
		got, err := defaultFeaturesDir()
		if err != nil || got != filepath.Join(base, "snowhfx", "features") {
			t.Fatalf("default directory = %q, %v", got, err)
		}
	})
	t.Run("home fallback", func(t *testing.T) {
		t.Setenv("XDG_DATA_HOME", "")
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		got, err := defaultFeaturesDir()
		if err != nil || got != filepath.Join(home, ".local", "share", "snowhfx", "features") {
			t.Fatalf("default directory = %q, %v", got, err)
		}
	})
	t.Run("relative XDG directory", func(t *testing.T) {
		t.Setenv("XDG_DATA_HOME", "relative")
		if _, err := defaultFeaturesDir(); err == nil {
			t.Fatal("expected relative XDG_DATA_HOME to be rejected")
		}
	})
}

func TestFeatureManifestAndImmutableFiles(t *testing.T) {
	store, err := newFeatureStore(filepath.Join(t.TempDir(), "features"), featureBundle(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	handler := newAPIHandler(nil, store)
	w := featureRequest(handler, http.MethodGet, "/api/v1/features/manifest.json", "")
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("manifest response: %d %v", w.Code, w.Header())
	}
	var manifest featureManifest
	if err := json.Unmarshal(w.Body.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.FormatVersion != 4 || len(manifest.Datasets) != 2 {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}
	conditional := featureRequest(handler, http.MethodGet, "/api/v1/features/manifest.json", w.Header().Get("ETag"))
	if conditional.Code != http.StatusNotModified || conditional.Body.Len() != 0 {
		t.Fatalf("conditional manifest response: %d %s", conditional.Code, conditional.Body.String())
	}
	for mode, version := range manifest.Datasets {
		path := "/api/v1/features/" + version.SHA256 + ".bin"
		w := featureRequest(handler, http.MethodGet, path, "")
		if w.Code != http.StatusOK || int64(w.Body.Len()) != version.Size {
			t.Fatalf("%s response: %d size=%d want=%d", mode, w.Code, w.Body.Len(), version.Size)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(w.Body.Bytes())); got != version.SHA256 {
			t.Fatalf("%s hash = %s; want %s", mode, got, version.SHA256)
		}
		if w.Header().Get("Cache-Control") != "public, max-age=31536000, immutable" || w.Header().Get("Access-Control-Allow-Origin") != "*" {
			t.Fatalf("unexpected feature headers: %v", w.Header())
		}
		if w.Header().Get("Content-Type") != "application/octet-stream" {
			t.Fatalf("unexpected feature content type: %v", w.Header())
		}
		conditional := featureRequest(handler, http.MethodGet, path, w.Header().Get("ETag"))
		if conditional.Code != http.StatusNotModified || conditional.Header().Get("Cache-Control") != w.Header().Get("Cache-Control") {
			t.Fatalf("conditional feature response: %d %v", conditional.Code, conditional.Header())
		}
		head := featureRequest(handler, http.MethodHead, path, "")
		if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Content-Length") != fmt.Sprint(version.Size) {
			t.Fatalf("HEAD response: %d %v %s", head.Code, head.Header(), head.Body.String())
		}
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Range", "bytes=0-3")
		partial := httptest.NewRecorder()
		handler.ServeHTTP(partial, req)
		if partial.Code != http.StatusPartialContent || partial.Body.String() != "SHFX" || partial.Header().Get("Content-Range") != fmt.Sprintf("bytes 0-3/%d", version.Size) {
			t.Fatalf("range response: %d %v %s", partial.Code, partial.Header(), partial.Body.String())
		}
	}
}

func TestFeatureStorePreservesDeploymentsAndRollbacks(t *testing.T) {
	dir := t.TempDir()
	old, err := newFeatureStore(dir, featureBundle(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { old.Close() })
	current, err := newFeatureStore(dir, featureBundle(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { current.Close() })
	if bytes.Equal(old.manifest, current.manifest) {
		t.Fatal("changed data should change the manifest")
	}
	oldHash := fmt.Sprintf("%x", sha256.Sum256(featureFixture(1)))
	w := featureRequest(newAPIHandler(nil, current), http.MethodGet, "/api/v1/features/"+oldHash+".bin", "")
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), featureFixture(1)) {
		t.Fatalf("old deployment unavailable: %d %s", w.Code, w.Body.String())
	}
	rollback, err := newFeatureStore(dir, featureBundle(1))
	if err != nil || !bytes.Equal(old.manifest, rollback.manifest) || old.manifestETag != rollback.manifestETag {
		t.Fatalf("rollback failed: %v", err)
	}
	t.Cleanup(func() { rollback.Close() })
}

func TestConcurrentFeaturePublication(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	data := featureFixture(1)
	name := fmt.Sprintf("%x.bin", sha256.Sum256(data))
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- publishFeature(root, name, bytes.NewReader(data))
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != name {
		t.Fatalf("unexpected published files: %v, %v", entries, err)
	}
}

func TestFeatureStoreRejectsIncompleteBundle(t *testing.T) {
	for _, kind := range []string{"missing", "invalid", "empty"} {
		t.Run(kind, func(t *testing.T) {
			bundle := featureBundle(1)
			switch kind {
			case "missing":
				delete(bundle, "featuredata/features_cycling.bin")
			case "invalid":
				bundle["featuredata/features_cycling.bin"].Data = []byte("invalid")
			case "empty":
				data := featureFixture(1)
				data[28] = 0 // zero features in the segment
				bundle["featuredata/features_cycling.bin"].Data = data[:29]
			}
			dir := filepath.Join(t.TempDir(), "features")
			if _, err := newFeatureStore(dir, bundle); err == nil {
				t.Fatal("expected incomplete bundle to fail")
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("incomplete bundle should not be published")
			}
		})
	}
}

func TestFeatureFilesRejectCorruptionAndUnknownNames(t *testing.T) {
	dir := t.TempDir()
	store, err := newFeatureStore(dir, featureBundle(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	handler := newAPIHandler(nil, store)
	for _, name := range []string{"features.bin", strings.Repeat("0", 64) + ".bin", strings.Repeat("g", 64) + ".bin"} {
		w := featureRequest(handler, http.MethodGet, "/api/v1/features/"+name, "")
		if w.Code != http.StatusNotFound || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("unknown name response: %d %v", w.Code, w.Header())
		}
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(featureFixture(1)))
	if err := os.WriteFile(filepath.Join(dir, hash+".bin"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	w := featureRequest(handler, http.MethodGet, "/api/v1/features/"+hash+".bin", "")
	if w.Code != http.StatusInternalServerError || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("corrupt content response: %d %v", w.Code, w.Header())
	}
	if _, err := newFeatureStore(dir, featureBundle(1)); err == nil {
		t.Fatal("startup should reject an existing hash with changed content")
	}
	data, err := os.ReadFile(filepath.Join(dir, hash+".bin"))
	if err != nil || string(data) != "changed" {
		t.Fatal("startup overwrote an existing immutable file")
	}
}

func TestFeatureRootRejectsEscapingSymlinks(t *testing.T) {
	for _, relative := range []bool{false, true} {
		t.Run(fmt.Sprintf("relative=%v", relative), func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "features")
			store, err := newFeatureStore(dir, featureBundle(1))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { store.Close() })
			data := featureFixture(3)
			hash := fmt.Sprintf("%x", sha256.Sum256(data))
			outside := filepath.Join(parent, "outside.bin")
			if err := os.WriteFile(outside, data, 0600); err != nil {
				t.Fatal(err)
			}
			target := outside
			if relative {
				target = "../outside.bin"
			}
			if err := os.Symlink(target, filepath.Join(dir, hash+".bin")); err != nil {
				t.Fatal(err)
			}
			w := featureRequest(newAPIHandler(nil, store), http.MethodGet, "/api/v1/features/"+hash+".bin", "")
			if w.Code != http.StatusInternalServerError || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("outside file was served: %d %s", w.Code, w.Body.String())
			}
			if _, err := newFeatureStore(dir, featureBundle(3)); err == nil {
				t.Fatal("startup accepted a published path pointing outside the root")
			}
		})
	}
}

func TestFeatureRootTracksDirectoryRename(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "features")
	store, err := newFeatureStore(dir, featureBundle(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := os.Rename(dir, filepath.Join(parent, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(featureFixture(1)))
	w := featureRequest(newAPIHandler(nil, store), http.MethodGet, "/api/v1/features/"+hash+".bin", "")
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), featureFixture(1)) {
		t.Fatalf("root did not retain the original directory: %d %s", w.Code, w.Body.String())
	}
}

type openOnlyBundle struct{ fs.FS }

func (openOnlyBundle) ReadFile(string) ([]byte, error) {
	return nil, errors.New("whole-file reads are disabled")
}

func TestFeatureBundleUsesStreamingReads(t *testing.T) {
	store, err := newFeatureStore(t.TempDir(), openOnlyBundle{featureBundle(1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
}

func TestFailedFeatureCopyIsNotPublished(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	hash := fmt.Sprintf("%x", sha256.Sum256(featureFixture(1)))
	if err := publishFeature(root, hash+".bin", strings.NewReader("wrong content")); err == nil {
		t.Fatal("unexpected content was published at a hash URL")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed copy left files behind: %v, %v", entries, err)
	}
}
