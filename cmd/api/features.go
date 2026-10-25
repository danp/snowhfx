package main

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/danp/snowhfx/internal/featuresbin"
)

// The directory always contains its README so tests compile without generated data.
// Build deployable executables with scripts/build-api.sh to include both binaries.
//
//go:embed featuredata
var bundledFeatures embed.FS

type featureVersion struct {
	SHA256 string `json:"sha256"`
	Size   int    `json:"size"`
}

type featureManifest struct {
	FormatVersion uint8                     `json:"format_version"`
	Datasets      map[string]featureVersion `json:"datasets"`
}

type featureStore struct {
	dir          string
	manifest     []byte
	manifestETag string
}

func defaultFeaturesDir() (string, error) {
	base := os.Getenv("XDG_DATA_HOME")
	if base != "" {
		if !filepath.IsAbs(base) {
			return "", errors.New("XDG_DATA_HOME must be an absolute path")
		}
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		base = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(base, "snowhfx", "features"), nil
}

func newFeatureStore(dir string, bundle fs.FS) (*featureStore, error) {
	manifest := featureManifest{Datasets: make(map[string]featureVersion)}
	// Validate the complete bundle before publishing any of it.
	contents := make(map[string][]byte)
	for _, dataset := range []struct{ mode, file string }{
		{"sidewalks", "features.bin"},
		{"cycling", "features_cycling.bin"},
	} {
		data, err := fs.ReadFile(bundle, "featuredata/"+dataset.file)
		if err != nil {
			return nil, fmt.Errorf("read bundled %s: %w; build with ./scripts/build-api.sh", dataset.file, err)
		}
		features, _, header, err := featuresbin.Read(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("validate bundled %s: %w", dataset.file, err)
		}
		if len(features) == 0 {
			return nil, fmt.Errorf("bundled %s contains no features", dataset.file)
		}
		manifest.FormatVersion = header.FormatVersion
		hash := fmt.Sprintf("%x", sha256.Sum256(data))
		manifest.Datasets[dataset.mode] = featureVersion{SHA256: hash, Size: len(data)}
		contents[hash] = data
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create features directory: %w", err)
	}
	for hash, data := range contents {
		if err := publishFeature(dir, hash+".bin", data); err != nil {
			return nil, err
		}
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	return &featureStore{
		dir:          dir,
		manifest:     data,
		manifestETag: fmt.Sprintf(`"%x"`, sha256.Sum256(data)),
	}, nil
}

func publishFeature(dir, name string, data []byte) error {
	destination := filepath.Join(dir, name)
	verifyExisting := func() error {
		existing, err := os.ReadFile(destination)
		if err != nil {
			return err
		}
		if !bytes.Equal(existing, data) {
			return fmt.Errorf("immutable feature file %s has unexpected content", destination)
		}
		return nil
	}
	if _, err := os.Stat(destination); err == nil {
		return verifyExisting()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temp, err := os.CreateTemp(dir, ".features-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	if _, err := temp.Write(data); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	// A hard link publishes a complete file atomically without replacing an
	// existing version, including if two API processes start concurrently.
	if err := os.Link(temp.Name(), destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return verifyExisting()
		}
		return fmt.Errorf("publish %s: %w", destination, err)
	}
	return nil
}

func (store *featureStore) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/features/manifest.json", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("ETag", store.manifestETag)
		w.Header().Set("Content-Type", "application/json")
		http.ServeContent(w, r, "manifest.json", time.Time{}, bytes.NewReader(store.manifest))
	})
	mux.HandleFunc("GET /api/v1/features/{filename}", store.serveFeature)
}

func (store *featureStore) serveFeature(w http.ResponseWriter, r *http.Request) {
	setCORSHeaders(w)
	w.Header().Set("Cache-Control", "no-store")
	name := r.PathValue("filename")
	hash, ok := strings.CutSuffix(name, ".bin")
	decoded, err := hex.DecodeString(hash)
	if !ok || err != nil || len(decoded) != sha256.Size || hash != strings.ToLower(hash) {
		http.NotFound(w, r)
		return
	}
	data, err := os.ReadFile(filepath.Join(store.dir, name))
	if errors.Is(err, os.ErrNotExist) {
		http.NotFound(w, r)
		return
	}
	if err != nil || sha256.Sum256(data) != [sha256.Size]byte(decoded) {
		http.Error(w, "feature data unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("ETag", `"`+hash+`"`)
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
}
