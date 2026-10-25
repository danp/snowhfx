package main

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	Size   int64  `json:"size"`
}

type featureManifest struct {
	FormatVersion uint8                     `json:"format_version"`
	Datasets      map[string]featureVersion `json:"datasets"`
}

type featureStore struct {
	root         *os.Root
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
	datasets := []struct{ mode, file string }{
		{"sidewalks", "features.bin"},
		{"cycling", "features_cycling.bin"},
	}
	for _, dataset := range datasets {
		file, err := bundle.Open("featuredata/" + dataset.file)
		if err != nil {
			return nil, fmt.Errorf("read bundled %s: %w; build with ./scripts/build-api.sh", dataset.file, err)
		}
		version, header, err := inspectFeature(file)
		if err != nil {
			return nil, fmt.Errorf("validate bundled %s: %w", dataset.file, err)
		}
		manifest.FormatVersion = header.FormatVersion
		manifest.Datasets[dataset.mode] = version
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create features directory: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	complete := false
	defer func() {
		if !complete {
			root.Close()
		}
	}()
	for _, dataset := range datasets {
		file, err := bundle.Open("featuredata/" + dataset.file)
		if err != nil {
			return nil, err
		}
		err = publishFeature(root, manifest.Datasets[dataset.mode].SHA256+".bin", file)
		file.Close()
		if err != nil {
			return nil, err
		}
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	complete = true
	return &featureStore{
		root:         root,
		manifest:     data,
		manifestETag: fmt.Sprintf(`"%x"`, sha256.Sum256(data)),
	}, nil
}

func inspectFeature(file fs.File) (featureVersion, featuresbin.Header, error) {
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return featureVersion{}, featuresbin.Header{}, err
	}
	hash := sha256.New()
	header, err := featuresbin.Validate(io.TeeReader(file, hash))
	if err != nil {
		return featureVersion{}, featuresbin.Header{}, err
	}
	return featureVersion{SHA256: hex.EncodeToString(hash.Sum(nil)), Size: info.Size()}, header, nil
}

func (store *featureStore) Close() error {
	return store.root.Close()
}

func verifyFeature(file *os.File, expectedHash string) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("feature file %s is not a regular file", file.Name())
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != expectedHash {
		return fmt.Errorf("immutable feature file %s has unexpected content", file.Name())
	}
	_, err = file.Seek(0, io.SeekStart)
	return err
}

func publishFeature(root *os.Root, name string, source io.Reader) error {
	hash := strings.TrimSuffix(name, ".bin")
	verifyExisting := func() error {
		file, err := root.Open(name)
		if err != nil {
			return err
		}
		defer file.Close()
		return verifyFeature(file, hash)
	}
	if err := verifyExisting(); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tempName, err := randomID(".features-", 16)
	if err != nil {
		return err
	}
	temp, err := root.OpenFile(tempName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer root.Remove(tempName)
	defer temp.Close()
	if _, err := io.Copy(temp, source); err != nil {
		return err
	}
	if _, err := temp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := verifyFeature(temp, hash); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	// A hard link publishes a complete file atomically without replacing an
	// existing version, including if two API processes start concurrently.
	if err := root.Link(tempName, name); err != nil {
		if errors.Is(err, os.ErrExist) {
			return verifyExisting()
		}
		return fmt.Errorf("publish %s: %w", name, err)
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
	file, err := store.root.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, "feature data unavailable", http.StatusInternalServerError)
		return
	}
	defer file.Close()
	if err := verifyFeature(file, hash); err != nil {
		http.Error(w, "feature data unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("ETag", `"`+hash+`"`)
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, name, time.Time{}, file)
}
