#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

# Generate on the build host even when GOOS/GOARCH target a different server.
GOOS="$(go env GOHOSTOS)" GOARCH="$(go env GOHOSTARCH)" go run ./cmd/features "$@" \
  -out-travelways cmd/api/featuredata/features.bin \
  -out-bike cmd/api/featuredata/features_cycling.bin

go build -o "${SNOWHFX_API_OUT:-api}" ./cmd/api
