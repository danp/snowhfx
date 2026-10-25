Generated feature binaries are embedded from this directory by `go build`.

Use `./scripts/build-api.sh` from the repository root to regenerate both files and
build a deployable API executable. Generated `.bin` files are ignored by Git.
The README keeps ordinary builds and tests working in a clean checkout; an API
without both generated files exits at startup with build instructions.
