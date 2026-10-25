# snowhfx

[View the map](https://snowhfx.danp.net/)

A better Halifax sidewalk and cycling snow clearing map.

## How it works

Every hour, a system of mine runs:

``` shell
scraperlite -db snow.db https://www.halifax.ca/transportation/winter-operations/service-updates \
  clearingOps.html '#tablefield-wrapper-paragraph-125991-field_table-0' \
  updateTime.txt '#tablefield-paragraph-125991-field_table-0 > thead > tr > th.row_0.col_1.c-table__cell' \
  serviceUpdate.txt '#tablefield-paragraph-125991-field_table-0 > tbody > tr:nth-child(2) > td.row_1.col_1.c-table__cell' \
  endTime.txt '#tablefield-paragraph-125991-field_table-0 > tbody > tr:nth-child(3) > td.row_2.col_1.c-table__cell'
```

This uses [scraperlite](https://github.com/danp/scraperlite) to scrape key parts of the [Halifax Winter Operations Service Updates](https://www.halifax.ca/transportation/winter-operations/service-updates) page, such as weather event end times.

The output of that is visible [here](https://hrm.datasette.danp.net/snow), in the `observations` and `contents` tables.

`cmd/features` downloads the [Active Travelways](https://data-hrm.hub.arcgis.com/datasets/a3631c7664ef4ecb93afb1ea4c12022b_0/explore), [Bike Infrastructure and Suggested Routes](https://data-hrm.hub.arcgis.com/datasets/HRM::bike-infrastructure-and-suggested-routes/explore), and [Ice Routes](https://data-hrm.hub.arcgis.com/datasets/HRM::ice-routes/explore) datasets and builds `features.bin` and `features_cycling.bin`.
`features.bin` encodes travelways:

* lines for each travelway (sidewalk, path, etc)
* their titles
* their snow clearing priority (1/2/3)

`features_cycling.bin` encodes cycling routes. Protected bike routes inherit priorities by matching against nearby travelways; other routes match ice routes first. If a match can't be found, `WINT_LOS` is used as a fallback. Routes marked as not plowed (or that match a nearby no-plow travelway) are skipped. Both files include a source dataset id to support popups.

`cmd/events` is run after `scraperlite` against the same database to produce the `events` table, visible [here](https://hrm.datasette.danp.net/snow/events).
It scans through all observed changes and builds a log of notable changes to the service update and weather event end times.
From those it derives states.

`cmd/api` runs an API server against that same database and serves event data plus community condition reports:

* `GET /api/v1/current-event`
* `GET /api/v1/features/manifest.json`
* `GET /api/v1/features/<sha256>.bin`
* `GET /api/v1/surface-conditions`
* `GET /api/v1/community-reports?dataset_mode=...`
* `POST /api/v1/community-reports`

Reports auto-expire after 3 days, and all active reports are expired when a new weather event id is observed.

Surface conditions are unavailable when the latest observation has no temperature or is more than 2 hours old. Missing temperatures remain null, and temperature trends use valid readings from the last 12 hours. Observation freshness is checked when serving cached conditions as well as when computing them.

`index.html` uses the API for feature datasets, the current weather event, and community reports.

The site runs on GitHub Pages. `.github/workflows/pages.yml` publishes `index.html` directly from `main` using GitHub Actions; no separate `pages` or `gh-pages` branch is required. In the repository's Pages settings, the publishing source should be **GitHub Actions**.

While the map is visible, it refreshes weather events every 5 minutes and community reports every 2 minutes. Both refresh immediately when returning to the tab. Failed requests retain the last successful data and show when it was last checked.

## Build and deploy the API

Build a single executable containing fresh feature datasets:

```sh
./scripts/build-api.sh
```

This downloads the municipal datasets, generates both feature binaries, and embeds them in `api`. Building for a Linux amd64 server from another platform works with:

```sh
GOOS=linux GOARCH=amd64 ./scripts/build-api.sh
```

Use `GOARCH=arm64` instead for an ARM64 server. The generator always runs on the build host. Set `SNOWHFX_API_OUT` to choose another executable output path. Existing local GeoJSON files can be supplied with the generator's `-travelways`, `-bike`, and `-ice` flags.

SCP the executable to your server as a temporary filename, rename it into its usual location, and restart your service as before. Deploy the API before publishing a frontend that uses the feature manifest.

On startup, the API publishes its bundled datasets under their SHA-256 filenames in a persistent, user-owned directory. The default is `$XDG_DATA_HOME/snowhfx/features`, or `~/.local/share/snowhfx/features` when `XDG_DATA_HOME` is unset. It creates the directory itself; root access and changes to `/var/lib` are unnecessary. An explicit directory can be supplied with:

```sh
./api -db snow.db -features-dir "$HOME/snowhfx-data/features"
```

Keep the same features directory across deployments and rollbacks. Older files remain available for existing tabs, while the manifest always describes the running executable's bundled datasets. Data updates require another build and deploy; startup performs no dataset downloads. A plain `go build ./cmd/api` uses any previously generated bundle; on a clean checkout, the API exits at startup with instructions to use the build script.

The manifest uses `Cache-Control: no-cache` with an ETag for conditional revalidation. Hashed binaries use `Cache-Control: public, max-age=31536000, immutable`; a hash URL never serves different bytes. Neither replacing the executable nor rolling it back deletes old datasets.

## Tests

Run `go test ./...` for the Go code and `node --test tests/map.test.cjs` for the map's loading behavior. The map tests use Node's built-in test runner and require no npm dependencies.

After generating feature data, run `node tests/map.bench.cjs` to measure polyline counts for sample Halifax views and synthetic report lookup costs. An optional argument selects a different features binary. These measurements use the Node map harness; they do not measure browser frame rates.
