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

`cmd/features` builds the sidewalk and cycling map data from Halifax's [Active Travelways](https://data-hrm.hub.arcgis.com/datasets/a3631c7664ef4ecb93afb1ea4c12022b_0/explore), [Bike Infrastructure and Suggested Routes](https://data-hrm.hub.arcgis.com/datasets/HRM::bike-infrastructure-and-suggested-routes/explore), and [Ice Routes](https://data-hrm.hub.arcgis.com/datasets/HRM::ice-routes/explore) datasets. Cycling priorities combine winter-maintenance fields with matches to nearby travelways and ice routes.

`cmd/events` runs after `scraperlite` against the same database. It turns changes to service updates and weather event end times into the [events table](https://hrm.datasette.danp.net/snow/events).

`cmd/api` serves the map data, weather events, and community condition reports. It also ingests Environment Canada weather data to estimate surface conditions.

Reports auto-expire after 3 days, and all active reports are expired when a new weather event id is observed.

The frontend is hosted on GitHub Pages using the included Actions workflow and refreshes event data and reports while the map is open.

## Build and deploy the API

Requires Go 1.27 or later. Build the API with fresh map data:

```sh
./scripts/build-api.sh
```

The script downloads the municipal datasets and bundles the generated map data in `api`. To build for a Linux amd64 server:

```sh
GOOS=linux GOARCH=amd64 ./scripts/build-api.sh
```

Use `GOARCH=arm64` for an ARM64 server. Set `SNOWHFX_API_OUT` to choose another output path. To use local datasets, pass the generator's `-travelways`, `-bike`, and `-ice` flags.

Copy `api` to your server and restart the service. To run it directly:

```sh
./api -db snow.db
```

Map files are stored in `$XDG_DATA_HOME/snowhfx/features`, or `~/.local/share/snowhfx/features` when `XDG_DATA_HOME` is unset. Use `-features-dir` to override this. Keep the directory across deployments so older map versions remain available. Rebuild and redeploy the API to update the map data.

## Tests

```sh
go test ./...
node --test tests/map.test.cjs
```
