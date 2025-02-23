# snowhfx

[View the map](https://snowhfx.danp.net/)

A better Halifax sidewalk snow clearing map.

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

`cmd/features` downloads the [Active Travelways](https://data-hrm.hub.arcgis.com/datasets/a3631c7664ef4ecb93afb1ea4c12022b_0/explore) dataset and builds `features.bin` from it.
`features.bin` encodes:

* lines for each travelway (sidewalk, path, etc)
* their titles
* their snow clearing priority (1/2/3)

`cmd/events` is run after `scraperlite` against the same database to produce the `events` table, visible [here](https://hrm.datasette.danp.net/snow/events).
It scans through all observed changes and builds a log of notable changes to the service update and weather event end times.
From those it derives states.

`index.html` loads `features.bin` for lines to show on the map and queries [this condensed events data](https://hrm.datasette.danp.net/snow/snowhfx) for the current weather event state and last weather event end time.

The site runs on GitHub Pages, served from the [`pages`](https://github.com/danp/snowhfx/tree/pages) branch.
