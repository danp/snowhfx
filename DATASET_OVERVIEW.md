# Dataset Overview for Improved `cmd/features`

Goal: combine three HRM datasets (Active Travelways, Bike Infrastructure, Ice Routes) to produce sidewalk and bike route data with accurate winter clearing priority. This doc summarizes what the local GeoJSON contains, what the metadata says those fields mean, and what that implies for a new/better `cmd/features`.

Sources
- Local GeoJSON: `data/travelways.geojson`, `data/bike.geojson`, `data/ice.geojson` (as of this workspace).
- Metadata PDFs: `data/ODMT_Active_Travelways.pdf`, `data/ODMT_Bike_Infrastructure_and_Suggested_Routes.pdf`, `data/ODMT_Ice_Routes.pdf`.
- Current logic reference: `cmd/features/main.go`.

---

## 1) Active Travelways (sidewalks, trails, pathways)

Dataset intent (metadata)
- Linear assets including sidewalks, walkways, pathways, multi-use paths, trails.
- Work in progress; quality/completeness still improving.
- Winter maintenance fields describe plow responsibility and level of service.

Observed GeoJSON (local)
- Feature count: 49,653
- Geometry types: LineString (49,636), MultiLineString (17)
- Common/missing winter fields (counts from local file):
  - `WINT_PLOW`: missing 34,572; Y 11,430; N 3,651
  - `WINT_LOS`: missing 38,696; PRI1 4,926; PRI2 1,309; PRI3 4,722
  - `WINT_MAINT`: missing 34,746; common values: SWZ6, WSZ1, HRM2, WSZ4, SWZ8, NA
  - `WINT_ROUTE`: missing 45,567; common values: RB, RA, R2, R1, R4, R3
  - `OWNER`: common values: NA (31,571), HRM (16,937), PROV (554), PRIV (235)
  - `LOCATION`: missing 19,191; common values include PUBLIC GARDENS, ROBIE ST, HERRING COVE RD

Key metadata fields (from PDF)
- `OWNER` domain (AAA_asset_owner): HRM, PROV, PRIV, NA, UN, etc.
- `WINT_PLOW` domain (AAA_yes_no): Y/N.
- `WINT_MAINT` domain (AST_sidewalk_plow_zone): HRM1/HRM2, SWZ1..SWZ11, WSZ1..WSZ4, W1/W2/W3, E1/E2, PB1/PB4, BLDG, PARKS, NA.
- `WINT_ROUTE` domain (AST_sidewalk_plow_route): R1..R6, RA, RB, RP, PUR, BLR, GR, YR, ORR, BRR, BUR, RER, PE, PW.
- `WINT_LOS` domain (AST_sidewalk_plow_level): PRI1, PRI2, PRI3, PRI1TRAN, TRAN, TRANRESD.
  - Note: the local GeoJSON only uses PRI1/PRI2/PRI3 (no PRI1TRAN/TRAN/TRANRESD seen).
- Local-domain coverage notes (from GeoJSON):
  - `OWNER` uses a subset of domain codes: CNDO, DND, FED, HRM, HRSB, HW, NA, NSPI, NTO, PRIV, PROV, TPA, UN.
  - `WINT_MAINT` uses a subset of domain codes: BLDG, E2, HRM1/HRM2, PARKS, SWZ3/4/5/6/7/8/10, W1, WSZ1/2/3/4, NA.
  - `WINT_ROUTE` uses a subset of domain codes, and also includes codes not listed in metadata: BLR, R1..R6, RA, RB, RP plus PBL and R4W.

Implications for a better pipeline
- A very large share of travelways lack `WINT_LOS` even though `WINT_MAINT`/`WINT_ROUTE` might be present.
- `WINT_LOS` has additional values (PRI1TRAN, TRAN, TRANRESD) not present in the local file; current parsing would reject them if they show up in future refreshes.
- `OWNER=PRIV` is explicitly private; metadata also uses NA/UN for unknown/not-applicable.

---

## 2) Bike Infrastructure and Suggested Routes

Dataset intent (metadata)
- Existing and suggested bike routes/infrastructure.
- Winter maintenance fields apply to protected bike lanes only.
- Used for bike planning; includes both on-street and off-street facilities.

Observed GeoJSON (local)
- Feature count: 935
- Geometry types: LineString (918), MultiLineString (17)
- Common/missing winter fields (counts from local file):
  - `WINT_PLOW`: missing 886; Y 41; N 8
  - `WINT_LOS`: missing 899; PRI1 27; PRI2 9
  - `WINT_MAINT`: missing 894; common values: HRM1, W2, W1, NA
  - `WINT_ROUTE`: missing 902; common values: R2, R4, R1
- Core classification fields:
  - `BIKETYPE` (always present)
  - `PROT_TYPE` (missing 211; common values: NONE, OFFSTREET, RAISED, CURB, BOLLARD)
  - `BIKE_NAME` missing 573; `STREETNAME` missing 334
  - Name presence (for reporting): 842 features have `BIKE_NAME` or `STREETNAME`, 93 have neither.
  - Protected counts: `BIKETYPE` PROTBL/INT_PROTBL = 44; `PROT_TYPE` not NONE = 222

Key metadata fields (from PDF)
- `BIKETYPE` (TRN_bike_infra_type) includes:
  - On-street or suggested routes: BRLOCALRD, BRMAINRD, LOCALSTBW, PAINTEDBL, PAVESHLDR, SIGNBR, SCDAYLP
  - Protected: PROTBL, INT_PROTBL
  - Off-street / path: MUPATH, INT_MUPATH
  - Other: INT_QUIET, HELPCONN, UNCLASS
- `PROT_TYPE` (TRN_prot_type): CURB, BOLLARD, RAISED, OFFSTREET, OTHER, NONE.
- Winter maintenance fields (`WINT_PLOW`, `WINT_MAINT`, `WINT_ROUTE`, `WINT_LOS`, `WINT_MAT`) are explicitly for protected bike lanes.
  - Local-domain coverage notes (from GeoJSON):
    - `BIKETYPE` uses a subset of domain values: BRLOCALRD, BRMAINRD, HELPCONN, INT_PROTBL, INT_QUIET, LOCALSTBW, MUPATH, PAINTEDBL, PAVESHLDR, PROTBL, SCDAYLP. (No SIGNBR, UNCLASS, INT_MUPATH seen.)
    - `PROT_TYPE` uses all domain values.
    - `WINT_ROUTE` uses a small subset: BLR, R1..R4, RP.

Implications for a better pipeline
- Most bike features have no winter fields; relying on bike `WINT_LOS` gives sparse coverage.
- Use `BIKETYPE` + `PROT_TYPE` to decide how to source clearing priority:
  - Protected lanes should align with adjacent sidewalks/travelways if winter maintenance is on sidewalks.
  - On-street bikeways should align with ice routes (street plow priorities).
  - Some MUPATH / INT_MUPATH may overlap with travelways and could be considered sidewalk-like.
- Bike names and street names are often missing; title normalization is still needed.

---

## 3) Ice Routes (street plow routes)

Dataset intent (metadata)
- Snow and ice clearance routes; includes treatment material and priority.
- Used for winter maintenance management and maps.

Observed GeoJSON (local)
- Feature count: 13,025
- Geometry types: LineString (13,017), MultiLineString (8)
- Priority:
  - `PRIORITY` missing 885; values present: 1 (7,099), 2 (5,041)
- Other fields in local file:
  - `ROUTE_NAME` (many set)
  - `OPERATOR` is present in the data (not called out in PDF as a field)

Key metadata fields (from PDF)
- `PRIORITY` (RTE_ice_route_priority): 1 or 2.
- `MATERIAL` (RTE_material): SALT/SAND.
- `ROUTE_NAME` exists.
  - Local-domain coverage notes (from GeoJSON):
    - `PRIORITY` values present are only 1 and 2.
    - `MATERIAL` values present are only SALT and SAND.

Implications for a better pipeline
- Ice priority is only 1–2 per metadata; allowing 3 may be unnecessary.
- Ice routes provide the most complete priority information for on-street facilities.

---

## 4) Current `cmd/features` behavior (baseline)

Travelways
- Drops `OWNER=PRIV`.
- Drops `WINT_PLOW=N`.
- Drops if `WINT_LOS` missing/invalid (only accepts PRI1–PRI3).
- Produces line features with `priority` = WINT_LOS (1–3), source `travelways`.

Bike
- Drops only `WINT_PLOW=N`.
- Protected bike: match to travelways (nearest within distance/angle), then uses travelway priority.
  - If nearest is a **no-plow** travelway (WINT_PLOW=N) and it is closer than any plowed travelway, the bike feature is skipped.
- Unprotected bike: match to ice routes and use ice priority.
- If no match, fallback to bike `WINT_LOS` (PRI1–PRI3 only).

Ice
- Expects priority 1–3, and errors if outside 1–3.

---

## 5) Gaps and opportunities for “more accurate” priority

Travelways coverage gaps
- ~78% of travelways in the local file have no `WINT_LOS`.
- `WINT_LOS` domain includes additional values (PRI1TRAN, TRAN, TRANRESD) not present in the local file but rejected today if they appear later.
- Some fields like `WINT_MAINT`/`WINT_ROUTE` may be present even when `WINT_LOS` is missing.

Bike coverage gaps
- Bike `WINT_LOS` is missing for ~96% of bike features.
- Metadata clarifies winter fields apply to protected lanes only, so using WINT_LOS broadly for unprotected routes is likely wrong.

Ice route constraints
- Priority values are only 1–2 per metadata.
- Ice routes should be the source of on-street winter priority (street plowing).

---

## 6) Useful field inventory (what to expect)

Active Travelways (expected fields used in current code)
- `OBJECTID` (int)
- `WINT_PLOW` (Y/N)
- `WINT_LOS` (domain includes PRI1/PRI2/PRI3/PRI1TRAN/TRAN/TRANRESD; local file only has PRI1/PRI2/PRI3)
- `WINT_MAINT` (zone; see domain)
- `WINT_ROUTE` (route code; see domain)
- `OWNER` (HRM/PROV/PRIV/NA/UN/etc)
- `LOCATION` (string name)

Bike Infrastructure (expected fields used in current code)
- `OBJECTID`
- `BIKETYPE`
- `PROT_TYPE`
- `BIKE_NAME`
- `STREETNAME`
- `WINT_PLOW`, `WINT_LOS`, `WINT_MAINT`, `WINT_ROUTE`

Ice Routes (expected fields used in current code)
- `OBJECTID`
- `PRIORITY` (1/2 per metadata)
- `ROUTE_NAME`
- `OPERATOR` (present in data, not explicitly mentioned in PDF)

---

## 7) Strategy notes for a new/better `cmd/features`

1) Expand LOS parsing (future-proofing)
- Optionally accept PRI1TRAN/TRAN/TRANRESD in travelways and map to priorities (TBD mapping decision) if they appear in a future data refresh.

2) Reduce travelway drops
- If `WINT_LOS` missing but `WINT_MAINT`/`WINT_ROUTE` exists, consider deriving priority or marking as “unknown but plowed”.
- Treat `OWNER=PRIV` as exclude; consider how to treat `OWNER=NA` and `OWNER=UN` (unknown/not taken over) depending on desired coverage.

3) Bike route logic by facility type
- Protected lanes: prefer travelways (sidewalk-level priority). Use proximity and angle constraints as today.
- On-street bikeways: prefer ice routes (street-level priority). Use proximity and angle constraints.
- MUPATH / INT_MUPATH / OFFSTREET: likely travelways or self-contained; use travelways match first, then ice as fallback.

4) Priority blending
- For bike features that intersect both travelways and ice routes, track both priorities for debugging and decide tie-break rules.

5) Metadata consistency checks
- Keep ice priority validation to 1–2 per metadata; log if 3 is encountered to catch anomalies.

6) Matching heuristics (overlap-first + angle guards)
- Use **overlap attribution** as the primary decision rule.
- Compute overlap length between bike segments and nearby ice/travelways segments (distance + angle tolerance), then select the dominant priority by total overlap.
- Keep segment-angle and overall-line-angle filters to avoid incorrect cross-street matches when segments don’t align cleanly.

7) Source precedence rules (avoid wrong priority source)
- Protected bike facilities should prefer **travelways** priority even if an ice route is closer (see Stuart Graham Ave).
- On-street facilities (BRMAINRD/BRLOCALRD/PAINTEDBL/LOCALSTBW/INT_QUIET) should prefer **ice routes** even if a travelway is nearer.
- HELPCONN can overlap with travelways; decide source by matching quality (nearest/overlap) rather than a fixed rule (see bike `OBJECTID` 812 example).
- For MUPATH/INT_MUPATH/OFFSTREET, default to travelways first, then ice as a fallback if travelways are missing.

8) Naming/alias handling (reporting correctness)
- Bike street names can be misnamed vs travelways (e.g., “Stuart Graham Ave” vs “STUART GRAHAM DR”).
  Consider a small alias table for known mismatches to improve reporting and title inheritance.
  This is orthogonal to spatial matching and should not change priority decisions.

9) Overlap-based attribution (primary decision rule)
- Compute **overlap length** between each bike segment and nearby ice/travelways segments (within distance + angle tolerance).
- Aggregate overlap length per priority and either:
  - Choose the dominant priority by total overlap length (single datapoint for rendering), or
  - Split the bike feature into a few priority runs when overlap length changes materially.
- Use overlap attribution for HELPCONN and any segment with mixed candidates.

10) Display eligibility (travelways map + 311 reporting)
- Show only segments that are confidently cleared, have a known priority, and a usable name.
- Proposed filters:
  - `WINT_PLOW=Y` (explicitly cleared).
  - Valid `WINT_LOS` (PRI1–PRI3), unless a trusted fallback is defined.
  - `LOCATION` present (or a high‑confidence alias).
  - Exclude `OWNER=PRIV`.
- This is stricter than current `cmd/features` behavior and should reduce uncertain reporting.
  - In the current data, adding `LOCATION` reduces included travelways by ~210 (10,926 → 10,716).

11) Display eligibility (bike routes map + 311 reporting)
- Show only segments that have a name and an inferred priority from the correct source (travelways for protected/off‑street, ice routes for on‑street).
- Proposed filters:
  - Name present (`BIKE_NAME` or `STREETNAME`), or a high‑confidence alias.
  - Priority successfully assigned via matching (or overlap attribution) to travelways/ice routes.
  - Exclude `WINT_PLOW=N` when present; treat missing `WINT_PLOW` as unknown and rely on matched source instead.
- In the current data, 842 of 935 bike features have a name; 93 would be excluded on name alone.
  - Name fallback: if bike name is missing, inherit the **dominant overlapping travelway `LOCATION`** when overlap attribution is available (e.g., bike `OBJECTID` 812 overlaps travelway `OBJECTID` 2400 with a clear `LOCATION`).

---

## 8) Worked example (bike feature → priority source)

Example feature from `data/bike.geojson` (on-street bikeway)
- `OBJECTID`: 2
- `BIKETYPE`: BRLOCALRD (Suggested Bike Route on Local Road)
- `PROT_TYPE`: NONE
- `STREETNAME`: Bellbrook Cr
- Winter fields: `WINT_PLOW`/`WINT_LOS`/`WINT_MAINT`/`WINT_ROUTE` are all missing
- Geometry: LineString with 61 coordinates (non-trivial length)

Priority-source decision
- Because this is an on-street bikeway (`BIKETYPE=BRLOCALRD`, `PROT_TYPE=NONE`), its winter clearing priority should come from the **Ice Routes** dataset.
- There is no usable winter data on the bike feature itself, so any priority must be inferred by spatial matching.

Notable implication for matching
- On-street bike segments often **won’t align 1:1** with ice route segments; they can be longer, shorter, or segmented differently.
  For matching, this implies:
  - Use a distance tolerance rather than expecting exact overlap.
  - Compare segment angles and overall line direction to reduce incorrect matches.
  - Allow partial overlaps (e.g., nearest-match within tolerance) and track match distance for diagnostics.

Worked matching result (current heuristics: 30m distance, 30° segment angle, 60° overall angle)
- Found 5 ice route candidates within tolerance; the nearest matches are at 0.0m distance.
- Top matches all agree on priority and route info:
  - Example: Ice `OBJECTID` 1158, `PRIORITY` 1, `ROUTE_NAME` WSZ1, `OPERATOR` WSZ1, `MATERIAL` SALT (distance 0.0m).
  - Additional 0.0m matches: `OBJECTID` 1777, 11370, 7833, 391 (same priority/route/operator/material).
- Conclusion for this bike feature: assign priority **1** from ice routes, with WSZ1 as the inferred route name.

Chain Lake Dr example (on-street bikeways with many overlapping ice routes)
- Found 10 bike features with `STREETNAME=Chain Lake Dr` (BRMAINRD and PAINTEDBL). All are `PROT_TYPE=NONE` with no winter fields.
- All 10 features matched to ice routes with priority **1** and route `WSZ4` under current heuristics.
  - Distances to best match range from ~0.02m to ~5.49m.
- One segment (`OBJECTID` 66) had both priority 1 and priority 2 ice candidates within 30m.
  Under overlap-first selection, priority 1 still dominates, aligning with the expected outcome.
- Conclusion: even with multiple overlapping ice segments, the algorithm yields priority 1 across Chain Lake Dr, aligning with the expected outcome.

Stuart Graham Ave example (misnamed bike street vs travelways)
- Bike data uses `STREETNAME=Stuart Graham Ave`, but the travelways dataset has `LOCATION=STUART GRAHAM DR`.
  This indicates a naming mismatch that title normalization alone won’t fix unless we add a manual alias table.
- Two bike features in the data:
  - `OBJECTID` 514: `BIKETYPE=INT_QUIET`, `PROT_TYPE=NONE` (on-street style).
    - Ice candidates within 30m include `OBJECTID` 3115 at **0.0m** with priority **2** (route W2-3-4X4) and `OBJECTID` 6331 at ~0.02m with priority **1** (route W2-3).
    - Overlap-first selection should choose priority **2** based on the exact overlap with `OBJECTID` 3115.
  - `OBJECTID` 654: `BIKETYPE=PROTBL`, `PROT_TYPE=RAISED` (protected). Ice match gives priority **2** at ~1.24m, but travelways match gives priority **1** at ~5.54m with `LOCATION=STUART GRAHAM DR`.
    This illustrates why protected facilities should prefer travelways even if an ice route is closer.
- Development note: consider a small alias map for known street-name mismatches (Ave vs Dr) so bike titles can inherit travelway names for reporting.

Vernon St example (bike `OBJECTID` 110; multiple ice priorities)
- Bike feature: `BIKETYPE=LOCALSTBW`, `PROT_TYPE=NONE`, `STREETNAME=Vernon St` (length ~681m).
- Overlap-based attribution (5m distance, 30° angle) finds both priority 1 and priority 2 ice segments:
  - Priority 1 overlap ≈ 487m.
  - Priority 2 overlap ≈ 194m.
- Top ice features by overlap length all share route `W1-2` but include both priority 1 and 2 segments.
- Suggested rendering strategy:
  - If minimizing datapoints, assign **priority 1** as dominant (most overlap).
  - If higher fidelity is needed, split into two runs (priority 1 and priority 2) at the overlap transition.
- Minimal-segment guidance:
  - Generate per-segment priority using overlap attribution, then **collapse consecutive segments with the same priority** into runs.
  - Apply a minimum run-length threshold (e.g., 15–25m) to merge tiny runs into neighbors, keeping output small while still showing mixed priorities.
  - Optionally simplify each run’s geometry (small tolerance) to further reduce size without changing priority transitions.
  - Suggested defaults: minimum run length 20m; simplify tolerance 2–5m.

HELPCONN example (bike `OBJECTID` 812; overlaps both sources)
- Bike feature: `BIKETYPE=HELPCONN`, `PROT_TYPE` missing, no name fields (`BIKE_NAME`/`STREETNAME`).
- Ice candidates within 30m include multiple **0.0m** overlaps with priorities 1 and 2.
  Example: ice `OBJECTID` 6087 (priority 1) vs `OBJECTID` 1905/2019/7452/8158 (priority 2), all at 0.0m.
  Use overlap attribution (dominant overlap).
- Travelways candidates are also very close (e.g., `OBJECTID` 26754 with `WINT_LOS=PRI1` at ~0.21m), indicating the segment likely overlaps a travelway corridor.
  For HELPCONN, prefer overlap-based attribution or combined scoring rather than a hard source rule.

---

## 9) Travelways ambiguities (examples)

- Missing LOS but plowed/maintained:
  - `OBJECTID` 20: `WINT_PLOW=Y`, `WINT_LOS` missing, `WINT_MAINT=PARKS`, `OWNER=HRM`, `LOCATION` missing.
  - `OBJECTID` 49257: `WINT_PLOW=Y`, `WINT_LOS` missing, `WINT_MAINT=HRM1`, `WINT_ROUTE=R1`, `LOCATION=GRANVILLE ST, HALIFAX (WALKWAY FROM DUKE ST TO 1895 GRANVILLE ST)`.
  - These imply winter maintenance exists but priority is unspecified.
- Owner ambiguity:
  - `OBJECTID` 1977: `OWNER=NA`, `WINT_PLOW=Y`, `WINT_LOS=PRI1`, `WINT_MAINT=SWZ7`, `LOCATION` missing.
  - `OWNER=NA/UN` appears frequently; it’s unclear whether those should be treated as eligible for priority mapping.
- Private owner with LOS set:
  - `OBJECTID` 2075: `OWNER=PRIV`, `WINT_PLOW=N`, `WINT_LOS=PRI3`, `LOCATION=HORIZON CRT`.
  - Current logic excludes private owners, so LOS here is ignored.
- LOS set but no location:
  - `OBJECTID` 193: `WINT_PLOW=Y`, `WINT_LOS=PRI2`, `WINT_MAINT=SWZ8`, `LOCATION` missing.
  - Reporting and title normalization are harder without `LOCATION`.

---

## 10) Open questions to resolve

- If PRI1TRAN/TRAN/TRANRESD appear in a future data refresh, how should they map to priority levels?
- Should `OWNER=NA`/`UN` be treated as eligible or excluded?
- For MUPATH routes, should travelways always win over ice routes when both match?
- Should we keep a “confidence” or “source” flag for each priority assignment (travelways/ice/bike fallback/inferred)?

---

If you want, I can update this doc with concrete mapping proposals and a new decision tree for `cmd/features` once we settle the open questions.
