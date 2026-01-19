package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

const (
	activeTravelwaysItemID = "a3631c7664ef4ecb93afb1ea4c12022b"
	bikeInfraItemID        = "460bba0983504ff9a3d74f144128b1ad"
	iceRoutesItemID        = "e9dd1561e22e4a149c5b45f54ec0942d"

	defaultTravelwaysOut = "features.bin"
	defaultBikeOut       = "features_cycling.bin"
)

const (
	datasetTravelways uint8 = iota
	datasetBike
	datasetIce
)

func main() {
	ctx := context.Background()

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var travelwaysFile string
	var bikeFile string
	var iceFile string
	var travelwaysOut string
	var bikeOut string
	var maxMatchMeters float64
	var maxAngleDeg float64
	var maxOverallAngleDeg float64
	var priorityBiasMeters float64
	var debugOut string
	fs.StringVar(&travelwaysFile, "travelways", "", "path to travelways geojson file, otherwise download")
	fs.StringVar(&bikeFile, "bike", "", "path to bike infrastructure geojson file, otherwise download")
	fs.StringVar(&iceFile, "ice", "", "path to ice routes geojson file, otherwise download")
	fs.StringVar(&travelwaysOut, "out-travelways", defaultTravelwaysOut, "path to write travelways features bin")
	fs.StringVar(&bikeOut, "out-bike", defaultBikeOut, "path to write bike infrastructure features bin")
	fs.Float64Var(&maxMatchMeters, "max-match-meters", 30, "max distance in meters to match bike routes to travelways or ice routes")
	fs.Float64Var(&maxAngleDeg, "max-angle-deg", 30, "max angle delta in degrees for matching bike routes to other datasets")
	fs.Float64Var(&maxOverallAngleDeg, "max-overall-angle-deg", 60, "max angle delta in degrees between overall line directions")
	fs.Float64Var(&priorityBiasMeters, "priority-bias-meters", 1, "distance window to prefer higher priority matches over the nearest line")
	fs.StringVar(&debugOut, "debug-out", "", "path to write debug json with decision details")
	fs.Parse(os.Args[1:])

	travelwaysFC, err := loadFeatureCollection(ctx, travelwaysFile, activeTravelwaysItemID)
	if err != nil {
		log.Fatal(err)
	}
	bikeFC, err := loadFeatureCollection(ctx, bikeFile, bikeInfraItemID)
	if err != nil {
		log.Fatal(err)
	}
	iceFC, err := loadFeatureCollection(ctx, iceFile, iceRoutesItemID)
	if err != nil {
		log.Fatal(err)
	}

	noPlowTravelways := travelwayNoPlowLines(travelwaysFC)
	var noPlowIndex *spatialIndex
	if len(noPlowTravelways) > 0 {
		noPlowIndex, err = newSpatialIndex(noPlowTravelways, 48, 24)
		if err != nil {
			log.Fatal(err)
		}
	}

	var debugEntries []debugEntry
	titleNormalizer := newTitleNormalizer()
	seedTitleNormalizerFromTravelways(travelwaysFC, titleNormalizer)
	seedTitleNormalizerFromBike(bikeFC, titleNormalizer)
	travelwaysFeatures, err := travelwayLines(travelwaysFC, titleNormalizer, &debugEntries)
	if err != nil {
		log.Fatal(err)
	}

	travelwaysIndex, err := newSpatialIndex(linesForIndex(travelwaysFeatures), 48, 24)
	if err != nil {
		log.Fatal(err)
	}
	travelwayTitles := travelwayTitleMap(travelwaysFeatures)
	iceLines, err := iceRouteLines(iceFC)
	if err != nil {
		log.Fatal(err)
	}
	iceIndex, err := newSpatialIndex(iceLines, 48, 24)
	if err != nil {
		log.Fatal(err)
	}

	bikeFeatures, err := bikeLines(bikeFC, titleNormalizer, travelwaysIndex, travelwayTitles, noPlowIndex, iceIndex, maxMatchMeters, maxAngleDeg, maxOverallAngleDeg, priorityBiasMeters, &debugEntries)
	if err != nil {
		log.Fatal(err)
	}

	if err := writeFeaturesBin(travelwaysOut, travelwaysFeatures); err != nil {
		log.Fatal(err)
	}
	if err := writeFeaturesBin(bikeOut, bikeFeatures); err != nil {
		log.Fatal(err)
	}
	if debugOut != "" {
		cfg := debugConfig{
			MaxMatchMeters:     maxMatchMeters,
			MaxAngleDeg:        maxAngleDeg,
			MaxOverallAngleDeg: maxOverallAngleDeg,
			PriorityBiasMeters: priorityBiasMeters,
		}
		if err := writeDebug(debugOut, debugEntries, cfg); err != nil {
			log.Fatal(err)
		}
	}
}

type lineFeature struct {
	title         string
	priority      uint8
	coords        orb.LineString
	sourceDataset uint8
	objectID      int
}

type debugEntry struct {
	Dataset        string         `json:"dataset"`
	ObjectID       int            `json:"object_id"`
	Title          string         `json:"title,omitempty"`
	Included       bool           `json:"included"`
	Reason         string         `json:"reason"`
	Priority       uint8          `json:"priority,omitempty"`
	SourceDataset  string         `json:"source_dataset,omitempty"`
	MatchDistance  float64        `json:"match_distance,omitempty"`
	WintPlow       string         `json:"wint_plow,omitempty"`
	WintLOS        string         `json:"wint_los,omitempty"`
	BikeType       string         `json:"bike_type,omitempty"`
	ProtType       string         `json:"prot_type,omitempty"`
	BikeName       string         `json:"bike_name,omitempty"`
	StreetName     string         `json:"street_name,omitempty"`
	ProtectedBike  bool           `json:"protected_bike,omitempty"`
	Coords         orb.LineString `json:"coords,omitempty"`
	NoPlowObjectID int            `json:"no_plow_object_id,omitempty"`
	NoPlowDistance float64        `json:"no_plow_distance,omitempty"`
}

type debugConfig struct {
	MaxMatchMeters     float64 `json:"max_match_meters"`
	MaxAngleDeg        float64 `json:"max_angle_deg"`
	MaxOverallAngleDeg float64 `json:"max_overall_angle_deg"`
	PriorityBiasMeters float64 `json:"priority_bias_meters"`
}

func writeFeaturesBin(path string, features []lineFeature) error {
	var out bytes.Buffer
	if err := encodeFeatures(features, &out); err != nil {
		return err
	}
	if err := os.WriteFile(path, out.Bytes(), 0644); err != nil {
		return err
	}
	return nil
}

func writeDebug(path string, entries []debugEntry, cfg debugConfig) error {
	payload := struct {
		GeneratedAt time.Time    `json:"generated_at"`
		Config      debugConfig  `json:"config"`
		Entries     []debugEntry `json:"entries"`
	}{
		GeneratedAt: time.Now().UTC(),
		Config:      cfg,
		Entries:     entries,
	}
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

func appendDebug(entries *[]debugEntry, entry debugEntry) {
	if entries == nil {
		return
	}
	*entries = append(*entries, entry)
}

func datasetName(code uint8) string {
	switch code {
	case datasetTravelways:
		return "travelways"
	case datasetBike:
		return "bike"
	case datasetIce:
		return "ice"
	default:
		return "unknown"
	}
}

func travelwayLines(fc *geojson.FeatureCollection, titles *titleNormalizer, debug *[]debugEntry) ([]lineFeature, error) {
	features := make([]lineFeature, 0, len(fc.Features))
	skippedNoPlow := 0
	for _, f := range fc.Features {
		props := f.Properties
		objectID := props.MustInt("OBJECTID", 0)
		wintPlow := strings.TrimSpace(props.MustString("WINT_PLOW", ""))
		wintLOS := strings.TrimSpace(props.MustString("WINT_LOS", ""))
		owner := strings.TrimSpace(props.MustString("OWNER", ""))
		if isPrivateOwner(owner) {
			appendDebug(debug, debugEntry{
				Dataset:  "travelways",
				ObjectID: objectID,
				Title:    props.MustString("LOCATION", ""),
				Included: false,
				Reason:   "OWNER=PRIV",
				WintPlow: wintPlow,
				WintLOS:  wintLOS,
			})
			continue
		}
		if isNotPlowed(props) {
			skippedNoPlow++
			appendDebug(debug, debugEntry{
				Dataset:  "travelways",
				ObjectID: objectID,
				Title:    props.MustString("LOCATION", ""),
				Included: false,
				Reason:   "WINT_PLOW=N",
				WintPlow: wintPlow,
				WintLOS:  wintLOS,
			})
			continue
		}

		priority, ok := priorityFromWintLOS(wintLOS)
		if !ok {
			appendDebug(debug, debugEntry{
				Dataset:  "travelways",
				ObjectID: objectID,
				Title:    props.MustString("LOCATION", ""),
				Included: false,
				Reason:   "missing or invalid WINT_LOS",
				WintPlow: wintPlow,
				WintLOS:  wintLOS,
			})
			continue
		}

		ls, ok, err := flattenLineString(f.Geometry)
		if err != nil {
			appendDebug(debug, debugEntry{
				Dataset:  "travelways",
				ObjectID: objectID,
				Title:    props.MustString("LOCATION", ""),
				Included: false,
				Reason:   "invalid geometry",
				WintPlow: wintPlow,
				WintLOS:  wintLOS,
			})
			return nil, err
		}
		if !ok {
			appendDebug(debug, debugEntry{
				Dataset:  "travelways",
				ObjectID: objectID,
				Title:    props.MustString("LOCATION", ""),
				Included: false,
				Reason:   "empty geometry",
				WintPlow: wintPlow,
				WintLOS:  wintLOS,
			})
			continue
		}

		title := titles.normalize(props.MustString("LOCATION", ""))
		if strings.EqualFold(title, "CORNWALLIS ST") {
			title = titles.normalize("Nora Bernard St")
		}
		features = append(features, lineFeature{
			title:         title,
			priority:      priority,
			coords:        ls,
			sourceDataset: datasetTravelways,
			objectID:      objectID,
		})
		appendDebug(debug, debugEntry{
			Dataset:  "travelways",
			ObjectID: objectID,
			Title:    title,
			Included: true,
			Reason:   "included",
			Priority: priority,
			WintPlow: wintPlow,
			WintLOS:  wintLOS,
			Coords:   ls,
		})
	}

	if skippedNoPlow > 0 {
		log.Printf("travelways skipped not plowed=%d", skippedNoPlow)
	}
	return features, nil
}

func travelwayTitleMap(features []lineFeature) map[int]string {
	titles := make(map[int]string, len(features))
	for _, feature := range features {
		if feature.objectID == 0 || feature.title == "" {
			continue
		}
		titles[feature.objectID] = feature.title
	}
	return titles
}

func travelwayNoPlowLines(fc *geojson.FeatureCollection) []indexedLine {
	lines := make([]indexedLine, 0)
	for _, f := range fc.Features {
		props := f.Properties
		owner := strings.TrimSpace(props.MustString("OWNER", ""))
		if isPrivateOwner(owner) {
			continue
		}
		if !isNotPlowed(props) {
			continue
		}
		ls, ok, err := flattenLineString(f.Geometry)
		if err != nil || !ok {
			continue
		}
		objectID := props.MustInt("OBJECTID", 0)
		lines = append(lines, indexedLine{
			coords:   ls,
			priority: 1,
			objectID: objectID,
		})
	}
	return lines
}

func seedTitleNormalizerFromTravelways(fc *geojson.FeatureCollection, titles *titleNormalizer) {
	for _, f := range fc.Features {
		props := f.Properties
		titles.observe(props.MustString("LOCATION", ""))
	}
}

func seedTitleNormalizerFromBike(fc *geojson.FeatureCollection, titles *titleNormalizer) {
	for _, f := range fc.Features {
		props := f.Properties
		titles.observe(props.MustString("BIKE_NAME", ""))
		titles.observe(props.MustString("STREETNAME", ""))
	}
}

type titleNormalizer struct {
	bestByLower map[string]string
}

func newTitleNormalizer() *titleNormalizer {
	return &titleNormalizer{
		bestByLower: make(map[string]string),
	}
}

func (t *titleNormalizer) observe(value string) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return
	}
	key := strings.ToLower(raw)
	if best, ok := t.bestByLower[key]; ok {
		if isAllUpper(best) && !isAllUpper(raw) {
			t.bestByLower[key] = raw
		}
		return
	}
	t.bestByLower[key] = raw
}

func (t *titleNormalizer) normalize(value string) string {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return ""
	}
	key := strings.ToLower(raw)
	if best, ok := t.bestByLower[key]; ok {
		return best
	}
	return raw
}

func isAllUpper(value string) bool {
	hasLetter := false
	for _, r := range value {
		if unicode.IsLetter(r) {
			hasLetter = true
			if unicode.IsLower(r) {
				return false
			}
		}
	}
	return hasLetter
}

func bikeTitle(props geojson.Properties, titles *titleNormalizer) (string, bool) {
	title := titles.normalize(props.MustString("BIKE_NAME", ""))
	if title != "" {
		return title, false
	}
	title = titles.normalize(props.MustString("STREETNAME", ""))
	if title != "" {
		return title, false
	}
	title = titles.normalize(props.MustString("BIKETYPE", ""))
	return title, true
}

func bikeLines(fc *geojson.FeatureCollection, titles *titleNormalizer, travelwaysIndex *spatialIndex, travelwayTitles map[int]string, noPlowIndex, iceIndex *spatialIndex, maxMatchMeters, maxAngleDeg, maxOverallAngleDeg, priorityBiasMeters float64, debug *[]debugEntry) ([]lineFeature, error) {
	var (
		matchedTravelways int
		matchedIce        int
		matchedFallback   int
		skipped           int
		skippedNotPlowed  int
		skippedNoPlowTW   int
	)
	maxAngleRad := deg2rad(maxAngleDeg)
	maxOverallAngleRad := deg2rad(maxOverallAngleDeg)

	features := make([]lineFeature, 0, len(fc.Features))
	for _, f := range fc.Features {
		props := f.Properties
		objectID := props.MustInt("OBJECTID", 0)
		wintPlow := strings.TrimSpace(props.MustString("WINT_PLOW", ""))
		wintLOS := strings.TrimSpace(props.MustString("WINT_LOS", ""))
		if isNotPlowed(props) {
			skippedNotPlowed++
			appendDebug(debug, debugEntry{
				Dataset:    "bike",
				ObjectID:   objectID,
				Title:      props.MustString("BIKE_NAME", ""),
				Included:   false,
				Reason:     "WINT_PLOW=N",
				WintPlow:   wintPlow,
				WintLOS:    wintLOS,
				BikeType:   props.MustString("BIKETYPE", ""),
				ProtType:   props.MustString("PROT_TYPE", ""),
				BikeName:   props.MustString("BIKE_NAME", ""),
				StreetName: props.MustString("STREETNAME", ""),
			})
			continue
		}

		ls, ok, err := flattenLineString(f.Geometry)
		if err != nil {
			return nil, err
		}
		if !ok {
			appendDebug(debug, debugEntry{
				Dataset:    "bike",
				ObjectID:   objectID,
				Title:      props.MustString("BIKE_NAME", ""),
				Included:   false,
				Reason:     "empty geometry",
				WintPlow:   wintPlow,
				WintLOS:    wintLOS,
				BikeType:   props.MustString("BIKETYPE", ""),
				ProtType:   props.MustString("PROT_TYPE", ""),
				BikeName:   props.MustString("BIKE_NAME", ""),
				StreetName: props.MustString("STREETNAME", ""),
			})
			continue
		}

		title, titleFromType := bikeTitle(props, titles)
		var (
			priority      uint8
			matchDistance float64
			sourceDataset uint8
			travelwayID   int
			found         bool
			reason        string
		)
		if isProtectedBike(props) {
			var noPlowObjectID int
			var noPlowDistance float64
			if noPlowIndex != nil {
				match := noPlowIndex.nearestMatch(ls, maxMatchMeters, maxAngleRad, maxOverallAngleRad, 0)
				noPlowObjectID = match.objectID
				noPlowDistance = match.distance
			}
			if noPlowObjectID != 0 {
				skippedNoPlowTW++
				appendDebug(debug, debugEntry{
					Dataset:        "bike",
					ObjectID:       objectID,
					Title:          title,
					Included:       false,
					Reason:         "matched no-plow travelway",
					WintPlow:       wintPlow,
					WintLOS:        wintLOS,
					BikeType:       props.MustString("BIKETYPE", ""),
					ProtType:       props.MustString("PROT_TYPE", ""),
					BikeName:       props.MustString("BIKE_NAME", ""),
					StreetName:     props.MustString("STREETNAME", ""),
					ProtectedBike:  true,
					Coords:         ls,
					NoPlowObjectID: noPlowObjectID,
					NoPlowDistance: noPlowDistance,
				})
				continue
			}
			match := travelwaysIndex.nearestMatch(ls, maxMatchMeters, maxAngleRad, maxOverallAngleRad, priorityBiasMeters)
			priority = match.priority
			travelwayID = match.objectID
			matchDistance = match.distance
			found = match.hasMatch
			if found {
				sourceDataset = datasetTravelways
				reason = "matched travelways"
				matchedTravelways++
			}
		} else {
			match := iceIndex.nearestMatch(ls, maxMatchMeters, maxAngleRad, maxOverallAngleRad, priorityBiasMeters)
			priority = match.priority
			matchDistance = match.distance
			found = match.hasMatch
			if found {
				sourceDataset = datasetIce
				reason = "matched ice"
				matchedIce++
			}
		}
		if !found {
			if fallback, ok := priorityFromWintLOS(props.MustString("WINT_LOS", "")); ok {
				priority = fallback
				sourceDataset = datasetBike
				reason = "fallback WINT_LOS"
				matchedFallback++
				found = true
			}
		}
		if titleFromType && travelwaysIndex != nil {
			if travelwayID == 0 {
				match := travelwaysIndex.nearestMatch(ls, maxMatchMeters, maxAngleRad, maxOverallAngleRad, 0)
				travelwayID = match.objectID
			}
			if travelwayID != 0 {
				if travelwayTitle := travelwayTitles[travelwayID]; travelwayTitle != "" {
					title = travelwayTitle
				}
			}
		}
		if !found {
			skipped++
			appendDebug(debug, debugEntry{
				Dataset:       "bike",
				ObjectID:      objectID,
				Title:         title,
				Included:      false,
				Reason:        "no match",
				WintPlow:      wintPlow,
				WintLOS:       wintLOS,
				BikeType:      props.MustString("BIKETYPE", ""),
				ProtType:      props.MustString("PROT_TYPE", ""),
				BikeName:      props.MustString("BIKE_NAME", ""),
				StreetName:    props.MustString("STREETNAME", ""),
				ProtectedBike: isProtectedBike(props),
				Coords:        ls,
			})
			continue
		}

		features = append(features, lineFeature{
			title:         title,
			priority:      priority,
			coords:        ls,
			sourceDataset: sourceDataset,
		})
		appendDebug(debug, debugEntry{
			Dataset:       "bike",
			ObjectID:      objectID,
			Title:         title,
			Included:      true,
			Reason:        reason,
			Priority:      priority,
			WintPlow:      wintPlow,
			WintLOS:       wintLOS,
			BikeType:      props.MustString("BIKETYPE", ""),
			ProtType:      props.MustString("PROT_TYPE", ""),
			BikeName:      props.MustString("BIKE_NAME", ""),
			StreetName:    props.MustString("STREETNAME", ""),
			ProtectedBike: isProtectedBike(props),
			SourceDataset: datasetName(sourceDataset),
			MatchDistance: matchDistance,
			Coords:        ls,
		})
	}

	log.Printf("bike lines matched travelways=%d ice=%d fallback=%d skipped=%d", matchedTravelways, matchedIce, matchedFallback, skipped)
	if skippedNotPlowed > 0 {
		log.Printf("bike lines skipped not plowed=%d", skippedNotPlowed)
	}
	if skippedNoPlowTW > 0 {
		log.Printf("bike lines skipped due to no-plow travelways=%d", skippedNoPlowTW)
	}

	return features, nil
}

func isNotPlowed(props geojson.Properties) bool {
	return strings.EqualFold(strings.TrimSpace(props.MustString("WINT_PLOW", "")), "N")
}

func isPrivateOwner(owner string) bool {
	return strings.EqualFold(strings.TrimSpace(owner), "PRIV")
}

func isProtectedBike(props geojson.Properties) bool {
	protType := strings.TrimSpace(props.MustString("PROT_TYPE", ""))
	if protType != "" && !strings.EqualFold(protType, "NONE") {
		return true
	}
	switch strings.TrimSpace(props.MustString("BIKETYPE", "")) {
	case "PROTBL", "INT_PROTBL", "MUPATH":
		return true
	default:
		return false
	}
}

func priorityFromWintLOS(value string) (uint8, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	priorityNum := strings.TrimPrefix(value, "PRI")
	priority, err := strconv.Atoi(priorityNum)
	if err != nil || priority < 1 || priority > 3 {
		return 0, false
	}
	return uint8(priority), true
}

func iceRouteLines(fc *geojson.FeatureCollection) ([]indexedLine, error) {
	lines := make([]indexedLine, 0, len(fc.Features))
	for _, f := range fc.Features {
		props := f.Properties
		priorityStr := strings.TrimSpace(props.MustString("PRIORITY", ""))
		if priorityStr == "" {
			continue
		}
		priorityNum, err := strconv.Atoi(priorityStr)
		if err != nil || priorityNum < 1 || priorityNum > 3 {
			return nil, fmt.Errorf("invalid ice route priority: %q", priorityStr)
		}

		ls, ok, err := flattenLineString(f.Geometry)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		lines = append(lines, indexedLine{
			coords:   ls,
			priority: uint8(priorityNum),
		})
	}

	if len(lines) == 0 {
		return nil, fmt.Errorf("no ice route lines")
	}

	return lines, nil
}

func linesForIndex(features []lineFeature) []indexedLine {
	lines := make([]indexedLine, 0, len(features))
	for _, f := range features {
		lines = append(lines, indexedLine{
			coords:   f.coords,
			priority: f.priority,
			objectID: f.objectID,
		})
	}
	return lines
}

func flattenLineString(geom orb.Geometry) (orb.LineString, bool, error) {
	var ls orb.LineString
	switch g := geom.(type) {
	case orb.LineString:
		ls = g
	case orb.MultiLineString:
		for _, sub := range g {
			ls = append(ls, sub...)
		}
	default:
		return nil, false, fmt.Errorf("unknown geometry type: %T", g)
	}
	if len(ls) == 0 {
		return nil, false, nil
	}
	return ls, true, nil
}

func encodeFeatures(features []lineFeature, writer io.Writer) error {
	if len(features) == 0 {
		return fmt.Errorf("no features")
	}

	globalMinLon, globalMinLat := math.MaxFloat64, math.MaxFloat64
	globalMaxLon, globalMaxLat := -math.MaxFloat64, -math.MaxFloat64

	type featureForSeg struct {
		data           lineFeature
		repLon, repLat float64
	}
	var featuresForSeg []featureForSeg

	for _, feature := range features {
		ls := feature.coords
		if len(ls) == 0 {
			continue
		}

		for _, coord := range ls {
			if coord[0] < globalMinLon {
				globalMinLon = coord[0]
			}
			if coord[0] > globalMaxLon {
				globalMaxLon = coord[0]
			}
			if coord[1] < globalMinLat {
				globalMinLat = coord[1]
			}
			if coord[1] > globalMaxLat {
				globalMaxLat = coord[1]
			}
		}

		repLon, repLat := ls[0][0], ls[0][1]
		featuresForSeg = append(featuresForSeg, featureForSeg{
			data:   feature,
			repLon: repLon,
			repLat: repLat,
		})
	}

	const cols = 8
	const rows = 4

	type cellKey struct {
		row, col int
	}
	segmentsMap := make(map[cellKey][]lineFeature)
	for _, f := range featuresForSeg {
		var col int
		if globalMaxLon > globalMinLon {
			col = int((f.repLon - globalMinLon) / (globalMaxLon - globalMinLon) * cols)
		} else {
			col = 0
		}
		if col < 0 {
			col = 0
		}
		if col >= cols {
			col = cols - 1
		}

		var row int
		if globalMaxLat > globalMinLat {
			row = int((f.repLat - globalMinLat) / (globalMaxLat - globalMinLat) * rows)
		} else {
			row = 0
		}
		if row < 0 {
			row = 0
		}
		if row >= rows {
			row = rows - 1
		}

		key := cellKey{row: row, col: col}
		segmentsMap[key] = append(segmentsMap[key], f.data)
	}

	type segment struct {
		row, col int
		features []lineFeature
	}
	var segments []segment
	for row := range rows {
		for col := range cols {
			key := cellKey{row: row, col: col}
			if feats, ok := segmentsMap[key]; ok {
				segments = append(segments, segment{
					row:      row,
					col:      col,
					features: feats,
				})
			}
		}
	}

	if err := binary.Write(writer, binary.LittleEndian, uint32(len(segments))); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, globalMinLon); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, globalMinLat); err != nil {
		return err
	}

	for _, seg := range segments {
		segMinLon, segMinLat := math.MaxFloat64, math.MaxFloat64
		segMaxLon, segMaxLat := -math.MaxFloat64, -math.MaxFloat64
		for _, feature := range seg.features {
			for _, coord := range feature.coords {
				if coord[0] < segMinLon {
					segMinLon = coord[0]
				}
				if coord[0] > segMaxLon {
					segMaxLon = coord[0]
				}
				if coord[1] < segMinLat {
					segMinLat = coord[1]
				}
				if coord[1] > segMaxLat {
					segMaxLat = coord[1]
				}
			}
		}

		deltaMinLon := int32(math.Round((segMinLon - globalMinLon) * 1000000))
		deltaMinLat := int32(math.Round((segMinLat - globalMinLat) * 1000000))
		deltaMaxLon := int32(math.Round((segMaxLon - globalMinLon) * 1000000))
		deltaMaxLat := int32(math.Round((segMaxLat - globalMinLat) * 1000000))
		if err := binary.Write(writer, binary.LittleEndian, deltaMinLon); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, deltaMinLat); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, deltaMaxLon); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, deltaMaxLat); err != nil {
			return err
		}

		if err := binary.Write(writer, binary.LittleEndian, uint32(len(seg.features))); err != nil {
			return err
		}

		for _, f := range seg.features {
			titleBytes := []byte(f.title)
			if len(titleBytes) > 255 {
				return fmt.Errorf("title too long: %q exceeds 255 bytes", f.title)
			}
			if err := binary.Write(writer, binary.LittleEndian, uint8(len(titleBytes))); err != nil {
				return err
			}
			if _, err := writer.Write(titleBytes); err != nil {
				return err
			}
			if err := binary.Write(writer, binary.LittleEndian, f.priority); err != nil {
				return err
			}
			if err := binary.Write(writer, binary.LittleEndian, f.sourceDataset); err != nil {
				return err
			}

			if len(f.coords) > math.MaxUint16 {
				return fmt.Errorf("too many coordinates in feature: %d exceeds uint16 capacity", len(f.coords))
			}
			if err := binary.Write(writer, binary.LittleEndian, uint16(len(f.coords))); err != nil {
				return err
			}

			for _, coord := range f.coords {
				dLon := int32(math.Round((coord[0] - globalMinLon) * 1000000))
				dLat := int32(math.Round((coord[1] - globalMinLat) * 1000000))
				if err := binary.Write(writer, binary.LittleEndian, dLon); err != nil {
					return err
				}
				if err := binary.Write(writer, binary.LittleEndian, dLat); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func loadFeatureCollection(ctx context.Context, path, itemID string) (*geojson.FeatureCollection, error) {
	var data []byte
	if path == "" {
		rc, err := download(ctx, itemID)
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		b, err := io.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		data = b
	} else {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		data = b
	}

	fc := geojson.NewFeatureCollection()
	if err := json.Unmarshal(data, &fc); err != nil {
		return nil, err
	}
	return fc, nil
}

func download(ctx context.Context, itemID string) (io.ReadCloser, error) {
	downloadURL := fmt.Sprintf("https://hub.arcgis.com/api/download/v1/items/%s/geojson?redirect=false&layers=0&spatialRefId=4326", itemID)

	deadline := time.Now().Add(5 * time.Minute)

	var resultURL string
	for time.Now().Before(deadline) {
		u, err := func() (string, error) {
			req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
			if err != nil {
				return "", fmt.Errorf("creating request: %w", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return "", fmt.Errorf("executing request: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode/100 != 2 {
				return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}

			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return "", fmt.Errorf("reading body: %w", err)
			}

			var body struct {
				ResultURL string `json:"resultUrl"`
			}
			if err := json.Unmarshal(b, &body); err != nil {
				return "", fmt.Errorf("unmarshaling body: %w", err)
			}

			return body.ResultURL, nil
		}()
		if err != nil {
			return nil, fmt.Errorf("downloading data: %w", err)
		}
		if u == "" {
			log.Printf("waiting for %s export", itemID)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		resultURL = u
		break
	}

	if resultURL == "" {
		return nil, fmt.Errorf("timed out waiting for %s export", itemID)
	}

	log.Println("downloading from", resultURL)

	req, err := http.NewRequestWithContext(ctx, "GET", resultURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	return resp.Body, nil
}

type pointXY struct {
	x float64
	y float64
}

type indexedLine struct {
	coords          orb.LineString
	xy              []pointXY
	minLon          float64
	minLat          float64
	maxLon          float64
	maxLat          float64
	priority        uint8
	objectID        int
	overallAngleRad float64
	hasOverallAngle bool
}

type spatialIndex struct {
	lines     []indexedLine
	minLon    float64
	minLat    float64
	maxLon    float64
	maxLat    float64
	cols      int
	rows      int
	cells     map[cellKey][]int
	projector projector
}

type nearestMatchResult struct {
	priority uint8
	objectID int
	distance float64
	hasMatch bool
}

type cellKey struct {
	row, col int
}

type projector struct {
	lat0Rad float64
}

func (p projector) toXY(point orb.Point) pointXY {
	lonRad := deg2rad(point[0])
	latRad := deg2rad(point[1])
	return pointXY{
		x: lonRad * math.Cos(p.lat0Rad) * orb.EarthRadius,
		y: latRad * orb.EarthRadius,
	}
}

func (p projector) lineToXY(line orb.LineString) []pointXY {
	pts := make([]pointXY, 0, len(line))
	for _, pt := range line {
		pts = append(pts, p.toXY(pt))
	}
	return pts
}

func newSpatialIndex(lines []indexedLine, cols, rows int) (*spatialIndex, error) {
	if len(lines) == 0 {
		return nil, fmt.Errorf("no lines to index")
	}

	globalMinLon, globalMinLat := math.MaxFloat64, math.MaxFloat64
	globalMaxLon, globalMaxLat := -math.MaxFloat64, -math.MaxFloat64

	for i := range lines {
		minLon, minLat, maxLon, maxLat := lineBounds(lines[i].coords)
		lines[i].minLon = minLon
		lines[i].minLat = minLat
		lines[i].maxLon = maxLon
		lines[i].maxLat = maxLat

		if minLon < globalMinLon {
			globalMinLon = minLon
		}
		if maxLon > globalMaxLon {
			globalMaxLon = maxLon
		}
		if minLat < globalMinLat {
			globalMinLat = minLat
		}
		if maxLat > globalMaxLat {
			globalMaxLat = maxLat
		}
	}

	lat0 := (globalMinLat + globalMaxLat) / 2
	proj := projector{lat0Rad: deg2rad(lat0)}
	for i := range lines {
		lines[i].xy = proj.lineToXY(lines[i].coords)
		lines[i].overallAngleRad, lines[i].hasOverallAngle = overallLineAngle(lines[i].xy)
	}

	idx := &spatialIndex{
		lines:     lines,
		minLon:    globalMinLon,
		minLat:    globalMinLat,
		maxLon:    globalMaxLon,
		maxLat:    globalMaxLat,
		cols:      cols,
		rows:      rows,
		cells:     make(map[cellKey][]int),
		projector: proj,
	}

	cellWidth := (idx.maxLon - idx.minLon) / float64(cols)
	cellHeight := (idx.maxLat - idx.minLat) / float64(rows)

	for i, line := range idx.lines {
		colMin := int((line.minLon - idx.minLon) / cellWidth)
		colMax := int((line.maxLon - idx.minLon) / cellWidth)
		rowMin := int((line.minLat - idx.minLat) / cellHeight)
		rowMax := int((line.maxLat - idx.minLat) / cellHeight)

		if colMin < 0 {
			colMin = 0
		}
		if rowMin < 0 {
			rowMin = 0
		}
		if colMax >= cols {
			colMax = cols - 1
		}
		if rowMax >= rows {
			rowMax = rows - 1
		}

		for row := rowMin; row <= rowMax; row++ {
			for col := colMin; col <= colMax; col++ {
				key := cellKey{row: row, col: col}
				idx.cells[key] = append(idx.cells[key], i)
			}
		}
	}

	return idx, nil
}

func (idx *spatialIndex) nearestMatch(line orb.LineString, maxDistanceMeters, maxAngleRad, maxOverallAngleRad, priorityBiasMeters float64) nearestMatchResult {
	if len(line) == 0 {
		return nearestMatchResult{}
	}

	minLon, minLat, maxLon, maxLat := lineBounds(line)
	minLon -= metersToDegreesLon(maxDistanceMeters, idx.projector.lat0Rad)
	maxLon += metersToDegreesLon(maxDistanceMeters, idx.projector.lat0Rad)
	minLat -= metersToDegreesLat(maxDistanceMeters)
	maxLat += metersToDegreesLat(maxDistanceMeters)

	candidateIdxs := idx.candidates(minLon, minLat, maxLon, maxLat)
	if len(candidateIdxs) == 0 {
		return nearestMatchResult{}
	}

	lineXY := idx.projector.lineToXY(line)
	lineOverallAngle, hasLineOverall := overallLineAngle(lineXY)
	minDistance := math.Inf(1)
	for _, i := range candidateIdxs {
		candidate := idx.lines[i]
		if maxOverallAngleRad > 0 && hasLineOverall && candidate.hasOverallAngle {
			if angleDelta(lineOverallAngle, candidate.overallAngleRad) > maxOverallAngleRad {
				continue
			}
		}
		distance := minLineDistanceWithAngle(lineXY, candidate.xy, maxAngleRad)
		if distance < minDistance {
			minDistance = distance
		}
	}

	if minDistance > maxDistanceMeters {
		return nearestMatchResult{}
	}
	if priorityBiasMeters < 0 {
		priorityBiasMeters = 0
	}
	cutoff := minDistance + priorityBiasMeters

	bestDistance := math.Inf(1)
	bestPriority := uint8(math.MaxUint8)
	bestObjectID := 0
	for _, i := range candidateIdxs {
		candidate := idx.lines[i]
		if maxOverallAngleRad > 0 && hasLineOverall && candidate.hasOverallAngle {
			if angleDelta(lineOverallAngle, candidate.overallAngleRad) > maxOverallAngleRad {
				continue
			}
		}
		distance := minLineDistanceWithAngle(lineXY, candidate.xy, maxAngleRad)
		if distance > cutoff {
			continue
		}
		if candidate.priority < bestPriority || (candidate.priority == bestPriority && distance < bestDistance) {
			bestPriority = candidate.priority
			bestDistance = distance
			bestObjectID = candidate.objectID
		}
	}

	if bestDistance <= maxDistanceMeters {
		return nearestMatchResult{
			priority: bestPriority,
			objectID: bestObjectID,
			distance: bestDistance,
			hasMatch: true,
		}
	}
	return nearestMatchResult{}
}

func (idx *spatialIndex) candidates(minLon, minLat, maxLon, maxLat float64) []int {
	cellWidth := (idx.maxLon - idx.minLon) / float64(idx.cols)
	cellHeight := (idx.maxLat - idx.minLat) / float64(idx.rows)

	colMin := int((minLon - idx.minLon) / cellWidth)
	colMax := int((maxLon - idx.minLon) / cellWidth)
	rowMin := int((minLat - idx.minLat) / cellHeight)
	rowMax := int((maxLat - idx.minLat) / cellHeight)

	if colMin < 0 {
		colMin = 0
	}
	if rowMin < 0 {
		rowMin = 0
	}
	if colMax >= idx.cols {
		colMax = idx.cols - 1
	}
	if rowMax >= idx.rows {
		rowMax = idx.rows - 1
	}

	seen := make(map[int]struct{})
	var out []int
	for row := rowMin; row <= rowMax; row++ {
		for col := colMin; col <= colMax; col++ {
			key := cellKey{row: row, col: col}
			for _, idx := range idx.cells[key] {
				if _, ok := seen[idx]; ok {
					continue
				}
				seen[idx] = struct{}{}
				out = append(out, idx)
			}
		}
	}
	return out
}

func minLineDistance(a, b []pointXY) float64 {
	if len(a) == 0 || len(b) == 0 {
		return math.Inf(1)
	}
	if len(a) == 1 && len(b) == 1 {
		return distancePoint(a[0], b[0])
	}
	if len(a) == 1 {
		return minPointLineDistance(a[0], b)
	}
	if len(b) == 1 {
		return minPointLineDistance(b[0], a)
	}

	minDist := math.Inf(1)
	for i := 0; i < len(a)-1; i++ {
		for j := 0; j < len(b)-1; j++ {
			d := segmentDistance(a[i], a[i+1], b[j], b[j+1])
			if d < minDist {
				minDist = d
			}
		}
	}
	return minDist
}

func minLineDistanceWithAngle(a, b []pointXY, maxAngleRad float64) float64 {
	if maxAngleRad <= 0 {
		return minLineDistance(a, b)
	}
	if len(a) < 2 || len(b) < 2 {
		return minLineDistance(a, b)
	}

	minDist := math.Inf(1)
	for i := 0; i < len(a)-1; i++ {
		angleA, okA := segmentAngle(a[i], a[i+1])
		if !okA {
			continue
		}
		for j := 0; j < len(b)-1; j++ {
			angleB, okB := segmentAngle(b[j], b[j+1])
			if !okB {
				continue
			}
			if angleDelta(angleA, angleB) > maxAngleRad {
				continue
			}
			d := segmentDistance(a[i], a[i+1], b[j], b[j+1])
			if d < minDist {
				minDist = d
			}
		}
	}
	return minDist
}

func minPointLineDistance(point pointXY, line []pointXY) float64 {
	minDist := math.Inf(1)
	for i := 0; i < len(line)-1; i++ {
		d := pointSegmentDistance(point, line[i], line[i+1])
		if d < minDist {
			minDist = d
		}
	}
	return minDist
}

func segmentDistance(a1, a2, b1, b2 pointXY) float64 {
	if segmentsIntersect(a1, a2, b1, b2) {
		return 0
	}
	return math.Min(
		math.Min(pointSegmentDistance(a1, b1, b2), pointSegmentDistance(a2, b1, b2)),
		math.Min(pointSegmentDistance(b1, a1, a2), pointSegmentDistance(b2, a1, a2)),
	)
}

func pointSegmentDistance(p, a, b pointXY) float64 {
	vx := b.x - a.x
	vy := b.y - a.y
	wx := p.x - a.x
	wy := p.y - a.y

	c1 := vx*wx + vy*wy
	if c1 <= 0 {
		return distancePoint(p, a)
	}
	c2 := vx*vx + vy*vy
	if c2 <= c1 {
		return distancePoint(p, b)
	}
	t := c1 / c2
	proj := pointXY{x: a.x + t*vx, y: a.y + t*vy}
	return distancePoint(p, proj)
}

func segmentsIntersect(a1, a2, b1, b2 pointXY) bool {
	o1 := orientation(a1, a2, b1)
	o2 := orientation(a1, a2, b2)
	o3 := orientation(b1, b2, a1)
	o4 := orientation(b1, b2, a2)

	if o1*o2 < 0 && o3*o4 < 0 {
		return true
	}

	if o1 == 0 && onSegment(a1, b1, a2) {
		return true
	}
	if o2 == 0 && onSegment(a1, b2, a2) {
		return true
	}
	if o3 == 0 && onSegment(b1, a1, b2) {
		return true
	}
	if o4 == 0 && onSegment(b1, a2, b2) {
		return true
	}

	return false
}

func orientation(a, b, c pointXY) float64 {
	return (b.y-a.y)*(c.x-b.x) - (b.x-a.x)*(c.y-b.y)
}

func onSegment(a, b, c pointXY) bool {
	return b.x >= math.Min(a.x, c.x) && b.x <= math.Max(a.x, c.x) &&
		b.y >= math.Min(a.y, c.y) && b.y <= math.Max(a.y, c.y)
}

func distancePoint(a, b pointXY) float64 {
	dx := a.x - b.x
	dy := a.y - b.y
	return math.Sqrt(dx*dx + dy*dy)
}

func segmentAngle(a, b pointXY) (float64, bool) {
	dx := b.x - a.x
	dy := b.y - a.y
	if dx == 0 && dy == 0 {
		return 0, false
	}
	return math.Atan2(dy, dx), true
}

func overallLineAngle(points []pointXY) (float64, bool) {
	if len(points) < 2 {
		return 0, false
	}
	start := points[0]
	end := points[len(points)-1]
	if start == end {
		for i := len(points) - 2; i >= 0; i-- {
			if points[i] != start {
				end = points[i]
				break
			}
		}
	}
	return segmentAngle(start, end)
}

func angleDelta(a, b float64) float64 {
	diff := math.Mod(math.Abs(a-b), 2*math.Pi)
	if diff > math.Pi {
		diff = 2*math.Pi - diff
	}
	alt := math.Abs(diff - math.Pi)
	if alt < diff {
		return alt
	}
	return diff
}

func lineBounds(line orb.LineString) (minLon, minLat, maxLon, maxLat float64) {
	minLon, minLat = math.MaxFloat64, math.MaxFloat64
	maxLon, maxLat = -math.MaxFloat64, -math.MaxFloat64
	for _, coord := range line {
		if coord[0] < minLon {
			minLon = coord[0]
		}
		if coord[0] > maxLon {
			maxLon = coord[0]
		}
		if coord[1] < minLat {
			minLat = coord[1]
		}
		if coord[1] > maxLat {
			maxLat = coord[1]
		}
	}
	return minLon, minLat, maxLon, maxLat
}

func metersToDegreesLat(meters float64) float64 {
	return rad2deg(meters / orb.EarthRadius)
}

func metersToDegreesLon(meters float64, lat0Rad float64) float64 {
	return rad2deg(meters / (orb.EarthRadius * math.Cos(lat0Rad)))
}

func deg2rad(deg float64) float64 {
	return deg * math.Pi / 180
}

func rad2deg(rad float64) float64 {
	return rad * 180 / math.Pi
}
