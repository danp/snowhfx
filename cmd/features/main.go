package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
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

	featuresBinMagic   = "SHFX"
	featuresBinVersion = uint8(4)
)

const (
	datasetTravelways uint8 = iota
	datasetBike
	datasetIce
)

func main() {
	ctx := context.Background()

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	cfg := runConfig{}
	fs.StringVar(&cfg.TravelwaysFile, "travelways", "", "path to travelways geojson file, otherwise download")
	fs.StringVar(&cfg.BikeFile, "bike", "", "path to bike infrastructure geojson file, otherwise download")
	fs.StringVar(&cfg.IceFile, "ice", "", "path to ice routes geojson file, otherwise download")
	fs.StringVar(&cfg.SaveDownloadsDir, "save-downloads-dir", "", "directory to save downloaded geojson files")
	fs.StringVar(&cfg.TravelwaysOut, "out-travelways", defaultTravelwaysOut, "path to write travelways features bin")
	fs.StringVar(&cfg.BikeOut, "out-bike", defaultBikeOut, "path to write bike infrastructure features bin")
	fs.Float64Var(&cfg.MaxMatchMeters, "max-match-meters", 30, "max distance in meters to match bike routes to travelways or ice routes")
	fs.Float64Var(&cfg.MaxAngleDeg, "max-angle-deg", 30, "max angle delta in degrees for matching bike routes to other datasets")
	fs.Float64Var(&cfg.MinRunMeters, "min-run-meters", 20, "min run length in meters when collapsing bike priority segments")
	fs.Float64Var(&cfg.SimplifyMeters, "simplify-meters", 2, "simplify line geometry (Douglas-Peucker) in meters; 0 disables")
	fs.StringVar(&cfg.DebugOut, "debug-out", "", "path to write debug json with decision details")
	fs.Parse(os.Args[1:])

	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

type runConfig struct {
	TravelwaysFile   string
	BikeFile         string
	IceFile          string
	SaveDownloadsDir string
	TravelwaysOut    string
	BikeOut          string
	MaxMatchMeters   float64
	MaxAngleDeg      float64
	MinRunMeters     float64
	SimplifyMeters   float64
	DebugOut         string
}

func run(ctx context.Context, cfg runConfig) error {
	travelwaysFC, err := loadFeatureCollection(ctx, cfg.TravelwaysFile, cfg.SaveDownloadsDir, "travelways.geojson", activeTravelwaysItemID)
	if err != nil {
		return err
	}
	bikeFC, err := loadFeatureCollection(ctx, cfg.BikeFile, cfg.SaveDownloadsDir, "bike.geojson", bikeInfraItemID)
	if err != nil {
		return err
	}
	iceFC, err := loadFeatureCollection(ctx, cfg.IceFile, cfg.SaveDownloadsDir, "ice.geojson", iceRoutesItemID)
	if err != nil {
		return err
	}

	var debugEntries []debugEntry
	titleNormalizer := newTitleNormalizer()
	seedTitleNormalizerFromTravelways(travelwaysFC, titleNormalizer)
	seedTitleNormalizerFromBike(bikeFC, titleNormalizer)
	travelwaysFeatures, err := travelwayLines(travelwaysFC, titleNormalizer, &debugEntries)
	if err != nil {
		return err
	}

	priorityTravelways, priorityTravelwayRoutes, err := travelwayPriorityLines(travelwaysFC)
	if err != nil {
		return err
	}
	travelwaysIndex, err := newSpatialIndex(priorityTravelways, 48, 24)
	if err != nil {
		return err
	}
	travelwayTitles := travelwayTitleMap(travelwaysFeatures)
	iceLines, err := iceRouteLines(iceFC)
	if err != nil {
		return err
	}
	iceRoutes := iceRouteMap(iceFC)
	iceIndex, err := newSpatialIndex(iceLines, 48, 24)
	if err != nil {
		return err
	}

	nameTravelways, nameTravelwayTitles, err := travelwayNameLines(travelwaysFC, titleNormalizer)
	if err != nil {
		return err
	}
	nameTravelwaysIndex, err := newSpatialIndex(nameTravelways, 48, 24)
	if err != nil {
		return err
	}

	bikeFeatures, err := bikeLines(bikeFC, titleNormalizer, travelwaysIndex, nameTravelwaysIndex, travelwayTitles, nameTravelwayTitles, priorityTravelwayRoutes, iceRoutes, iceIndex, cfg.MaxMatchMeters, cfg.MaxAngleDeg, cfg.MinRunMeters, &debugEntries)
	if err != nil {
		return err
	}

	if err := writeFeaturesBin(cfg.TravelwaysOut, travelwaysFeatures, cfg.SimplifyMeters); err != nil {
		return err
	}
	if err := writeFeaturesBin(cfg.BikeOut, bikeFeatures, cfg.SimplifyMeters); err != nil {
		return err
	}
	if cfg.DebugOut != "" {
		debugCfg := debugConfig{
			MaxMatchMeters: cfg.MaxMatchMeters,
			MaxAngleDeg:    cfg.MaxAngleDeg,
			MinRunMeters:   cfg.MinRunMeters,
			SimplifyMeters: cfg.SimplifyMeters,
		}
		if err := writeDebug(cfg.DebugOut, debugEntries, debugCfg); err != nil {
			return err
		}
	}
	return nil
}

type lineFeature struct {
	stableID      string
	title         string
	priority      uint8
	coords        orb.LineString
	sourceDataset uint8
	objectID      int
	wintMaint     string
	wintRoute     string
	routeID       uint16
}

type debugEntry struct {
	Dataset        string         `json:"dataset"`
	ObjectID       int            `json:"object_id"`
	SourceStableID string         `json:"source_stable_id,omitempty"`
	RunStableIDs   []string       `json:"run_stable_ids,omitempty"`
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
	MaxMatchMeters float64 `json:"max_match_meters"`
	MaxAngleDeg    float64 `json:"max_angle_deg"`
	MinRunMeters   float64 `json:"min_run_meters"`
	SimplifyMeters float64 `json:"simplify_meters"`
}

func writeFeaturesBin(path string, features []lineFeature, simplifyMeters float64) error {
	if simplifyMeters > 0 {
		var before, after int
		for i := range features {
			before += len(features[i].coords)
			features[i].coords = simplifyLineString(features[i].coords, simplifyMeters)
			after += len(features[i].coords)
		}
		log.Printf("simplify %s: points %d -> %d (tolerance %.1fm)", path, before, after, simplifyMeters)
	}
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

func travelwayStableID(props geojson.Properties, objectID int) string {
	if id := strings.TrimSpace(props.MustString("ASSETID", "")); id != "" {
		return id
	}
	if id := strings.TrimSpace(props.MustString("TR_ID", "")); id != "" {
		return id
	}
	if objectID != 0 {
		return fmt.Sprintf("objectid:%d", objectID)
	}
	return ""
}

func bikeStableID(props geojson.Properties, objectID int) string {
	if id := strings.TrimSpace(props.MustString("BIKEFACID", "")); id != "" {
		return id
	}
	if objectID != 0 {
		return fmt.Sprintf("objectid:%d", objectID)
	}
	return ""
}

func runStableSuffix(run lineRun) string {
	h := fnv.New64a()
	for _, coord := range run.coords {
		lon := int32(math.Round(coord[0] * 1_000_000))
		lat := int32(math.Round(coord[1] * 1_000_000))
		_ = binary.Write(h, binary.LittleEndian, lon)
		_ = binary.Write(h, binary.LittleEndian, lat)
	}
	_ = binary.Write(h, binary.LittleEndian, run.priority)
	_ = binary.Write(h, binary.LittleEndian, run.sourceDataset)
	_ = binary.Write(h, binary.LittleEndian, int32(run.startSegment))
	_ = binary.Write(h, binary.LittleEndian, int32(run.endSegment))
	return fmt.Sprintf("%x", h.Sum64())[:10]
}

func splitFixedChunks(value string, chunkSize int) []string {
	if value == "" || chunkSize <= 0 {
		return nil
	}
	out := make([]string, 0, (len(value)+chunkSize-1)/chunkSize)
	for i := 0; i < len(value); i += chunkSize {
		end := i + chunkSize
		if end > len(value) {
			end = len(value)
		}
		out = append(out, value[i:end])
	}
	return out
}

func ensureFieldPieces(value string, pieceEntries *[]string, pieceIndex map[string]uint16) ([]uint16, error) {
	if value == "" {
		return nil, nil
	}
	fields := strings.Fields(value)
	ids := make([]uint16, 0, len(fields))
	for _, field := range fields {
		id, ok := pieceIndex[field]
		if !ok {
			if len(*pieceEntries) >= math.MaxUint16 {
				return nil, fmt.Errorf("too many string pieces: %d exceeds uint16 capacity", len(*pieceEntries)+1)
			}
			*pieceEntries = append(*pieceEntries, field)
			id = uint16(len(*pieceEntries))
			pieceIndex[field] = id
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func travelwayLines(fc *geojson.FeatureCollection, titles *titleNormalizer, debug *[]debugEntry) ([]lineFeature, error) {
	features := make([]lineFeature, 0, len(fc.Features))
	skippedNoPlow := 0
	for _, f := range fc.Features {
		props := f.Properties
		objectID := props.MustInt("OBJECTID", 0)
		location := strings.TrimSpace(props.MustString("LOCATION", ""))
		wintPlow := strings.TrimSpace(props.MustString("WINT_PLOW", ""))
		wintLOS := strings.TrimSpace(props.MustString("WINT_LOS", ""))
		wintMaint := strings.TrimSpace(props.MustString("WINT_MAINT", ""))
		wintRoute := strings.TrimSpace(props.MustString("WINT_ROUTE", ""))
		owner := strings.TrimSpace(props.MustString("OWNER", ""))
		stableID := travelwayStableID(props, objectID)
		if isPrivateOwner(owner) {
			appendDebug(debug, debugEntry{
				Dataset:        "travelways",
				ObjectID:       objectID,
				SourceStableID: stableID,
				Title:          props.MustString("LOCATION", ""),
				Included:       false,
				Reason:         "OWNER=PRIV",
				WintPlow:       wintPlow,
				WintLOS:        wintLOS,
			})
			continue
		}
		if isNotPlowed(props) {
			skippedNoPlow++
			appendDebug(debug, debugEntry{
				Dataset:        "travelways",
				ObjectID:       objectID,
				SourceStableID: stableID,
				Title:          props.MustString("LOCATION", ""),
				Included:       false,
				Reason:         "WINT_PLOW=N",
				WintPlow:       wintPlow,
				WintLOS:        wintLOS,
			})
			continue
		}

		priority, ok := priorityFromWintLOS(wintLOS)
		if !ok {
			appendDebug(debug, debugEntry{
				Dataset:        "travelways",
				ObjectID:       objectID,
				SourceStableID: stableID,
				Title:          location,
				Included:       false,
				Reason:         "missing or invalid WINT_LOS",
				WintPlow:       wintPlow,
				WintLOS:        wintLOS,
			})
			continue
		}

		ls, ok, err := flattenLineString(f.Geometry)
		if err != nil {
			appendDebug(debug, debugEntry{
				Dataset:        "travelways",
				ObjectID:       objectID,
				SourceStableID: stableID,
				Title:          props.MustString("LOCATION", ""),
				Included:       false,
				Reason:         "invalid geometry",
				WintPlow:       wintPlow,
				WintLOS:        wintLOS,
			})
			return nil, err
		}
		if !ok {
			appendDebug(debug, debugEntry{
				Dataset:        "travelways",
				ObjectID:       objectID,
				SourceStableID: stableID,
				Title:          props.MustString("LOCATION", ""),
				Included:       false,
				Reason:         "empty geometry",
				WintPlow:       wintPlow,
				WintLOS:        wintLOS,
			})
			continue
		}

		if location == "" {
			appendDebug(debug, debugEntry{
				Dataset:        "travelways",
				ObjectID:       objectID,
				SourceStableID: stableID,
				Title:          "",
				Included:       false,
				Reason:         "missing LOCATION",
				WintPlow:       wintPlow,
				WintLOS:        wintLOS,
			})
			continue
		}
		title := titles.normalize(location)
		if strings.EqualFold(title, "CORNWALLIS ST") {
			title = titles.normalize("Nora Bernard St")
		}
		features = append(features, lineFeature{
			stableID:      stableID,
			title:         title,
			priority:      priority,
			coords:        ls,
			sourceDataset: datasetTravelways,
			objectID:      objectID,
			wintMaint:     wintMaint,
			wintRoute:     wintRoute,
		})
		appendDebug(debug, debugEntry{
			Dataset:        "travelways",
			ObjectID:       objectID,
			SourceStableID: stableID,
			RunStableIDs:   []string{stableID},
			Title:          title,
			Included:       true,
			Reason:         "included",
			Priority:       priority,
			WintPlow:       wintPlow,
			WintLOS:        wintLOS,
			Coords:         ls,
		})
	}

	if skippedNoPlow > 0 {
		log.Printf("travelways skipped not plowed=%d", skippedNoPlow)
	}
	return features, nil
}

func travelwayNameLines(fc *geojson.FeatureCollection, titles *titleNormalizer) ([]indexedLine, map[int]string, error) {
	lines := make([]indexedLine, 0, len(fc.Features))
	titleMap := make(map[int]string)
	for _, f := range fc.Features {
		props := f.Properties
		objectID := props.MustInt("OBJECTID", 0)
		owner := strings.TrimSpace(props.MustString("OWNER", ""))
		if isPrivateOwner(owner) {
			continue
		}
		location := strings.TrimSpace(props.MustString("LOCATION", ""))
		if location == "" {
			continue
		}
		ls, ok, err := flattenLineString(f.Geometry)
		if err != nil || !ok {
			continue
		}
		title := titles.normalize(location)
		if strings.EqualFold(title, "CORNWALLIS ST") {
			title = titles.normalize("Nora Bernard St")
		}
		titleMap[objectID] = title
		lines = append(lines, indexedLine{
			coords:   ls,
			priority: 1,
			objectID: objectID,
		})
	}
	if len(lines) == 0 {
		return nil, nil, fmt.Errorf("no travelway name lines")
	}
	return lines, titleMap, nil
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

type routeInfo struct {
	maint string
	route string
}

func travelwayPriorityLines(fc *geojson.FeatureCollection) ([]indexedLine, map[int]routeInfo, error) {
	lines := make([]indexedLine, 0, len(fc.Features))
	routes := make(map[int]routeInfo)
	for _, f := range fc.Features {
		props := f.Properties
		objectID := props.MustInt("OBJECTID", 0)
		owner := strings.TrimSpace(props.MustString("OWNER", ""))
		if isPrivateOwner(owner) {
			continue
		}
		if isNotPlowed(props) {
			continue
		}
		wintLOS := strings.TrimSpace(props.MustString("WINT_LOS", ""))
		priority, ok := priorityFromWintLOS(wintLOS)
		if !ok {
			continue
		}
		ls, ok, err := flattenLineString(f.Geometry)
		if err != nil || !ok {
			continue
		}
		lines = append(lines, indexedLine{
			coords:   ls,
			priority: priority,
			objectID: objectID,
		})
		wintMaint := strings.TrimSpace(props.MustString("WINT_MAINT", ""))
		wintRoute := strings.TrimSpace(props.MustString("WINT_ROUTE", ""))
		if wintMaint != "" || wintRoute != "" {
			routes[objectID] = routeInfo{maint: wintMaint, route: wintRoute}
		}
	}
	if len(lines) == 0 {
		return nil, nil, fmt.Errorf("no travelway priority lines")
	}
	return lines, routes, nil
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

func bikeLines(fc *geojson.FeatureCollection, titles *titleNormalizer, travelwaysIndex, nameTravelwaysIndex *spatialIndex, travelwayTitles, nameTravelwayTitles map[int]string, travelwayRoutes map[int]routeInfo, iceRoutes map[int]routeInfo, iceIndex *spatialIndex, maxMatchMeters, maxAngleDeg, minRunMeters float64, debug *[]debugEntry) ([]lineFeature, error) {
	var (
		matchedTravelways int
		matchedIce        int
		matchedBike       int
		matchedFallback   int
		skipped           int
		skippedNotPlowed  int
		skippedNoName     int
	)
	maxAngleRad := deg2rad(maxAngleDeg)

	features := make([]lineFeature, 0, len(fc.Features))
	for _, f := range fc.Features {
		props := f.Properties
		objectID := props.MustInt("OBJECTID", 0)
		wintPlow := strings.TrimSpace(props.MustString("WINT_PLOW", ""))
		wintLOS := strings.TrimSpace(props.MustString("WINT_LOS", ""))
		bikeType := strings.TrimSpace(props.MustString("BIKETYPE", ""))
		baseStableID := bikeStableID(props, objectID)
		if isNotPlowed(props) {
			skippedNotPlowed++
			appendDebug(debug, debugEntry{
				Dataset:        "bike",
				ObjectID:       objectID,
				SourceStableID: baseStableID,
				Title:          props.MustString("BIKE_NAME", ""),
				Included:       false,
				Reason:         "WINT_PLOW=N",
				WintPlow:       wintPlow,
				WintLOS:        wintLOS,
				BikeType:       bikeType,
				ProtType:       props.MustString("PROT_TYPE", ""),
				BikeName:       props.MustString("BIKE_NAME", ""),
				StreetName:     props.MustString("STREETNAME", ""),
			})
			continue
		}

		lines, err := lineStringsFromGeometry(f.Geometry)
		if err != nil {
			return nil, err
		}
		if len(lines) == 0 {
			appendDebug(debug, debugEntry{
				Dataset:        "bike",
				ObjectID:       objectID,
				SourceStableID: baseStableID,
				Title:          props.MustString("BIKE_NAME", ""),
				Included:       false,
				Reason:         "empty geometry",
				WintPlow:       wintPlow,
				WintLOS:        wintLOS,
				BikeType:       bikeType,
				ProtType:       props.MustString("PROT_TYPE", ""),
				BikeName:       props.MustString("BIKE_NAME", ""),
				StreetName:     props.MustString("STREETNAME", ""),
			})
			continue
		}

		baseTitle, baseTitleFromType := bikeTitle(props, titles)
		wintMaint := strings.TrimSpace(props.MustString("WINT_MAINT", ""))
		wintRoute := strings.TrimSpace(props.MustString("WINT_ROUTE", ""))
		preferredBikePriority, hasPreferredBikePriority := preferredBikePriority(wintPlow, wintLOS, wintMaint, wintRoute)

		isHelpConn := strings.EqualFold(bikeType, "HELPCONN")
		isProtected := isProtectedBike(props)
		isOffstreetFallback := strings.EqualFold(bikeType, "MUPATH") ||
			strings.EqualFold(bikeType, "INT_MUPATH") ||
			strings.EqualFold(strings.TrimSpace(props.MustString("PROT_TYPE", "")), "OFFSTREET")

		for _, ls := range lines {
			title := baseTitle
			titleFromType := baseTitleFromType
			var (
				sourceDataset uint8
				attr          overlapAttributionResult
				found         bool
				reason        string
			)

			if hasPreferredBikePriority {
				length := lineLengthMeters(ls, projectorForLine(ls))
				attr = overlapAttributionResult{
					byPriority: map[uint8]float64{preferredBikePriority: length},
				}
				sourceDataset = datasetBike
				reason = "prefer bike WINT_LOS+route"
				found = true
				matchedBike++
			} else {
				if isHelpConn {
					attr = overlapAttributionPrefer(ls, iceIndex, datasetIce, travelwaysIndex, datasetTravelways, maxMatchMeters, maxAngleRad)
					if attr.totalLength > 0 {
						sourceDataset = datasetIce
						reason = "overlap-first ice with travelways fallback"
						found = true
						matchedIce++
					}
				} else if isProtected {
					if isOffstreetFallback {
						attr = overlapAttributionPrefer(ls, travelwaysIndex, datasetTravelways, iceIndex, datasetIce, maxMatchMeters, maxAngleRad)
						if attr.totalLength > 0 {
							sourceDataset = datasetTravelways
							reason = "overlap-first travelways with ice fallback"
							found = true
							matchedTravelways++
						}
					} else {
						attr = overlapAttribution(ls, travelwaysIndex, datasetTravelways, maxMatchMeters, maxAngleRad)
						if attr.totalLength > 0 {
							sourceDataset = datasetTravelways
							reason = "overlap-first travelways"
							found = true
							matchedTravelways++
						}
					}
				} else {
					if isOffstreetFallback {
						attr = overlapAttributionPrefer(ls, travelwaysIndex, datasetTravelways, iceIndex, datasetIce, maxMatchMeters, maxAngleRad)
						if attr.totalLength > 0 {
							sourceDataset = datasetTravelways
							reason = "overlap-first travelways with ice fallback"
							found = true
						}
					} else {
						attr = overlapAttribution(ls, iceIndex, datasetIce, maxMatchMeters, maxAngleRad)
						if attr.totalLength > 0 {
							sourceDataset = datasetIce
							reason = "overlap-first ice"
							found = true
						}
					}
					if attr.totalLength > 0 {
						if sourceDataset == datasetTravelways {
							matchedTravelways++
						} else if sourceDataset == datasetIce {
							matchedIce++
						}
					}
				}
			}

			if title == "" {
				skippedNoName++
				appendDebug(debug, debugEntry{
					Dataset:        "bike",
					ObjectID:       objectID,
					SourceStableID: baseStableID,
					Title:          "",
					Included:       false,
					Reason:         "missing name",
					WintPlow:       wintPlow,
					WintLOS:        wintLOS,
					BikeType:       bikeType,
					ProtType:       props.MustString("PROT_TYPE", ""),
					BikeName:       props.MustString("BIKE_NAME", ""),
					StreetName:     props.MustString("STREETNAME", ""),
					Coords:         ls,
				})
				continue
			}

			if !found {
				if fallback, ok := priorityFromWintLOS(props.MustString("WINT_LOS", "")); ok {
					length := lineLengthMeters(ls, projectorForLine(ls))
					attr = overlapAttributionResult{
						byPriority: map[uint8]float64{fallback: length},
					}
					sourceDataset = datasetBike
					reason = "fallback WINT_LOS"
					found = true
					matchedFallback++
				}
			}

			if !found {
				skipped++
				appendDebug(debug, debugEntry{
					Dataset:        "bike",
					ObjectID:       objectID,
					SourceStableID: baseStableID,
					Title:          title,
					Included:       false,
					Reason:         "no match",
					WintPlow:       wintPlow,
					WintLOS:        wintLOS,
					BikeType:       bikeType,
					ProtType:       props.MustString("PROT_TYPE", ""),
					BikeName:       props.MustString("BIKE_NAME", ""),
					StreetName:     props.MustString("STREETNAME", ""),
					ProtectedBike:  isProtected,
					Coords:         ls,
				})
				continue
			}

			var runs []lineRun
			if sourceDataset == datasetBike {
				runs = []lineRun{
					{
						priority:      dominantPriority(attr.byPriority),
						sourceDataset: sourceDataset,
						coords:        ls,
						length:        lineLengthMeters(ls, projectorForLine(ls)),
					},
				}
			} else {
				runs = runsFromAssignments(attr.assignments, minRunMeters)
			}
			if len(runs) == 0 {
				skipped++
				appendDebug(debug, debugEntry{
					Dataset:        "bike",
					ObjectID:       objectID,
					SourceStableID: baseStableID,
					Title:          title,
					Included:       false,
					Reason:         "no overlap runs",
					WintPlow:       wintPlow,
					WintLOS:        wintLOS,
					BikeType:       bikeType,
					ProtType:       props.MustString("PROT_TYPE", ""),
					BikeName:       props.MustString("BIKE_NAME", ""),
					StreetName:     props.MustString("STREETNAME", ""),
					ProtectedBike:  isProtected,
					Coords:         ls,
				})
				continue
			}

			runStableIDs := make([]string, 0, len(runs))
			for _, run := range runs {
				runTitle := title
				if (runTitle == "" || titleFromType) && nameTravelwaysIndex != nil {
					nameAttr := overlapAttribution(run.coords, nameTravelwaysIndex, datasetTravelways, maxMatchMeters, maxAngleRad)
					if id := dominantObjectID(nameAttr.byObjectID); id != 0 {
						if name := nameTravelwayTitles[id]; name != "" {
							runTitle = name
						}
					}
				}
				runWintMaint := wintMaint
				runWintRoute := wintRoute
				if runWintMaint == "" && runWintRoute == "" {
					dominantID := dominantObjectID(run.byObjectID)
					if dominantID != 0 {
						if run.sourceDataset == datasetTravelways {
							if info, ok := travelwayRoutes[dominantID]; ok {
								runWintMaint = info.maint
								runWintRoute = info.route
							}
						} else if run.sourceDataset == datasetIce {
							if info, ok := iceRoutes[dominantID]; ok {
								runWintMaint = info.maint
								runWintRoute = info.route
							}
						}
					}
				}
				runStableID := baseStableID
				if runStableID != "" {
					runStableID = runStableID + ":" + runStableSuffix(run)
				}
				runStableIDs = append(runStableIDs, runStableID)
				features = append(features, lineFeature{
					stableID:      runStableID,
					title:         runTitle,
					priority:      run.priority,
					coords:        run.coords,
					sourceDataset: run.sourceDataset,
					objectID:      objectID,
					wintMaint:     runWintMaint,
					wintRoute:     runWintRoute,
				})
			}

			appendDebug(debug, debugEntry{
				Dataset:        "bike",
				ObjectID:       objectID,
				SourceStableID: baseStableID,
				RunStableIDs:   runStableIDs,
				Title:          title,
				Included:       true,
				Reason:         reason,
				Priority:       dominantPriority(attr.byPriority),
				WintPlow:       wintPlow,
				WintLOS:        wintLOS,
				BikeType:       bikeType,
				ProtType:       props.MustString("PROT_TYPE", ""),
				BikeName:       props.MustString("BIKE_NAME", ""),
				StreetName:     props.MustString("STREETNAME", ""),
				ProtectedBike:  isProtected,
				SourceDataset:  datasetName(sourceDataset),
				Coords:         ls,
			})
		}
	}

	log.Printf("bike lines matched travelways=%d ice=%d bike=%d fallback=%d skipped=%d", matchedTravelways, matchedIce, matchedBike, matchedFallback, skipped)
	if skippedNotPlowed > 0 {
		log.Printf("bike lines skipped not plowed=%d", skippedNotPlowed)
	}
	if skippedNoName > 0 {
		log.Printf("bike lines skipped missing name=%d", skippedNoName)
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
	case "PROTBL", "INT_PROTBL":
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

func preferredBikePriority(wintPlow, wintLOS, wintMaint, wintRoute string) (uint8, bool) {
	if !strings.EqualFold(strings.TrimSpace(wintPlow), "Y") {
		return 0, false
	}
	if strings.TrimSpace(wintMaint) == "" && strings.TrimSpace(wintRoute) == "" {
		return 0, false
	}
	return priorityFromWintLOS(wintLOS)
}

func iceRouteLines(fc *geojson.FeatureCollection) ([]indexedLine, error) {
	lines := make([]indexedLine, 0, len(fc.Features))
	for _, f := range fc.Features {
		props := f.Properties
		objectID := props.MustInt("OBJECTID", 0)
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
			objectID: objectID,
		})
	}

	if len(lines) == 0 {
		return nil, fmt.Errorf("no ice route lines")
	}

	return lines, nil
}

func iceRouteMap(fc *geojson.FeatureCollection) map[int]routeInfo {
	routes := make(map[int]routeInfo, len(fc.Features))
	for _, f := range fc.Features {
		props := f.Properties
		objectID := props.MustInt("OBJECTID", 0)
		if objectID == 0 {
			continue
		}
		maint := strings.TrimSpace(props.MustString("OPERATOR", ""))
		route := strings.TrimSpace(props.MustString("ROUTE_NAME", ""))
		if maint == "" && route == "" {
			continue
		}
		routes[objectID] = routeInfo{maint: maint, route: route}
	}
	return routes
}

func flattenLineString(geom orb.Geometry) (orb.LineString, bool, error) {
	var ls orb.LineString
	switch g := geom.(type) {
	case nil:
		return nil, false, nil
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

func lineStringsFromGeometry(geom orb.Geometry) ([]orb.LineString, error) {
	switch g := geom.(type) {
	case nil:
		return nil, nil
	case orb.LineString:
		if len(g) == 0 {
			return nil, nil
		}
		return []orb.LineString{g}, nil
	case orb.MultiLineString:
		out := make([]orb.LineString, 0, len(g))
		for _, sub := range g {
			if len(sub) == 0 {
				continue
			}
			out = append(out, sub)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown geometry type: %T", g)
	}
}

func writeUvarint(w io.Writer, value uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	_, err := w.Write(buf[:n])
	return err
}

func encodeZigZag(value int64) uint64 {
	return uint64(uint64(value<<1) ^ uint64(value>>63))
}

func writeVarintZigZag(w io.Writer, value int64) error {
	return writeUvarint(w, encodeZigZag(value))
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
	routeEntries := make([]routeInfo, 0)
	routeIndex := make(map[routeInfo]uint16)
	pieceEntries := make([]string, 0)
	pieceIndex := make(map[string]uint16)
	stablePieceIDs := make(map[string][]uint16)
	titlePieceIDs := make(map[string][]uint16)
	routeMaintPieceIDs := make(map[string][]uint16)
	routeNamePieceIDs := make(map[string][]uint16)

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

		var routeID uint16
		if feature.wintMaint != "" || feature.wintRoute != "" {
			key := routeInfo{maint: feature.wintMaint, route: feature.wintRoute}
			if id, ok := routeIndex[key]; ok {
				routeID = id
			} else {
				if len(routeEntries) >= math.MaxUint16 {
					return fmt.Errorf("too many winter routes: %d exceeds uint16 capacity", len(routeEntries)+1)
				}
				routeEntries = append(routeEntries, key)
				routeID = uint16(len(routeEntries))
				routeIndex[key] = routeID
				if _, ok := routeMaintPieceIDs[key.maint]; !ok {
					ids, err := ensureFieldPieces(key.maint, &pieceEntries, pieceIndex)
					if err != nil {
						return err
					}
					routeMaintPieceIDs[key.maint] = ids
				}
				if _, ok := routeNamePieceIDs[key.route]; !ok {
					ids, err := ensureFieldPieces(key.route, &pieceEntries, pieceIndex)
					if err != nil {
						return err
					}
					routeNamePieceIDs[key.route] = ids
				}
			}
		}
		feature.routeID = routeID

		if feature.stableID != "" {
			if _, ok := stablePieceIDs[feature.stableID]; !ok {
				chunks := splitFixedChunks(feature.stableID, 3)
				ids := make([]uint16, 0, len(chunks))
				for _, chunk := range chunks {
					id, ok := pieceIndex[chunk]
					if !ok {
						if len(pieceEntries) >= math.MaxUint16 {
							return fmt.Errorf("too many string pieces: %d exceeds uint16 capacity", len(pieceEntries)+1)
						}
						pieceEntries = append(pieceEntries, chunk)
						id = uint16(len(pieceEntries))
						pieceIndex[chunk] = id
					}
					ids = append(ids, id)
				}
				stablePieceIDs[feature.stableID] = ids
			}
		}
		if feature.title != "" {
			if _, ok := titlePieceIDs[feature.title]; !ok {
				ids, err := ensureFieldPieces(feature.title, &pieceEntries, pieceIndex)
				if err != nil {
					return err
				}
				titlePieceIDs[feature.title] = ids
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

	if _, err := writer.Write([]byte(featuresBinMagic)); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, featuresBinVersion); err != nil {
		return err
	}
	if err := writeUvarint(writer, uint64(len(segments))); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, globalMinLon); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, globalMinLat); err != nil {
		return err
	}
	if err := writeUvarint(writer, uint64(len(routeEntries))); err != nil {
		return err
	}
	if err := writeUvarint(writer, uint64(len(pieceEntries))); err != nil {
		return err
	}
	for _, piece := range pieceEntries {
		pieceBytes := []byte(piece)
		if len(pieceBytes) > 255 {
			return fmt.Errorf("title piece too long: %q exceeds 255 bytes", piece)
		}
		if err := writeUvarint(writer, uint64(len(pieceBytes))); err != nil {
			return err
		}
		if _, err := writer.Write(pieceBytes); err != nil {
			return err
		}
	}
	for _, entry := range routeEntries {
		maintIDs := routeMaintPieceIDs[entry.maint]
		routeIDs := routeNamePieceIDs[entry.route]
		if err := writeUvarint(writer, uint64(len(maintIDs))); err != nil {
			return err
		}
		for _, id := range maintIDs {
			if err := writeUvarint(writer, uint64(id)); err != nil {
				return err
			}
		}
		if err := writeUvarint(writer, uint64(len(routeIDs))); err != nil {
			return err
		}
		for _, id := range routeIDs {
			if err := writeUvarint(writer, uint64(id)); err != nil {
				return err
			}
		}
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
		if err := writeVarintZigZag(writer, int64(deltaMinLon)); err != nil {
			return err
		}
		if err := writeVarintZigZag(writer, int64(deltaMinLat)); err != nil {
			return err
		}
		if err := writeVarintZigZag(writer, int64(deltaMaxLon)); err != nil {
			return err
		}
		if err := writeVarintZigZag(writer, int64(deltaMaxLat)); err != nil {
			return err
		}

		if err := writeUvarint(writer, uint64(len(seg.features))); err != nil {
			return err
		}

		for _, f := range seg.features {
			stableIDs := []uint16(nil)
			if f.stableID != "" {
				ids, ok := stablePieceIDs[f.stableID]
				if !ok {
					return fmt.Errorf("missing stable id pieces for stable id %q", f.stableID)
				}
				stableIDs = ids
			}
			if len(stableIDs) > math.MaxUint8 {
				return fmt.Errorf("too many stable id pieces in stable id %q: %d exceeds uint8 capacity", f.stableID, len(stableIDs))
			}
			if err := writeUvarint(writer, uint64(len(stableIDs))); err != nil {
				return err
			}
			for _, pieceID := range stableIDs {
				if err := writeUvarint(writer, uint64(pieceID)); err != nil {
					return err
				}
			}
			pieceIDs := []uint16(nil)
			if f.title != "" {
				ids, ok := titlePieceIDs[f.title]
				if !ok {
					return fmt.Errorf("missing title pieces for title %q", f.title)
				}
				pieceIDs = ids
			}
			if len(pieceIDs) > math.MaxUint8 {
				return fmt.Errorf("too many title pieces in feature title %q: %d exceeds uint8 capacity", f.title, len(pieceIDs))
			}
			if err := writeUvarint(writer, uint64(len(pieceIDs))); err != nil {
				return err
			}
			for _, pieceID := range pieceIDs {
				if err := writeUvarint(writer, uint64(pieceID)); err != nil {
					return err
				}
			}
			if err := writeUvarint(writer, uint64(f.priority)); err != nil {
				return err
			}
			if err := writeUvarint(writer, uint64(f.sourceDataset)); err != nil {
				return err
			}
			if err := writeUvarint(writer, uint64(f.routeID)); err != nil {
				return err
			}

			if len(f.coords) > math.MaxUint16 {
				return fmt.Errorf("too many coordinates in feature: %d exceeds uint16 capacity", len(f.coords))
			}
			if err := writeUvarint(writer, uint64(len(f.coords))); err != nil {
				return err
			}
			prevLon := int32(0)
			prevLat := int32(0)
			for i, coord := range f.coords {
				absLon := int32(math.Round((coord[0] - globalMinLon) * 1000000))
				absLat := int32(math.Round((coord[1] - globalMinLat) * 1000000))
				dLon := absLon
				dLat := absLat
				if i > 0 {
					dLon = absLon - prevLon
					dLat = absLat - prevLat
				}
				if err := writeVarintZigZag(writer, int64(dLon)); err != nil {
					return err
				}
				if err := writeVarintZigZag(writer, int64(dLat)); err != nil {
					return err
				}
				prevLon = absLon
				prevLat = absLat
			}
		}
	}
	return nil
}

func loadFeatureCollection(ctx context.Context, path, saveDir, saveName, itemID string) (*geojson.FeatureCollection, error) {
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
		if saveDir != "" {
			if err := os.MkdirAll(saveDir, 0755); err != nil {
				return nil, err
			}
			if err := os.WriteFile(filepath.Join(saveDir, saveName), data, 0644); err != nil {
				return nil, err
			}
		}
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
	coords   orb.LineString
	xy       []pointXY
	minLon   float64
	minLat   float64
	maxLon   float64
	maxLat   float64
	priority uint8
	objectID int
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

type segmentAssignment struct {
	priority       uint8
	start          orb.Point
	end            orb.Point
	length         float64
	distanceMeters float64
	objectID       int
	sourceDataset  uint8
	segmentIndex   int
}

type overlapAttributionResult struct {
	totalLength float64
	byPriority  map[uint8]float64
	byObjectID  map[int]float64
	assignments []segmentAssignment
}

type lineRun struct {
	priority      uint8
	sourceDataset uint8
	coords        orb.LineString
	length        float64
	byObjectID    map[int]float64
	startSegment  int
	endSegment    int
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

type segmentXY struct {
	a     pointXY
	b     pointXY
	angle float64
}

func lineSegments(points []pointXY) []segmentXY {
	if len(points) < 2 {
		return nil
	}
	out := make([]segmentXY, 0, len(points)-1)
	for i := 0; i < len(points)-1; i++ {
		angle, ok := segmentAngle(points[i], points[i+1])
		if !ok {
			continue
		}
		out = append(out, segmentXY{
			a:     points[i],
			b:     points[i+1],
			angle: angle,
		})
	}
	return out
}

func overlapAttribution(line orb.LineString, idx *spatialIndex, sourceDataset uint8, maxDistanceMeters, maxAngleRad float64) overlapAttributionResult {
	result := overlapAttributionResult{
		byPriority: make(map[uint8]float64),
		byObjectID: make(map[int]float64),
	}
	if idx == nil || len(line) < 2 {
		return result
	}

	minLon, minLat, maxLon, maxLat := lineBounds(line)
	minLon -= metersToDegreesLon(maxDistanceMeters, idx.projector.lat0Rad)
	maxLon += metersToDegreesLon(maxDistanceMeters, idx.projector.lat0Rad)
	minLat -= metersToDegreesLat(maxDistanceMeters)
	maxLat += metersToDegreesLat(maxDistanceMeters)

	candidateIdxs := idx.candidates(minLon, minLat, maxLon, maxLat)
	if len(candidateIdxs) == 0 {
		return result
	}

	lineXY := idx.projector.lineToXY(line)
	lineSegmentsXY := lineSegments(lineXY)
	if len(lineSegmentsXY) == 0 {
		return result
	}
	type candidateLine struct {
		objectID int
		priority uint8
		segments []segmentXY
	}

	candidates := make([]candidateLine, 0, len(candidateIdxs))
	for _, i := range candidateIdxs {
		candidate := idx.lines[i]
		segs := lineSegments(candidate.xy)
		if len(segs) == 0 {
			continue
		}
		candidates = append(candidates, candidateLine{
			objectID: candidate.objectID,
			priority: candidate.priority,
			segments: segs,
		})
	}

	if len(candidates) == 0 {
		return result
	}

	for i := range lineSegmentsXY {
		seg := lineSegmentsXY[i]
		segLength := distancePoint(seg.a, seg.b)
		if segLength == 0 {
			continue
		}
		bestDist := math.Inf(1)
		bestPriority := uint8(0)
		bestObjectID := 0
		for _, cand := range candidates {
			for _, candSeg := range cand.segments {
				if maxAngleRad > 0 && angleDelta(seg.angle, candSeg.angle) > maxAngleRad {
					continue
				}
				d := segmentDistance(seg.a, seg.b, candSeg.a, candSeg.b)
				if d <= maxDistanceMeters && d < bestDist {
					bestDist = d
					bestPriority = cand.priority
					bestObjectID = cand.objectID
				}
			}
		}
		if bestPriority == 0 {
			continue
		}
		result.assignments = append(result.assignments, segmentAssignment{
			priority:       bestPriority,
			start:          line[i],
			end:            line[i+1],
			length:         segLength,
			distanceMeters: bestDist,
			objectID:       bestObjectID,
			sourceDataset:  sourceDataset,
			segmentIndex:   i,
		})
		result.totalLength += segLength
		result.byPriority[bestPriority] += segLength
		if bestObjectID != 0 {
			result.byObjectID[bestObjectID] += segLength
		}
	}

	return result
}

func overlapAttributionPrefer(line orb.LineString, primaryIdx *spatialIndex, primaryDataset uint8, fallbackIdx *spatialIndex, fallbackDataset uint8, maxDistanceMeters, maxAngleRad float64) overlapAttributionResult {
	result := overlapAttributionResult{
		byPriority: make(map[uint8]float64),
		byObjectID: make(map[int]float64),
	}
	if primaryIdx == nil && fallbackIdx == nil {
		return result
	}

	primary := overlapAttribution(line, primaryIdx, primaryDataset, maxDistanceMeters, maxAngleRad)
	if primaryIdx == nil || fallbackIdx == nil || primary.totalLength == 0 {
		if fallbackIdx == nil {
			return primary
		}
		fallback := overlapAttribution(line, fallbackIdx, fallbackDataset, maxDistanceMeters, maxAngleRad)
		if primary.totalLength == 0 {
			return fallback
		}
		return primary
	}

	fallback := overlapAttribution(line, fallbackIdx, fallbackDataset, maxDistanceMeters, maxAngleRad)

	// Merge by segment index (same line input)
	assignments := make([]segmentAssignment, 0, len(primary.assignments)+len(fallback.assignments))
	primaryMap := make(map[int]segmentAssignment)
	for _, seg := range primary.assignments {
		primaryMap[seg.segmentIndex] = seg
	}
	fallbackMap := make(map[int]segmentAssignment)
	for _, seg := range fallback.assignments {
		fallbackMap[seg.segmentIndex] = seg
	}
	for i := 0; i < len(line)-1; i++ {
		primarySeg, primaryOK := primaryMap[i]
		fallbackSeg, fallbackOK := fallbackMap[i]
		switch {
		case primaryOK && fallbackOK:
			seg := primarySeg
			if fallbackSeg.distanceMeters < primarySeg.distanceMeters {
				seg = fallbackSeg
			}
			assignments = append(assignments, seg)
			result.totalLength += seg.length
			result.byPriority[seg.priority] += seg.length
			if seg.objectID != 0 {
				result.byObjectID[seg.objectID] += seg.length
			}
		case primaryOK:
			assignments = append(assignments, primarySeg)
			result.totalLength += primarySeg.length
			result.byPriority[primarySeg.priority] += primarySeg.length
			if primarySeg.objectID != 0 {
				result.byObjectID[primarySeg.objectID] += primarySeg.length
			}
		case fallbackOK:
			assignments = append(assignments, fallbackSeg)
			result.totalLength += fallbackSeg.length
			result.byPriority[fallbackSeg.priority] += fallbackSeg.length
			if fallbackSeg.objectID != 0 {
				result.byObjectID[fallbackSeg.objectID] += fallbackSeg.length
			}
		}
	}
	result.assignments = assignments
	return result
}

func runsFromAssignments(assignments []segmentAssignment, minRunMeters float64) []lineRun {
	if len(assignments) == 0 {
		return nil
	}
	var runs []lineRun
	var current *lineRun
	lastSegment := -1

	for _, seg := range assignments {
		if seg.priority == 0 {
			if current != nil {
				runs = append(runs, *current)
				current = nil
			}
			lastSegment = -1
			continue
		}
		if lastSegment != -1 && seg.segmentIndex != lastSegment+1 {
			if current != nil {
				runs = append(runs, *current)
				current = nil
			}
		}
		if current == nil || current.priority != seg.priority || current.sourceDataset != seg.sourceDataset {
			if current != nil {
				runs = append(runs, *current)
			}
			current = &lineRun{
				priority:      seg.priority,
				sourceDataset: seg.sourceDataset,
				coords:        orb.LineString{seg.start, seg.end},
				length:        seg.length,
				byObjectID:    make(map[int]float64),
				startSegment:  seg.segmentIndex,
				endSegment:    seg.segmentIndex,
			}
			if seg.objectID != 0 {
				current.byObjectID[seg.objectID] = seg.length
			}
			lastSegment = seg.segmentIndex
			continue
		}
		current.coords = concatLineStrings(current.coords, orb.LineString{seg.start, seg.end})
		current.length += seg.length
		current.endSegment = seg.segmentIndex
		if seg.objectID != 0 {
			current.byObjectID[seg.objectID] += seg.length
		}
		lastSegment = seg.segmentIndex
	}
	if current != nil {
		runs = append(runs, *current)
	}
	if minRunMeters > 0 {
		runs = mergeShortRuns(runs, minRunMeters)
	}
	return runs
}

func mergeShortRuns(runs []lineRun, minLen float64) []lineRun {
	if len(runs) <= 1 {
		return runs
	}
	i := 0
	for i < len(runs) {
		if runs[i].length >= minLen || len(runs) == 1 {
			i++
			continue
		}
		if i == 0 {
			if runs[i].endSegment+1 != runs[1].startSegment {
				i++
				continue
			}
			runs[1].coords = concatLineStrings(runs[i].coords, runs[1].coords)
			runs[1].length += runs[i].length
			if runs[i].startSegment < runs[1].startSegment {
				runs[1].startSegment = runs[i].startSegment
			}
			for id, length := range runs[i].byObjectID {
				runs[1].byObjectID[id] += length
			}
			runs = append(runs[:i], runs[i+1:]...)
			continue
		}
		if i == len(runs)-1 {
			if runs[i-1].endSegment+1 != runs[i].startSegment {
				i++
				continue
			}
			runs[i-1].coords = concatLineStrings(runs[i-1].coords, runs[i].coords)
			runs[i-1].length += runs[i].length
			if runs[i].endSegment > runs[i-1].endSegment {
				runs[i-1].endSegment = runs[i].endSegment
			}
			for id, length := range runs[i].byObjectID {
				runs[i-1].byObjectID[id] += length
			}
			runs = append(runs[:i], runs[i+1:]...)
			i--
			continue
		}
		prevLen := runs[i-1].length
		nextLen := runs[i+1].length
		if nextLen >= prevLen {
			if runs[i].endSegment+1 != runs[i+1].startSegment {
				i++
				continue
			}
			runs[i+1].coords = concatLineStrings(runs[i].coords, runs[i+1].coords)
			runs[i+1].length += runs[i].length
			if runs[i].startSegment < runs[i+1].startSegment {
				runs[i+1].startSegment = runs[i].startSegment
			}
			for id, length := range runs[i].byObjectID {
				runs[i+1].byObjectID[id] += length
			}
			runs = append(runs[:i], runs[i+1:]...)
			continue
		}
		if runs[i-1].endSegment+1 != runs[i].startSegment {
			i++
			continue
		}
		runs[i-1].coords = concatLineStrings(runs[i-1].coords, runs[i].coords)
		runs[i-1].length += runs[i].length
		if runs[i].endSegment > runs[i-1].endSegment {
			runs[i-1].endSegment = runs[i].endSegment
		}
		for id, length := range runs[i].byObjectID {
			runs[i-1].byObjectID[id] += length
		}
		runs = append(runs[:i], runs[i+1:]...)
		i--
	}
	return runs
}

func concatLineStrings(a, b orb.LineString) orb.LineString {
	if len(a) == 0 {
		return append(orb.LineString{}, b...)
	}
	if len(b) == 0 {
		return a
	}
	if a[len(a)-1] == b[0] {
		return append(a, b[1:]...)
	}
	return append(a, b...)
}

func dominantObjectID(byObjectID map[int]float64) int {
	var bestID int
	bestLen := 0.0
	for id, length := range byObjectID {
		if length > bestLen {
			bestLen = length
			bestID = id
		}
	}
	return bestID
}

func dominantPriority(byPriority map[uint8]float64) uint8 {
	var bestPriority uint8
	bestLen := 0.0
	for p, length := range byPriority {
		if length > bestLen {
			bestLen = length
			bestPriority = p
		}
	}
	return bestPriority
}

func lineLengthMeters(line orb.LineString, proj projector) float64 {
	if len(line) < 2 {
		return 0
	}
	xy := proj.lineToXY(line)
	total := 0.0
	for i := 0; i < len(xy)-1; i++ {
		total += distancePoint(xy[i], xy[i+1])
	}
	return total
}

func simplifyLineString(line orb.LineString, toleranceMeters float64) orb.LineString {
	if toleranceMeters <= 0 || len(line) <= 2 {
		return line
	}
	proj := projectorForLine(line)
	xy := proj.lineToXY(line)
	keep := make([]bool, len(line))
	keep[0] = true
	keep[len(line)-1] = true
	stack := [][2]int{{0, len(line) - 1}}

	for len(stack) > 0 {
		n := len(stack) - 1
		start, end := stack[n][0], stack[n][1]
		stack = stack[:n]
		if end-start <= 1 {
			continue
		}
		maxDist := 0.0
		maxIdx := -1
		a := xy[start]
		b := xy[end]
		for i := start + 1; i < end; i++ {
			d := pointSegmentDistance(xy[i], a, b)
			if d > maxDist {
				maxDist = d
				maxIdx = i
			}
		}
		if maxIdx >= 0 && maxDist > toleranceMeters {
			keep[maxIdx] = true
			stack = append(stack, [2]int{start, maxIdx}, [2]int{maxIdx, end})
		}
	}

	out := make(orb.LineString, 0, len(line))
	for i, ok := range keep {
		if ok {
			out = append(out, line[i])
		}
	}
	if len(out) < 2 {
		return line
	}
	return out
}

func projectorForLine(line orb.LineString) projector {
	if len(line) == 0 {
		return projector{}
	}
	minLat, maxLat := line[0][1], line[0][1]
	for _, pt := range line[1:] {
		if pt[1] < minLat {
			minLat = pt[1]
		}
		if pt[1] > maxLat {
			maxLat = pt[1]
		}
	}
	lat0 := (minLat + maxLat) / 2
	return projector{lat0Rad: deg2rad(lat0)}
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
