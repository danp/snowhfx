package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/danp/snowhfx/internal/featuresbin"
)

type geojsonFeatureCollection struct {
	Type     string           `json:"type"`
	Features []geojsonFeature `json:"features"`
}

type geojsonFeature struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
	Geometry   geojsonGeometry        `json:"geometry"`
}

type geojsonGeometry struct {
	Type        string      `json:"type"`
	Coordinates interface{} `json:"coordinates"`
}

func writeGeoJSON(t *testing.T, path string, fc geojsonFeatureCollection) {
	t.Helper()
	b, err := json.Marshal(fc)
	if err != nil {
		t.Fatalf("marshal geojson: %v", err)
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		t.Fatalf("write geojson: %v", err)
	}
}

type decodedFeature struct {
	title         string
	priority      uint8
	sourceDataset uint8
	routeID       uint16
	coords        [][]float64
}

func readFeaturesBin(t *testing.T, path string) []decodedFeature {
	t.Helper()
	features, _, _, err := featuresbin.ReadFile(path)
	if err != nil {
		t.Fatalf("read features: %v", err)
	}
	out := make([]decodedFeature, 0, len(features))
	for _, feat := range features {
		out = append(out, decodedFeature{
			title:         feat.Title,
			priority:      feat.Priority,
			sourceDataset: feat.SourceDataset,
			routeID:       feat.RouteID,
			coords:        feat.Coords,
		})
	}
	return out
}

func findFeatureByTitle(features []decodedFeature, title string) *decodedFeature {
	for i := range features {
		if features[i].title == title {
			return &features[i]
		}
	}
	return nil
}

func addBaselineBikeAndIce(bike, ice geojsonFeatureCollection) (geojsonFeatureCollection, geojsonFeatureCollection) {
	bike.Features = append(bike.Features, geojsonFeature{
		Type: "Feature",
		Properties: map[string]interface{}{
			"OBJECTID":   90,
			"WINT_PLOW":  "Y",
			"WINT_LOS":   "PRI1",
			"BIKETYPE":   "ONSTREET",
			"PROT_TYPE":  "NONE",
			"BIKE_NAME":  "Baseline Bike",
			"STREETNAME": "Baseline St",
		},
		Geometry: geojsonGeometry{
			Type:        "LineString",
			Coordinates: [][]float64{{10, 10}, {10.001, 10}},
		},
	})
	ice.Features = append(ice.Features, geojsonFeature{
		Type: "Feature",
		Properties: map[string]interface{}{
			"PRIORITY": "1",
		},
		Geometry: geojsonGeometry{
			Type:        "LineString",
			Coordinates: [][]float64{{10, 10.0001}, {10.001, 10.0001}},
		},
	})
	return bike, ice
}

func runWithGeoJSON(t *testing.T, travelways, bike, ice geojsonFeatureCollection) (string, string) {
	t.Helper()
	dir := t.TempDir()
	travelwaysPath := filepath.Join(dir, "travelways.geojson")
	bikePath := filepath.Join(dir, "bike.geojson")
	icePath := filepath.Join(dir, "ice.geojson")
	travelwaysOut := filepath.Join(dir, "features.bin")
	bikeOut := filepath.Join(dir, "features_cycling.bin")

	writeGeoJSON(t, travelwaysPath, travelways)
	writeGeoJSON(t, bikePath, bike)
	writeGeoJSON(t, icePath, ice)

	cfg := runConfig{
		TravelwaysFile: travelwaysPath,
		BikeFile:       bikePath,
		IceFile:        icePath,
		TravelwaysOut:  travelwaysOut,
		BikeOut:        bikeOut,
		MaxMatchMeters: 30,
		MaxAngleDeg:    30,
	}
	if err := run(context.Background(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	return travelwaysOut, bikeOut
}

func TestTravelwaysFeaturesBin(t *testing.T) {
	travelways := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  1,
					"WINT_PLOW": "Y",
					"WINT_LOS":  "PRI2",
					"OWNER":     "HRM",
					"LOCATION":  "CORNWALLIS ST",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0, 0}, {0.001, 0}},
				},
			},
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  2,
					"WINT_PLOW": "N",
					"WINT_LOS":  "PRI1",
					"OWNER":     "HRM",
					"LOCATION":  "Not Plowed",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{1, 1}, {1.001, 1}},
				},
			},
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  3,
					"WINT_PLOW": "Y",
					"WINT_LOS":  "PRI1",
					"OWNER":     "PRIV",
					"LOCATION":  "Private Way",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{2, 2}, {2.001, 2}},
				},
			},
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  4,
					"WINT_PLOW": "Y",
					"WINT_LOS":  "PRI5",
					"OWNER":     "HRM",
					"LOCATION":  "Bad Priority",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{3, 3}, {3.001, 3}},
				},
			},
		},
	}
	bike := geojsonFeatureCollection{Type: "FeatureCollection"}
	ice := geojsonFeatureCollection{Type: "FeatureCollection"}
	bike, ice = addBaselineBikeAndIce(bike, ice)

	travelwaysOut, _ := runWithGeoJSON(t, travelways, bike, ice)
	features := readFeaturesBin(t, travelwaysOut)
	if len(features) != 1 {
		t.Fatalf("expected 1 travelway feature, got %d", len(features))
	}
	feature := features[0]
	if feature.title != "Nora Bernard St" {
		t.Fatalf("unexpected title: %q", feature.title)
	}
	if feature.priority != 2 {
		t.Fatalf("unexpected priority: %d", feature.priority)
	}
	if feature.sourceDataset != datasetTravelways {
		t.Fatalf("unexpected source dataset: %d", feature.sourceDataset)
	}
	if len(feature.coords) == 0 {
		t.Fatal("missing travelway coords")
	}
}

func TestTravelwaysRequiresLocation(t *testing.T) {
	travelways := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  10,
					"WINT_PLOW": "Y",
					"WINT_LOS":  "PRI1",
					"OWNER":     "HRM",
					"LOCATION":  "",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0, 0}, {0.001, 0}},
				},
			},
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  11,
					"WINT_PLOW": "Y",
					"WINT_LOS":  "PRI1",
					"OWNER":     "HRM",
					"LOCATION":  "Valid Name Line",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{5, 5}, {5.001, 5}},
				},
			},
		},
	}
	bike := geojsonFeatureCollection{Type: "FeatureCollection"}
	ice := geojsonFeatureCollection{Type: "FeatureCollection"}
	bike, ice = addBaselineBikeAndIce(bike, ice)

	travelwaysOut, _ := runWithGeoJSON(t, travelways, bike, ice)
	features := readFeaturesBin(t, travelwaysOut)
	if len(features) != 1 {
		t.Fatalf("expected 1 travelway feature, got %d", len(features))
	}
	if features[0].title != "Valid Name Line" {
		t.Fatalf("unexpected title: %q", features[0].title)
	}
}

func TestBikeFeaturesBin(t *testing.T) {
	type expectedBikeFeature struct {
		title         string
		priority      uint8
		sourceDataset uint8
		present       bool
	}

	tests := []struct {
		name          string
		travelways    geojsonFeatureCollection
		bike          geojsonFeatureCollection
		ice           geojsonFeatureCollection
		ensureOutput  bool
		minCount      int
		expectedTests []expectedBikeFeature
	}{
		{
			name: "protected prefers closer plowed",
			travelways: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":  1,
							"WINT_PLOW": "Y",
							"WINT_LOS":  "PRI1",
							"OWNER":     "HRM",
							"LOCATION":  "Plowed Way",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{0, 0.0001}, {0.001, 0.0001}},
						},
					},
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":  2,
							"WINT_PLOW": "N",
							"OWNER":     "HRM",
							"LOCATION":  "No Plow Way",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{0, 0.0002}, {0.001, 0.0002}},
						},
					},
				},
			},
			bike: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":   10,
							"WINT_PLOW":  "Y",
							"WINT_LOS":   "PRI1",
							"BIKETYPE":   "PROTBL",
							"PROT_TYPE":  "CURB",
							"BIKE_NAME":  "Test Protected",
							"STREETNAME": "Test St",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{0, 0}, {0.001, 0}},
						},
					},
				},
			},
			ice: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"PRIORITY": "1",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{5, 5}, {5.001, 5}},
						},
					},
				},
			},
			minCount: 1,
			expectedTests: []expectedBikeFeature{
				{
					title:         "Test Protected",
					priority:      1,
					sourceDataset: datasetTravelways,
					present:       true,
				},
			},
		},
		{
			name: "protected skipped when no-plow closer",
			travelways: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":  1,
							"WINT_PLOW": "Y",
							"WINT_LOS":  "PRI1",
							"OWNER":     "HRM",
							"LOCATION":  "Plowed Way",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{0, 0.0002}, {0.001, 0.0002}},
						},
					},
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":  2,
							"WINT_PLOW": "N",
							"OWNER":     "HRM",
							"LOCATION":  "No Plow Way",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{0, 0.00005}, {0.001, 0.00005}},
						},
					},
				},
			},
			bike: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":   11,
							"WINT_PLOW":  "Y",
							"WINT_LOS":   "PRI1",
							"BIKETYPE":   "PROTBL",
							"PROT_TYPE":  "CURB",
							"BIKE_NAME":  "Test Protected",
							"STREETNAME": "Test St",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{0, 0}, {0.001, 0}},
						},
					},
				},
			},
			ice: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"PRIORITY": "1",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{5, 5}, {5.001, 5}},
						},
					},
				},
			},
			ensureOutput: true,
			minCount:     1,
			expectedTests: []expectedBikeFeature{
				{
					title:         "Test Protected",
					present:       true,
					priority:      1,
					sourceDataset: datasetTravelways,
				},
				{
					title:   "Baseline Bike",
					present: true,
				},
			},
		},
		{
			name: "unprotected matches ice",
			travelways: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":  1,
							"WINT_PLOW": "Y",
							"WINT_LOS":  "PRI1",
							"OWNER":     "HRM",
							"LOCATION":  "Plowed Way",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{1, 1}, {1.001, 1}},
						},
					},
				},
			},
			bike: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":   12,
							"WINT_PLOW":  "Y",
							"WINT_LOS":   "PRI1",
							"BIKETYPE":   "ONSTREET",
							"PROT_TYPE":  "NONE",
							"BIKE_NAME":  "Test Unprotected",
							"STREETNAME": "Test St",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{0, 0}, {0.001, 0}},
						},
					},
				},
			},
			ice: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"PRIORITY": "2",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{0, 0.0001}, {0.001, 0.0001}},
						},
					},
				},
			},
			minCount: 1,
			expectedTests: []expectedBikeFeature{
				{
					title:         "Test Unprotected",
					priority:      2,
					sourceDataset: datasetIce,
					present:       true,
				},
			},
		},
		{
			name: "fallback to bike priority",
			travelways: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":  1,
							"WINT_PLOW": "Y",
							"WINT_LOS":  "PRI1",
							"OWNER":     "HRM",
							"LOCATION":  "Plowed Way",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{1, 1}, {1.001, 1}},
						},
					},
				},
			},
			bike: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"OBJECTID":   13,
							"WINT_PLOW":  "Y",
							"WINT_LOS":   "PRI2",
							"BIKETYPE":   "ONSTREET",
							"PROT_TYPE":  "NONE",
							"BIKE_NAME":  "Test Fallback",
							"STREETNAME": "Test St",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{0, 0}, {0.001, 0}},
						},
					},
				},
			},
			ice: geojsonFeatureCollection{
				Type: "FeatureCollection",
				Features: []geojsonFeature{
					{
						Type: "Feature",
						Properties: map[string]interface{}{
							"PRIORITY": "1",
						},
						Geometry: geojsonGeometry{
							Type:        "LineString",
							Coordinates: [][]float64{{5, 5}, {5.001, 5}},
						},
					},
				},
			},
			minCount: 1,
			expectedTests: []expectedBikeFeature{
				{
					title:         "Test Fallback",
					priority:      2,
					sourceDataset: datasetBike,
					present:       true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bike := tt.bike
			ice := tt.ice
			if tt.ensureOutput {
				bike, ice = addBaselineBikeAndIce(bike, ice)
			}
			_, bikeOut := runWithGeoJSON(t, tt.travelways, bike, ice)
			features := readFeaturesBin(t, bikeOut)
			if tt.minCount > 0 && len(features) < tt.minCount {
				t.Fatalf("expected at least %d bike features, got %d", tt.minCount, len(features))
			}
			for _, expected := range tt.expectedTests {
				actual := findFeatureByTitle(features, expected.title)
				if expected.present && actual == nil {
					t.Fatalf("expected feature %q to be present", expected.title)
				}
				if !expected.present && actual != nil {
					t.Fatalf("expected feature %q to be absent", expected.title)
				}
				if !expected.present || actual == nil {
					continue
				}
				if expected.priority != 0 && actual.priority != expected.priority {
					t.Fatalf("feature %q priority: got %d want %d", expected.title, actual.priority, expected.priority)
				}
				if expected.sourceDataset != 0 && actual.sourceDataset != expected.sourceDataset {
					t.Fatalf("feature %q source dataset: got %d want %d", expected.title, actual.sourceDataset, expected.sourceDataset)
				}
			}
		})
	}
}

func TestBikeMultiLinePreserved(t *testing.T) {
	travelways := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  1,
					"WINT_PLOW": "Y",
					"WINT_LOS":  "PRI1",
					"OWNER":     "HRM",
					"LOCATION":  "Split Trail",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0, 0}, {0.001, 0}},
				},
			},
		},
	}
	bike := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  200,
					"WINT_PLOW": "Y",
					"BIKETYPE":  "MUPATH",
					"PROT_TYPE": "OFFSTREET",
					"BIKE_NAME": "Split Trail",
				},
				Geometry: geojsonGeometry{
					Type: "MultiLineString",
					Coordinates: [][][]float64{
						{{0, 0}, {0.001, 0}},
						{{0, 1}, {0.001, 1}},
					},
				},
			},
		},
	}
	ice := geojsonFeatureCollection{Type: "FeatureCollection"}
	ice.Features = append(ice.Features, geojsonFeature{
		Type: "Feature",
		Properties: map[string]interface{}{
			"PRIORITY": "1",
		},
		Geometry: geojsonGeometry{
			Type:        "LineString",
			Coordinates: [][]float64{{0, 1}, {0.001, 1}},
		},
	})

	dir := t.TempDir()
	travelwaysPath := filepath.Join(dir, "travelways.geojson")
	bikePath := filepath.Join(dir, "bike.geojson")
	icePath := filepath.Join(dir, "ice.geojson")
	travelwaysOut := filepath.Join(dir, "features.bin")
	bikeOut := filepath.Join(dir, "features_cycling.bin")

	writeGeoJSON(t, travelwaysPath, travelways)
	writeGeoJSON(t, bikePath, bike)
	writeGeoJSON(t, icePath, ice)

	cfg := runConfig{
		TravelwaysFile: travelwaysPath,
		BikeFile:       bikePath,
		IceFile:        icePath,
		TravelwaysOut:  travelwaysOut,
		BikeOut:        bikeOut,
		MaxMatchMeters: 30,
		MaxAngleDeg:    30,
		MinRunMeters:   0,
	}
	if err := run(context.Background(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	features := readFeaturesBin(t, bikeOut)
	var count, travelCount, iceCount int
	for _, feat := range features {
		if feat.title != "Split Trail" {
			continue
		}
		count++
		if feat.sourceDataset == datasetTravelways {
			travelCount++
		}
		if feat.sourceDataset == datasetIce {
			iceCount++
		}
	}
	if count != 2 {
		t.Fatalf("expected 2 split trail features, got %d", count)
	}
	if travelCount == 0 || iceCount == 0 {
		t.Fatalf("expected travelways+ice split, got travelways=%d ice=%d", travelCount, iceCount)
	}
}

func TestBikeOverlapRuns(t *testing.T) {
	travelways := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  1,
					"WINT_PLOW": "Y",
					"WINT_LOS":  "PRI1",
					"OWNER":     "HRM",
					"LOCATION":  "Far Trail",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{10, 10}, {10.001, 10}},
				},
			},
		},
	}
	bike := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  300,
					"WINT_PLOW": "Y",
					"BIKETYPE":  "BRMAINRD",
					"PROT_TYPE": "NONE",
					"BIKE_NAME": "Overlap Run",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0, 0}, {0.0009, 0}, {0.0018, 0}},
				},
			},
		},
	}
	ice := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":   11,
					"PRIORITY":   "1",
					"OPERATOR":   "W1",
					"ROUTE_NAME": "R1",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0, 0}, {0.0005, 0}},
				},
			},
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":   22,
					"PRIORITY":   "2",
					"OPERATOR":   "W2",
					"ROUTE_NAME": "R2",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0.0013, 0}, {0.0018, 0}},
				},
			},
		},
	}

	dir := t.TempDir()
	travelwaysPath := filepath.Join(dir, "travelways.geojson")
	bikePath := filepath.Join(dir, "bike.geojson")
	icePath := filepath.Join(dir, "ice.geojson")
	travelwaysOut := filepath.Join(dir, "features.bin")
	bikeOut := filepath.Join(dir, "features_cycling.bin")

	writeGeoJSON(t, travelwaysPath, travelways)
	writeGeoJSON(t, bikePath, bike)
	writeGeoJSON(t, icePath, ice)

	cfg := runConfig{
		TravelwaysFile: travelwaysPath,
		BikeFile:       bikePath,
		IceFile:        icePath,
		TravelwaysOut:  travelwaysOut,
		BikeOut:        bikeOut,
		MaxMatchMeters: 30,
		MaxAngleDeg:    30,
		MinRunMeters:   0,
	}
	if err := run(context.Background(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}

	features := readFeaturesBin(t, bikeOut)
	routesByID := map[uint16]featuresbin.RouteEntry{}
	_, routes, _, err := featuresbin.ReadFile(bikeOut)
	if err != nil {
		t.Fatalf("read features (routes): %v", err)
	}
	for i, route := range routes {
		routesByID[uint16(i+1)] = route
	}
	var p1, p2 int
	var p1Route, p2Route string
	for _, feat := range features {
		if feat.title != "Overlap Run" {
			continue
		}
		switch feat.priority {
		case 1:
			p1++
			if route, ok := routesByID[feat.routeID]; ok {
				p1Route = route.Route
			}
		case 2:
			p2++
			if route, ok := routesByID[feat.routeID]; ok {
				p2Route = route.Route
			}
		}
	}
	if p1 == 0 || p2 == 0 {
		t.Fatalf("expected overlap runs with priorities 1 and 2, got p1=%d p2=%d", p1, p2)
	}
	if p1Route == "" || p2Route == "" || p1Route == p2Route {
		t.Fatalf("expected distinct routes per run, got p1=%q p2=%q", p1Route, p2Route)
	}
}

func TestBikeOffstreetFallbackToIce(t *testing.T) {
	travelways := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  1,
					"WINT_PLOW": "Y",
					"WINT_LOS":  "PRI1",
					"OWNER":     "HRM",
					"LOCATION":  "Far Trail",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{10, 10}, {10.001, 10}},
				},
			},
		},
	}
	bike := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  400,
					"WINT_PLOW": "Y",
					"BIKETYPE":  "MUPATH",
					"PROT_TYPE": "OFFSTREET",
					"BIKE_NAME": "Offstreet Fallback",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0, 0}, {0.001, 0}},
				},
			},
		},
	}
	ice := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type:       "Feature",
				Properties: map[string]interface{}{"PRIORITY": "1"},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0, 0}, {0.001, 0}},
				},
			},
		},
	}

	_, bikeOut := runWithGeoJSON(t, travelways, bike, ice)
	features := readFeaturesBin(t, bikeOut)
	found := false
	for _, feat := range features {
		if feat.title == "Offstreet Fallback" && feat.sourceDataset == datasetIce {
			found = true
		}
	}
	if !found {
		t.Fatal("expected offstreet fallback to ice")
	}
}

func TestBikeNameFallbackFromTravelways(t *testing.T) {
	travelways := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  1,
					"WINT_PLOW": "Y",
					"WINT_LOS":  "PRI1",
					"OWNER":     "HRM",
					"LOCATION":  "Fallback Name Trail",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0, 0}, {0.001, 0}},
				},
			},
		},
	}
	bike := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type: "Feature",
				Properties: map[string]interface{}{
					"OBJECTID":  500,
					"WINT_PLOW": "Y",
					"BIKETYPE":  "MUPATH",
					"PROT_TYPE": "OFFSTREET",
				},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{0, 0}, {0.001, 0}},
				},
			},
		},
	}
	ice := geojsonFeatureCollection{
		Type: "FeatureCollection",
		Features: []geojsonFeature{
			{
				Type:       "Feature",
				Properties: map[string]interface{}{"PRIORITY": "1"},
				Geometry: geojsonGeometry{
					Type:        "LineString",
					Coordinates: [][]float64{{5, 5}, {5.001, 5}},
				},
			},
		},
	}

	_, bikeOut := runWithGeoJSON(t, travelways, bike, ice)
	features := readFeaturesBin(t, bikeOut)
	found := false
	for _, feat := range features {
		if feat.title == "Fallback Name Trail" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected name fallback from travelways")
	}
}
