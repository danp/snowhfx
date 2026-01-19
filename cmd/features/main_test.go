package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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
	Coordinates [][]float64 `json:"coordinates"`
}

type decodedFeature struct {
	title         string
	priority      uint8
	sourceDataset uint8
	coords        [][]float64
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

func readFeaturesBin(t *testing.T, path string) []decodedFeature {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read features: %v", err)
	}
	r := bytes.NewReader(data)
	var segCount uint32
	if err := binary.Read(r, binary.LittleEndian, &segCount); err != nil {
		t.Fatalf("read segments: %v", err)
	}
	var globalMinLon, globalMinLat float64
	if err := binary.Read(r, binary.LittleEndian, &globalMinLon); err != nil {
		t.Fatalf("read global min lon: %v", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &globalMinLat); err != nil {
		t.Fatalf("read global min lat: %v", err)
	}

	var out []decodedFeature
	for i := uint32(0); i < segCount; i++ {
		var deltaMinLon, deltaMinLat, deltaMaxLon, deltaMaxLat int32
		if err := binary.Read(r, binary.LittleEndian, &deltaMinLon); err != nil {
			t.Fatalf("read seg min lon: %v", err)
		}
		if err := binary.Read(r, binary.LittleEndian, &deltaMinLat); err != nil {
			t.Fatalf("read seg min lat: %v", err)
		}
		if err := binary.Read(r, binary.LittleEndian, &deltaMaxLon); err != nil {
			t.Fatalf("read seg max lon: %v", err)
		}
		if err := binary.Read(r, binary.LittleEndian, &deltaMaxLat); err != nil {
			t.Fatalf("read seg max lat: %v", err)
		}
		_ = deltaMinLon
		_ = deltaMinLat
		_ = deltaMaxLon
		_ = deltaMaxLat

		var featureCount uint32
		if err := binary.Read(r, binary.LittleEndian, &featureCount); err != nil {
			t.Fatalf("read feature count: %v", err)
		}

		for j := uint32(0); j < featureCount; j++ {
			var titleLen uint8
			if err := binary.Read(r, binary.LittleEndian, &titleLen); err != nil {
				t.Fatalf("read title len: %v", err)
			}
			titleBytes := make([]byte, titleLen)
			if _, err := r.Read(titleBytes); err != nil {
				t.Fatalf("read title: %v", err)
			}
			var priority uint8
			if err := binary.Read(r, binary.LittleEndian, &priority); err != nil {
				t.Fatalf("read priority: %v", err)
			}
			var sourceDataset uint8
			if err := binary.Read(r, binary.LittleEndian, &sourceDataset); err != nil {
				t.Fatalf("read source dataset: %v", err)
			}
			var coordCount uint16
			if err := binary.Read(r, binary.LittleEndian, &coordCount); err != nil {
				t.Fatalf("read coord count: %v", err)
			}
			coords := make([][]float64, 0, coordCount)
			for k := uint16(0); k < coordCount; k++ {
				var dLon, dLat int32
				if err := binary.Read(r, binary.LittleEndian, &dLon); err != nil {
					t.Fatalf("read coord lon: %v", err)
				}
				if err := binary.Read(r, binary.LittleEndian, &dLat); err != nil {
					t.Fatalf("read coord lat: %v", err)
				}
				lon := globalMinLon + float64(dLon)/1000000
				lat := globalMinLat + float64(dLat)/1000000
				coords = append(coords, []float64{lon, lat})
			}
			out = append(out, decodedFeature{
				title:         string(titleBytes),
				priority:      priority,
				sourceDataset: sourceDataset,
				coords:        coords,
			})
		}
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
		TravelwaysFile:     travelwaysPath,
		BikeFile:           bikePath,
		IceFile:            icePath,
		TravelwaysOut:      travelwaysOut,
		BikeOut:            bikeOut,
		MaxMatchMeters:     30,
		MaxAngleDeg:        30,
		MaxOverallAngleDeg: 60,
		PriorityBiasMeters: 1,
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
					title:   "Test Protected",
					present: false,
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
