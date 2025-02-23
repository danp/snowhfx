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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func main() {
	ctx := context.Background()

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var travelwaysFile string
	fs.StringVar(&travelwaysFile, "travelways", "", "path to travelways geojson file, otherwise download")
	fs.Parse(os.Args[1:])

	var travelways []byte
	if travelwaysFile == "" {
		rc, err := download(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer rc.Close()

		b, err := io.ReadAll(rc)
		if err != nil {
			log.Fatal(err)
		}

		travelways = b
	} else {
		b, err := os.ReadFile(travelwaysFile)
		if err != nil {
			log.Fatal(err)
		}
		travelways = b
	}

	fc := geojson.NewFeatureCollection()
	if err := json.Unmarshal(travelways, &fc); err != nil {
		log.Fatal(err)
	}

	fc.Features = slices.DeleteFunc(fc.Features, func(f *geojson.Feature) bool {
		return f.Properties["WINT_LOS"] == nil
	})
	for _, f := range fc.Features {
		props := f.Properties
		f.Properties = make(geojson.Properties)

		priorityNum := strings.TrimPrefix(props.MustString("WINT_LOS", ""), "PRI")
		priority, err := strconv.Atoi(priorityNum)
		if err != nil {
			log.Fatal(err)
		}
		if priority < 1 || priority > 3 {
			log.Fatalf("invalid priority: %d", priority)
		}

		if title := props.MustString("LOCATION", ""); title != "" {
			if title == "CORNWALLIS ST" {
				title = "NORA BERNARD ST"
			}
			f.Properties["title"] = title
		}

		f.Properties["priority"] = priority
	}

	var out bytes.Buffer
	if err := encodeFeatures(fc.Features, &out); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile("features.bin", out.Bytes(), 0644); err != nil {
		log.Fatal(err)
	}
}

func encodeFeatures(features []*geojson.Feature, writer io.Writer) error {
	if len(features) == 0 {
		return fmt.Errorf("no features")
	}

	// 1. Compute global bounding box over all features.
	globalMinLon, globalMinLat := math.MaxFloat64, math.MaxFloat64
	globalMaxLon, globalMaxLat := -math.MaxFloat64, -math.MaxFloat64

	// We'll also build an intermediate slice that holds each feature's
	// flattened coordinate list along with a representative coordinate.
	type featureData struct {
		title    string
		priority uint8
		coords   orb.LineString // flattened geometry
	}
	// repLon, repLat are used for segmentation (here we use the first coordinate)
	type featureForSeg struct {
		data           featureData
		repLon, repLat float64
	}
	var featuresForSeg []featureForSeg

	for _, feature := range features {
		var ls orb.LineString
		switch g := feature.Geometry.(type) {
		case orb.LineString:
			ls = g
		case orb.MultiLineString:
			// flatten by concatenating all parts
			for _, sub := range g {
				ls = append(ls, sub...)
			}
		default:
			return fmt.Errorf("unknown geometry type: %T", g)
		}
		if len(ls) == 0 {
			continue
		}

		// Update global bounding box using every coordinate.
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

		// Use the first coordinate as the representative.
		repLon, repLat := ls[0][0], ls[0][1]

		title := feature.Properties.MustString("title", "")
		priority := uint8(feature.Properties.MustInt("priority", 0))
		featuresForSeg = append(featuresForSeg, featureForSeg{
			data: featureData{
				title:    title,
				priority: priority,
				coords:   ls,
			},
			repLon: repLon,
			repLat: repLat,
		})
	}

	// 2. Define a fixed grid for segmentation.
	const cols = 8
	const rows = 4

	// We'll assign each feature to a grid cell using its representative coordinate.
	// Use a simple key struct for grid cells.
	type cellKey struct {
		row, col int
	}
	segmentsMap := make(map[cellKey][]featureData)
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

	// Build a slice of segments (only non‑empty grid cells will be output).
	type segment struct {
		row, col int
		features []featureData
	}
	var segments []segment
	// We iterate over the grid in row‑major order.
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

	// 3. Write the file header.
	// We'll write the number of segments and then the global base.
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(segments))); err != nil {
		return err
	}
	// Use globalMinLon and globalMinLat as the base.
	if err := binary.Write(writer, binary.LittleEndian, globalMinLon); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, globalMinLat); err != nil {
		return err
	}

	// Precompute grid cell size.
	cellWidth := (globalMaxLon - globalMinLon) / float64(cols)
	cellHeight := (globalMaxLat - globalMinLat) / float64(rows)

	// 4. For each segment, write the segment header and its features.
	for _, seg := range segments {
		// Compute grid cell (segment) bounds.
		segMinLon := globalMinLon + float64(seg.col)*cellWidth
		segMinLat := globalMinLat + float64(seg.row)*cellHeight
		segMaxLon := segMinLon + cellWidth
		segMaxLat := segMinLat + cellHeight

		// Write the segment bounding box as delta values (relative to global base) multiplied by 1e6.
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

		// Write the number of features in this segment.
		if err := binary.Write(writer, binary.LittleEndian, uint32(len(seg.features))); err != nil {
			return err
		}

		// Write each feature.
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

			// Write the number of coordinates.
			if len(f.coords) > math.MaxUint16 {
				return fmt.Errorf("too many coordinates in feature: %d exceeds uint16 capacity", len(f.coords))
			}
			if err := binary.Write(writer, binary.LittleEndian, uint16(len(f.coords))); err != nil {
				return err
			}

			// Write each coordinate as a delta from the global base.
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

func download(ctx context.Context) (io.ReadCloser, error) {
	const activeTravelwaysDownloadURL = `https://hub.arcgis.com/api/download/v1/items/a3631c7664ef4ecb93afb1ea4c12022b/geojson?redirect=false&layers=0&spatialRefId=4326`

	deadline := time.Now().Add(5 * time.Minute)

	var resultURL string
	for time.Now().Before(deadline) {
		u, err := func() (string, error) {
			req, err := http.NewRequestWithContext(ctx, "GET", activeTravelwaysDownloadURL, nil)
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
			log.Println("waiting")
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
