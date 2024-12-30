package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

const activeTravelwaysDownloadURL = `https://hub.arcgis.com/api/download/v1/items/a3631c7664ef4ecb93afb1ea4c12022b/geojson?redirect=false&layers=0&spatialRefId=4326`

func main() {
	const peninsula = `
{"type":"Polygon","coordinates":[[[-63.631439208984375,44.66901911969433],[-63.63264083862304,44.66608907552703],[-63.61307144165039,44.63836841571474],[-63.610496520996094,44.63726908564141],[-63.60157012939453,44.633238030512764],[-63.595733642578125,44.63091699296066],[-63.59212875366211,44.62822935982843],[-63.584232330322266,44.625419428488065],[-63.57187271118165,44.61698881833206],[-63.56672286987305,44.61564469487975],[-63.555564880371094,44.62260936112512],[-63.561058044433594,44.63763553131337],[-63.567581176757805,44.647650816904346],[-63.5727310180664,44.65412319135287],[-63.57891082763672,44.659495934622264],[-63.587665557861335,44.66401353796326],[-63.59521865844727,44.67146071002704],[-63.6086082458496,44.67719804244711],[-63.62251281738282,44.680005466210474],[-63.631439208984375,44.66901911969433]]]}
`

	var peninsulaPolygon geojson.Polygon
	if err := json.Unmarshal([]byte(peninsula), &peninsulaPolygon); err != nil {
		log.Fatal(err)
	}

	endTimeRaw := os.Args[1]
	if endTimeRaw == "" {
		log.Fatal("missing end time")
	}
	endTime, err := time.ParseInLocation("2006-01-02T15:04:05", endTimeRaw, time.Local)
	if err != nil {
		log.Fatal(err)
	}

	type priority struct {
		Number   int
		Timeline time.Duration
		Deadline time.Time
	}
	priorities := map[string]priority{
		"1": {1, 12 * time.Hour, endTime.Add(12 * time.Hour)},
		"2": {2, 18 * time.Hour, endTime.Add(18 * time.Hour)},
		"3": {3, 36 * time.Hour, endTime.Add(36 * time.Hour)},
	}

	resp, err := http.Get(activeTravelwaysDownloadURL)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	var downloadBody struct {
		ResultURL string `json:"resultUrl"`
	}
	if err := json.Unmarshal(b, &downloadBody); err != nil {
		log.Fatal(err)
	}

	resp, err = http.Get(downloadBody.ResultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	b, err = io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	fc := geojson.NewFeatureCollection()
	if err := json.Unmarshal(b, &fc); err != nil {
		log.Fatal(err)
	}

	fc.Features = slices.DeleteFunc(fc.Features, func(f *geojson.Feature) bool {
		return f.Properties["WINT_LOS"] == nil
	})
	for _, f := range fc.Features {
		props := f.Properties
		f.Properties = make(geojson.Properties)

		priorityNum := strings.TrimPrefix(props.MustString("WINT_LOS", ""), "PRI")
		p, ok := priorities[priorityNum]
		if !ok {
			log.Fatalf("unknown WINT_LOS: %s", props.MustString("WINT_LOS", ""))
		}

		f.Properties["title"] = props["LOCATION"]
		f.Properties["priority"] = p.Number
		f.Properties["timeline"] = fmt.Sprintf("%d hours", int(p.Timeline.Hours()))
		f.Properties["deadline"] = p.Deadline.Format("Mon 3:04 PM")
	}

	var out bytes.Buffer
	if err := encodeFeatures(fc.Features, &out); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile("features.bin", out.Bytes(), 0644); err != nil {
		log.Fatal(err)
	}

	b, err = json.Marshal(priorities)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.WriteFile("priorities.json", b, 0644); err != nil {
		log.Fatal(err)
	}
}

func encodeFeatures(features []*geojson.Feature, writer io.Writer) error {
	if len(features) == 0 {
		return fmt.Errorf("no features")
	}
	if len(features) > math.MaxUint32 {
		return fmt.Errorf("too many features: %d exceeds uint32 capacity", len(features))
	}

	// Write the number of features
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(features))); err != nil {
		return err
	}

	var baseLon, baseLat float64
	first := features[0]
	switch g := first.Geometry.(type) {
	case orb.LineString:
		baseLon, baseLat = g[0][0], g[0][1]
	case orb.MultiLineString:
		baseLon, baseLat = g[0][0][0], g[0][0][1]
	default:
		return fmt.Errorf("unknown geometry type: %T", g)
	}

	if err := binary.Write(writer, binary.LittleEndian, float64(baseLon)); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, float64(baseLat)); err != nil {
		return err
	}

	for _, feature := range features {
		// Write title length and title
		titleBytes := []byte(feature.Properties.MustString("title", ""))
		if len(titleBytes) > 255 {
			return fmt.Errorf("title too long: %s exceeds 255 bytes", titleBytes)
		}
		if err := binary.Write(writer, binary.LittleEndian, uint8(len(titleBytes))); err != nil {
			return err
		}
		if _, err := writer.Write(titleBytes); err != nil {
			return err
		}

		// Write priority
		if err := binary.Write(writer, binary.LittleEndian, uint8(feature.Properties.MustInt("priority", 0))); err != nil {
			return err
		}

		var ls orb.LineString
		switch g := feature.Geometry.(type) {
		case orb.LineString:
			ls = g
		case orb.MultiLineString:
			for _, gLS := range g {
				ls = append(ls, gLS...)
			}
		default:
			return fmt.Errorf("unknown geometry type: %T", g)
		}

		if len(ls) > math.MaxUint16 {
			return fmt.Errorf("too many coordinates: %d exceeds uint16 capacity", len(ls))
		}

		// Write number of coordinates
		if err := binary.Write(writer, binary.LittleEndian, uint16(len(ls))); err != nil {
			return err
		}

		for _, coord := range ls {
			deltaLon := int32(math.Round((coord[0] - baseLon) * 1000000))
			if err := binary.Write(writer, binary.LittleEndian, deltaLon); err != nil {
				return err
			}
			deltaLat := int32(math.Round((coord[1] - baseLat) * 1000000))
			if err := binary.Write(writer, binary.LittleEndian, deltaLat); err != nil {
				return err
			}
		}
	}

	return nil
}
