package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/planar"
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
		Timeline time.Duration
		Deadline time.Time
	}
	priorities := map[string]priority{
		"1": {12 * time.Hour, endTime.Add(12 * time.Hour)},
		"2": {18 * time.Hour, endTime.Add(18 * time.Hour)},
		"3": {36 * time.Hour, endTime.Add(36 * time.Hour)},
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
		if f.Properties["WINT_LOS"] == nil {
			return true
		}
		if !isPolygonInsidePolygon(peninsulaPolygon.Geometry().(orb.Polygon), f.Geometry) {
			return true
		}
		return false
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
		f.Properties["priority"] = priorityNum
		f.Properties["timeline"] = fmt.Sprintf("%d hours", int(p.Timeline.Hours()))
		f.Properties["deadline"] = p.Deadline.Format("Mon 3:04 PM")
	}

	b, err = json.Marshal(fc)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.WriteFile("data.geojson", b, 0644); err != nil {
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

func isPolygonInsidePolygon(outer orb.Polygon, inner orb.Geometry) bool {
	switch inner := inner.(type) {
	case orb.Polygon:
		// Check if all points of the inner polygon are inside the outer polygon
		for _, ring := range inner {
			for _, point := range ring {
				if !planar.PolygonContains(outer, point) {
					return false // If any point is outside, it's not wholly contained
				}
			}
		}
	case orb.MultiPolygon:
		// Check if all points of the inner multipolygon are inside the outer polygon
		for _, p := range inner {
			if !isPolygonInsidePolygon(outer, p) {
				return false
			}
		}
	case orb.LineString:
		// Check if all points of the inner line string are inside the outer polygon
		for _, point := range inner {
			if !planar.PolygonContains(outer, point) {
				return false
			}
		}
	case orb.MultiLineString:
		// Check if all points of the inner multi line string are inside the outer polygon
		for _, ls := range inner {
			for _, point := range ls {
				if !planar.PolygonContains(outer, point) {
					return false
				}
			}
		}
	default:
		log.Fatalf("unknown geometry type: %T", inner)
	}
	return true
}
