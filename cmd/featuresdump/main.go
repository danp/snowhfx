package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/danp/snowhfx/internal/featuresbin"
)

type outputFeature struct {
	Index         int           `json:"index"`
	StableID      string        `json:"stable_id,omitempty"`
	Title         string        `json:"title"`
	Priority      uint8         `json:"priority"`
	SourceDataset uint8         `json:"source_dataset"`
	RouteID       uint16        `json:"route_id"`
	Coords        [][]float64   `json:"coords"`
	Route         *routePayload `json:"route,omitempty"`
}

type routePayload struct {
	Maint string `json:"maint"`
	Route string `json:"route"`
}

func main() {
	var (
		path       string
		pretty     bool
		withRoutes bool
	)
	flag.StringVar(&path, "in", "", "path to features bin")
	flag.BoolVar(&pretty, "pretty", false, "pretty-print json")
	flag.BoolVar(&withRoutes, "with-routes", true, "include route entries in output")
	flag.Parse()

	if path == "" {
		log.Fatal("-in is required")
	}

	features, routes, header, err := featuresbin.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	out := struct {
		Header   featuresbin.Header       `json:"header"`
		Routes   []featuresbin.RouteEntry `json:"routes,omitempty"`
		Features []outputFeature          `json:"features"`
	}{
		Header: header,
	}
	if withRoutes {
		out.Routes = routes
	}

	out.Features = make([]outputFeature, 0, len(features))
	for i, feat := range features {
		var route *routePayload
		if withRoutes && feat.RouteID > 0 {
			idx := int(feat.RouteID - 1)
			if idx >= 0 && idx < len(routes) {
				entry := routes[idx]
				route = &routePayload{Maint: entry.Maint, Route: entry.Route}
			}
		}
		out.Features = append(out.Features, outputFeature{
			Index:         i,
			StableID:      feat.StableID,
			Title:         feat.Title,
			Priority:      feat.Priority,
			SourceDataset: feat.SourceDataset,
			RouteID:       feat.RouteID,
			Coords:        feat.Coords,
			Route:         route,
		})
	}

	enc := json.NewEncoder(os.Stdout)
	if pretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(out); err != nil {
		log.Fatal(err)
	}
}
