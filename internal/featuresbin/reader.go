package featuresbin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Feature struct {
	Title         string
	Priority      uint8
	SourceDataset uint8
	RouteID       uint16
	Coords        [][]float64
}

type Header struct {
	SegmentCount uint32
	GlobalMinLon float64
	GlobalMinLat float64
	RouteCount   uint16
}

type RouteEntry struct {
	Maint string
	Route string
}

type Reader struct {
	r         *bytes.Reader
	header    Header
	routes    []RouteEntry
	segCount  uint32
	segIndex  uint32
	featIndex uint32
	featCount uint32
	globalLon float64
	globalLat float64
}

func ReadFile(path string) ([]Feature, []RouteEntry, Header, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, Header{}, err
	}
	return Read(bytes.NewReader(data))
}

func Read(r io.Reader) ([]Feature, []RouteEntry, Header, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, Header{}, err
	}
	reader := NewReader(bytes.NewReader(data))
	if err := reader.readHeader(); err != nil {
		return nil, nil, Header{}, err
	}
	if err := reader.readRoutes(); err != nil {
		return nil, nil, Header{}, err
	}
	var features []Feature
	for {
		feat, ok, err := reader.NextFeature()
		if err != nil {
			return nil, nil, Header{}, err
		}
		if !ok {
			break
		}
		features = append(features, feat)
	}
	return features, reader.routes, reader.header, nil
}

func NewReader(r *bytes.Reader) *Reader {
	return &Reader{r: r}
}

func (r *Reader) readHeader() error {
	var segCount uint32
	if err := binary.Read(r.r, binary.LittleEndian, &segCount); err != nil {
		return err
	}
	var globalMinLon, globalMinLat float64
	if err := binary.Read(r.r, binary.LittleEndian, &globalMinLon); err != nil {
		return err
	}
	if err := binary.Read(r.r, binary.LittleEndian, &globalMinLat); err != nil {
		return err
	}
	var routeCount uint16
	if err := binary.Read(r.r, binary.LittleEndian, &routeCount); err != nil {
		return err
	}
	r.header = Header{
		SegmentCount: segCount,
		GlobalMinLon: globalMinLon,
		GlobalMinLat: globalMinLat,
		RouteCount:   routeCount,
	}
	r.segCount = segCount
	r.globalLon = globalMinLon
	r.globalLat = globalMinLat
	return nil
}

func (r *Reader) readRoutes() error {
	r.routes = make([]RouteEntry, 0, r.header.RouteCount)
	for i := uint16(0); i < r.header.RouteCount; i++ {
		var maintLen uint8
		if err := binary.Read(r.r, binary.LittleEndian, &maintLen); err != nil {
			return err
		}
		maint := make([]byte, maintLen)
		if _, err := io.ReadFull(r.r, maint); err != nil {
			return err
		}
		var routeLen uint8
		if err := binary.Read(r.r, binary.LittleEndian, &routeLen); err != nil {
			return err
		}
		route := make([]byte, routeLen)
		if _, err := io.ReadFull(r.r, route); err != nil {
			return err
		}
		r.routes = append(r.routes, RouteEntry{Maint: string(maint), Route: string(route)})
	}
	return nil
}

func (r *Reader) NextFeature() (Feature, bool, error) {
	for r.segIndex < r.segCount {
		if r.featIndex == r.featCount {
			if err := r.readSegmentHeader(); err != nil {
				return Feature{}, false, err
			}
			if r.featCount == 0 {
				r.segIndex++
				continue
			}
		}
		feat, err := r.readFeature()
		if err != nil {
			return Feature{}, false, err
		}
		r.featIndex++
		return feat, true, nil
	}
	return Feature{}, false, nil
}

func (r *Reader) readSegmentHeader() error {
	var deltaMinLon, deltaMinLat, deltaMaxLon, deltaMaxLat int32
	if err := binary.Read(r.r, binary.LittleEndian, &deltaMinLon); err != nil {
		return err
	}
	if err := binary.Read(r.r, binary.LittleEndian, &deltaMinLat); err != nil {
		return err
	}
	if err := binary.Read(r.r, binary.LittleEndian, &deltaMaxLon); err != nil {
		return err
	}
	if err := binary.Read(r.r, binary.LittleEndian, &deltaMaxLat); err != nil {
		return err
	}
	_ = deltaMinLon
	_ = deltaMinLat
	_ = deltaMaxLon
	_ = deltaMaxLat
	var featCount uint32
	if err := binary.Read(r.r, binary.LittleEndian, &featCount); err != nil {
		return err
	}
	r.featCount = featCount
	r.featIndex = 0
	r.segIndex++
	return nil
}

func (r *Reader) readFeature() (Feature, error) {
	var titleLen uint8
	if err := binary.Read(r.r, binary.LittleEndian, &titleLen); err != nil {
		return Feature{}, err
	}
	titleBytes := make([]byte, titleLen)
	if _, err := io.ReadFull(r.r, titleBytes); err != nil {
		return Feature{}, err
	}
	var priority uint8
	if err := binary.Read(r.r, binary.LittleEndian, &priority); err != nil {
		return Feature{}, err
	}
	var sourceDataset uint8
	if err := binary.Read(r.r, binary.LittleEndian, &sourceDataset); err != nil {
		return Feature{}, err
	}
	var routeID uint16
	if err := binary.Read(r.r, binary.LittleEndian, &routeID); err != nil {
		return Feature{}, err
	}
	var coordCount uint16
	if err := binary.Read(r.r, binary.LittleEndian, &coordCount); err != nil {
		return Feature{}, err
	}
	coords := make([][]float64, 0, coordCount)
	for i := uint16(0); i < coordCount; i++ {
		var dLon, dLat int32
		if err := binary.Read(r.r, binary.LittleEndian, &dLon); err != nil {
			return Feature{}, err
		}
		if err := binary.Read(r.r, binary.LittleEndian, &dLat); err != nil {
			return Feature{}, err
		}
		lon := r.globalLon + float64(dLon)/1000000
		lat := r.globalLat + float64(dLat)/1000000
		coords = append(coords, []float64{lon, lat})
	}
	return Feature{
		Title:         string(titleBytes),
		Priority:      priority,
		SourceDataset: sourceDataset,
		RouteID:       routeID,
		Coords:        coords,
	}, nil
}

func (h Header) String() string {
	return fmt.Sprintf("segments=%d global_min=(%.6f,%.6f) routes=%d", h.SegmentCount, h.GlobalMinLon, h.GlobalMinLat, h.RouteCount)
}
