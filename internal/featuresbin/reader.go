package featuresbin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	magic     = "SHFX"
	versionV4 = uint8(4)
)

type Feature struct {
	StableID      string
	Title         string
	Priority      uint8
	SourceDataset uint8
	RouteID       uint16
	Coords        [][]float64
}

type Header struct {
	FormatVersion  uint8
	SegmentCount   uint32
	GlobalMinLon   float64
	GlobalMinLat   float64
	RouteCount     uint16
	NamePieceCount uint16
}

type RouteEntry struct {
	Maint string
	Route string
}

type Reader struct {
	r          *bytes.Reader
	header     Header
	routes     []RouteEntry
	namePieces []string
	segCount   uint32
	segIndex   uint32
	featIndex  uint32
	featCount  uint32
	globalLon  float64
	globalLat  float64
}

func decodeZigZag(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}

func (r *Reader) readUvarint() (uint64, error) {
	return binary.ReadUvarint(r.r)
}

func (r *Reader) readVarintZigZag() (int64, error) {
	u, err := r.readUvarint()
	if err != nil {
		return 0, err
	}
	return decodeZigZag(u), nil
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
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(r.r, prefix); err != nil {
		return err
	}
	if string(prefix) != magic {
		return fmt.Errorf("invalid magic: got %q want %q", string(prefix), magic)
	}
	var formatVersion uint8
	if err := binary.Read(r.r, binary.LittleEndian, &formatVersion); err != nil {
		return err
	}
	if formatVersion != versionV4 {
		return fmt.Errorf("unsupported format version: %d", formatVersion)
	}

	segCount64, err := r.readUvarint()
	if err != nil {
		return err
	}
	if segCount64 > uint64(^uint32(0)) {
		return fmt.Errorf("segment count overflow: %d", segCount64)
	}
	segCount := uint32(segCount64)
	var globalMinLon, globalMinLat float64
	if err := binary.Read(r.r, binary.LittleEndian, &globalMinLon); err != nil {
		return err
	}
	if err := binary.Read(r.r, binary.LittleEndian, &globalMinLat); err != nil {
		return err
	}
	routeCount64, err := r.readUvarint()
	if err != nil {
		return err
	}
	if routeCount64 > uint64(^uint16(0)) {
		return fmt.Errorf("route count overflow: %d", routeCount64)
	}
	routeCount := uint16(routeCount64)
	namePieceCount64, err := r.readUvarint()
	if err != nil {
		return err
	}
	if namePieceCount64 > uint64(^uint16(0)) {
		return fmt.Errorf("name piece count overflow: %d", namePieceCount64)
	}
	namePieceCount := uint16(namePieceCount64)
	r.header = Header{
		FormatVersion:  formatVersion,
		SegmentCount:   segCount,
		GlobalMinLon:   globalMinLon,
		GlobalMinLat:   globalMinLat,
		RouteCount:     routeCount,
		NamePieceCount: namePieceCount,
	}
	r.segCount = segCount
	r.globalLon = globalMinLon
	r.globalLat = globalMinLat
	return nil
}

func (r *Reader) readRoutes() error {
	r.namePieces = make([]string, 0, r.header.NamePieceCount)
	for i := uint16(0); i < r.header.NamePieceCount; i++ {
		pieceLen64, err := r.readUvarint()
		if err != nil {
			return err
		}
		if pieceLen64 > uint64(^uint8(0)) {
			return fmt.Errorf("piece length overflow: %d", pieceLen64)
		}
		piece := make([]byte, uint8(pieceLen64))
		if _, err := io.ReadFull(r.r, piece); err != nil {
			return err
		}
		r.namePieces = append(r.namePieces, string(piece))
	}
	r.routes = make([]RouteEntry, 0, r.header.RouteCount)
	for i := uint16(0); i < r.header.RouteCount; i++ {
		maintCount64, err := r.readUvarint()
		if err != nil {
			return err
		}
		maintPieces := make([]string, 0, maintCount64)
		for j := uint64(0); j < maintCount64; j++ {
			pieceID64, err := r.readUvarint()
			if err != nil {
				return err
			}
			if pieceID64 > uint64(^uint16(0)) {
				return fmt.Errorf("route maint piece id overflow: %d", pieceID64)
			}
			pieceID := uint16(pieceID64)
			if pieceID == 0 {
				return fmt.Errorf("invalid route maint piece id: 0")
			}
			idx := int(pieceID - 1)
			if idx < 0 || idx >= len(r.namePieces) {
				return fmt.Errorf("invalid route maint piece id: %d", pieceID)
			}
			maintPieces = append(maintPieces, r.namePieces[idx])
		}
		routeCount64, err := r.readUvarint()
		if err != nil {
			return err
		}
		routePieces := make([]string, 0, routeCount64)
		for j := uint64(0); j < routeCount64; j++ {
			pieceID64, err := r.readUvarint()
			if err != nil {
				return err
			}
			if pieceID64 > uint64(^uint16(0)) {
				return fmt.Errorf("route name piece id overflow: %d", pieceID64)
			}
			pieceID := uint16(pieceID64)
			if pieceID == 0 {
				return fmt.Errorf("invalid route name piece id: 0")
			}
			idx := int(pieceID - 1)
			if idx < 0 || idx >= len(r.namePieces) {
				return fmt.Errorf("invalid route name piece id: %d", pieceID)
			}
			routePieces = append(routePieces, r.namePieces[idx])
		}
		r.routes = append(r.routes, RouteEntry{
			Maint: strings.Join(maintPieces, " "),
			Route: strings.Join(routePieces, " "),
		})
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
	if _, err := r.readVarintZigZag(); err != nil {
		return err
	}
	if _, err := r.readVarintZigZag(); err != nil {
		return err
	}
	if _, err := r.readVarintZigZag(); err != nil {
		return err
	}
	if _, err := r.readVarintZigZag(); err != nil {
		return err
	}
	featCount64, err := r.readUvarint()
	if err != nil {
		return err
	}
	if featCount64 > uint64(^uint32(0)) {
		return fmt.Errorf("feature count overflow: %d", featCount64)
	}
	featCount := uint32(featCount64)
	r.featCount = featCount
	r.featIndex = 0
	r.segIndex++
	return nil
}

func (r *Reader) readFeature() (Feature, error) {
	stablePieceCount64, err := r.readUvarint()
	if err != nil {
		return Feature{}, err
	}
	if stablePieceCount64 > uint64(^uint8(0)) {
		return Feature{}, fmt.Errorf("stable piece count overflow: %d", stablePieceCount64)
	}
	stablePieceCount := uint8(stablePieceCount64)
	var stableBuilder strings.Builder
	for i := uint8(0); i < stablePieceCount; i++ {
		pieceID64, err := r.readUvarint()
		if err != nil {
			return Feature{}, err
		}
		if pieceID64 > uint64(^uint16(0)) {
			return Feature{}, fmt.Errorf("stable id piece id overflow: %d", pieceID64)
		}
		pieceID := uint16(pieceID64)
		if pieceID == 0 {
			return Feature{}, fmt.Errorf("invalid stable id piece id: 0")
		}
		idx := int(pieceID - 1)
		if idx < 0 || idx >= len(r.namePieces) {
			return Feature{}, fmt.Errorf("invalid stable id piece id: %d", pieceID)
		}
		stableBuilder.WriteString(r.namePieces[idx])
	}
	stableID := stableBuilder.String()
	pieceCount64, err := r.readUvarint()
	if err != nil {
		return Feature{}, err
	}
	if pieceCount64 > uint64(^uint8(0)) {
		return Feature{}, fmt.Errorf("title piece count overflow: %d", pieceCount64)
	}
	pieceCount := uint8(pieceCount64)
	titlePieces := make([]string, 0, pieceCount)
	for i := uint8(0); i < pieceCount; i++ {
		pieceID64, err := r.readUvarint()
		if err != nil {
			return Feature{}, err
		}
		if pieceID64 > uint64(^uint16(0)) {
			return Feature{}, fmt.Errorf("title piece id overflow: %d", pieceID64)
		}
		pieceID := uint16(pieceID64)
		if pieceID == 0 {
			return Feature{}, fmt.Errorf("invalid title piece id: 0")
		}
		idx := int(pieceID - 1)
		if idx < 0 || idx >= len(r.namePieces) {
			return Feature{}, fmt.Errorf("invalid title piece id: %d", pieceID)
		}
		titlePieces = append(titlePieces, r.namePieces[idx])
	}
	title := strings.Join(titlePieces, " ")
	priority64, err := r.readUvarint()
	if err != nil {
		return Feature{}, err
	}
	if priority64 > uint64(^uint8(0)) {
		return Feature{}, fmt.Errorf("priority overflow: %d", priority64)
	}
	priority := uint8(priority64)
	sourceDataset64, err := r.readUvarint()
	if err != nil {
		return Feature{}, err
	}
	if sourceDataset64 > uint64(^uint8(0)) {
		return Feature{}, fmt.Errorf("source dataset overflow: %d", sourceDataset64)
	}
	sourceDataset := uint8(sourceDataset64)
	routeID64, err := r.readUvarint()
	if err != nil {
		return Feature{}, err
	}
	if routeID64 > uint64(^uint16(0)) {
		return Feature{}, fmt.Errorf("route id overflow: %d", routeID64)
	}
	routeID := uint16(routeID64)
	coordCount64, err := r.readUvarint()
	if err != nil {
		return Feature{}, err
	}
	if coordCount64 > uint64(^uint16(0)) {
		return Feature{}, fmt.Errorf("coord count overflow: %d", coordCount64)
	}
	coordCount := uint16(coordCount64)
	coords := make([][]float64, 0, coordCount)
	absLon := int32(0)
	absLat := int32(0)
	for i := uint16(0); i < coordCount; i++ {
		dLon64, err := r.readVarintZigZag()
		if err != nil {
			return Feature{}, err
		}
		dLat64, err := r.readVarintZigZag()
		if err != nil {
			return Feature{}, err
		}
		dLon := int32(dLon64)
		dLat := int32(dLat64)
		if int64(dLon) != dLon64 || int64(dLat) != dLat64 {
			return Feature{}, fmt.Errorf("coordinate delta overflow: lon=%d lat=%d", dLon64, dLat64)
		}
		if i == 0 {
			absLon = dLon
			absLat = dLat
		} else {
			absLon += dLon
			absLat += dLat
		}
		lon := r.globalLon + float64(absLon)/1000000
		lat := r.globalLat + float64(absLat)/1000000
		coords = append(coords, []float64{lon, lat})
	}
	return Feature{
		StableID:      stableID,
		Title:         title,
		Priority:      priority,
		SourceDataset: sourceDataset,
		RouteID:       routeID,
		Coords:        coords,
	}, nil
}

func (h Header) String() string {
	return fmt.Sprintf("v%d segments=%d global_min=(%.6f,%.6f) routes=%d name_pieces=%d", h.FormatVersion, h.SegmentCount, h.GlobalMinLon, h.GlobalMinLat, h.RouteCount, h.NamePieceCount)
}
