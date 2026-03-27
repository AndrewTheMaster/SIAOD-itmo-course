package brute

import (
	"sort"

	"siaod-hw2/internal/geo"
	"siaod-hw2/internal/geoindex"
)

type Scanner struct {
	points []geoindex.Point
}

func New() *Scanner { return &Scanner{} }

func (s *Scanner) Insert(p geoindex.Point) {
	s.points = append(s.points, p)
}

func (s *Scanner) FindNearby(lat, lng, radiusKm float64) []geoindex.Result {
	var results []geoindex.Result
	for i := range s.points {
		d := geo.DistanceKm(lat, lng, s.points[i].Lat, s.points[i].Lng)
		if d <= radiusKm {
			results = append(results, geoindex.Result{Point: s.points[i], Distance: d})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})
	return results
}

func (s *Scanner) FindKNearest(lat, lng float64, k int) []geoindex.Result {
	results := make([]geoindex.Result, len(s.points))
	for i := range s.points {
		results[i] = geoindex.Result{
			Point:    s.points[i],
			Distance: geo.DistanceKm(lat, lng, s.points[i].Lat, s.points[i].Lng),
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})
	if k < len(results) {
		return results[:k]
	}
	return results
}

func (s *Scanner) Count() int { return len(s.points) }
