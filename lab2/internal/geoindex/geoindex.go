package geoindex

import (
	"sort"

	"siaod-hw2/internal/geo"
)

// Point — географическая точка с произвольными метаданными.
type Point struct {
	ID   string
	Lat  float64
	Lng  float64
	Data []byte
}

// Result — результат поиска: точка + расстояние до запросной координаты.
type Result struct {
	Point    Point
	Distance float64 // км
}

// Searcher — общий интерфейс для всех реализаций геопоиска.
// Используется в бенчмарках для сравнения алгоритмов.
type Searcher interface {
	// Insert добавляет точку в индекс.
	Insert(p Point)
	// FindNearby возвращает все точки в радиусе radiusKm вокруг (lat,lng),
	// отсортированные по расстоянию.
	FindNearby(lat, lng, radiusKm float64) []Result
	// Count возвращает число точек в индексе.
	Count() int
}

// Index — геохэш-индекс.
type Index struct {
	precision int
	cells     map[string][]Point
	count     int
}

// New создаёт новый индекс с заданной точностью геохэша (1–12).
func New(precision int) *Index {
	if precision < 1 {
		precision = 1
	}
	if precision > 12 {
		precision = 12
	}
	return &Index{
		precision: precision,
		cells:     make(map[string][]Point),
	}
}

// Insert добавляет точку в индекс.
func (idx *Index) Insert(p Point) {
	h := geo.Encode(p.Lat, p.Lng, idx.precision)
	idx.cells[h] = append(idx.cells[h], p)
	idx.count++
}

// FindNearby возвращает все точки в радиусе radiusKm, отсортированные по расстоянию.
func (idx *Index) FindNearby(lat, lng, radiusKm float64) []Result {
	cells := idx.candidateCells(lat, lng, radiusKm)

	var results []Result
	for _, cell := range cells {
		for _, p := range idx.cells[cell] {
			d := geo.DistanceKm(lat, lng, p.Lat, p.Lng)
			if d <= radiusKm {
				results = append(results, Result{Point: p, Distance: d})
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})
	return results
}

// candidateCells возвращает геохэш-ячейки через BFS, перекрывающиеся с кругом радиуса radiusKm.
func (idx *Index) candidateCells(lat, lng, radiusKm float64) []string {
	_, _, latErr, lngErr, _ := geo.Decode(geo.Encode(lat, lng, idx.precision))
	cellDiag := geo.DistanceKm(
		lat-latErr, lng-lngErr,
		lat+latErr, lng+lngErr,
	)
	cutoff := radiusKm + cellDiag

	startHash := geo.Encode(lat, lng, idx.precision)

	visited := make(map[string]struct{})
	queue := []string{startHash}
	visited[startHash] = struct{}{}
	var result []string

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		cLat, cLng, _, _, _ := geo.Decode(cur)
		if geo.DistanceKm(lat, lng, cLat, cLng) > cutoff {
			continue
		}
		result = append(result, cur)

		for _, nb := range geo.Neighbors(cur) {
			if _, ok := visited[nb]; !ok {
				visited[nb] = struct{}{}
				queue = append(queue, nb)
			}
		}
	}

	return result
}

// FindKNearest возвращает k ближайших точек через BFS-расширение по ячейкам.
func (idx *Index) FindKNearest(lat, lng float64, k int) []Result {
	if k <= 0 || idx.count == 0 {
		return nil
	}
	h := geo.Encode(lat, lng, idx.precision)
	visited := make(map[string]struct{})
	frontier := []string{h}
	var all []Result

	for len(frontier) > 0 && len(all) < k {
		nextFrontier := make(map[string]struct{})
		for _, cell := range frontier {
			if _, ok := visited[cell]; ok {
				continue
			}
			visited[cell] = struct{}{}
			for _, p := range idx.cells[cell] {
				d := geo.DistanceKm(lat, lng, p.Lat, p.Lng)
				all = append(all, Result{Point: p, Distance: d})
			}
			for _, nb := range geo.Neighbors(cell) {
				if _, ok := visited[nb]; !ok {
					nextFrontier[nb] = struct{}{}
				}
			}
		}
		frontier = frontier[:0]
		for nb := range nextFrontier {
			frontier = append(frontier, nb)
		}
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Distance < all[j].Distance
	})
	if len(all) > k {
		return all[:k]
	}
	return all
}

// Count возвращает число точек в индексе.
func (idx *Index) Count() int { return idx.count }

// Precision возвращает текущую точность геохэша.
func (idx *Index) Precision() int { return idx.precision }
