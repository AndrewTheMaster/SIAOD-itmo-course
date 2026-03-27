package kdtree

import (
	"sort"

	"siaod-hw2/internal/geo"
	"siaod-hw2/internal/geoindex"
)

type node struct {
	point       geoindex.Point
	left, right *node
}

// KDTree — реализация интерфейса Searcher на основе k-d дерева.
type KDTree struct {
	root  *node
	count int
}

// New создаёт пустое k-d дерево.
func New() *KDTree { return &KDTree{} }

// Insert добавляет точку в дерево (онлайн-вставка, O(log N) среднее).
func (t *KDTree) Insert(p geoindex.Point) {
	t.root = insertNode(t.root, p, 0)
	t.count++
}

// BuildBalanced строит сбалансированное k-d дерево из набора точек за O(N log N).
// Используйте вместо последовательных Insert, если все точки известны заранее.
func (t *KDTree) BuildBalanced(points []geoindex.Point) {
	pts := make([]geoindex.Point, len(points))
	copy(pts, points)
	t.root = buildBalanced(pts, 0)
	t.count = len(points)
}

// FindNearby возвращает все точки в радиусе radiusKm вокруг (lat, lng),
// отсортированные по расстоянию.
func (t *KDTree) FindNearby(lat, lng, radiusKm float64) []geoindex.Result {
	var results []geoindex.Result
	searchRange(t.root, lat, lng, radiusKm, 0, &results)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})
	return results
}

// FindKNearest возвращает k ближайших точек.
func (t *KDTree) FindKNearest(lat, lng float64, k int) []geoindex.Result {
	if k <= 0 {
		return nil
	}
	heap := &maxHeap{}
	knnSearch(t.root, lat, lng, k, 0, heap)
	results := make([]geoindex.Result, len(heap.items))
	for i, it := range heap.items {
		results[i] = it
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})
	return results
}

// Count возвращает число точек в дереве.
func (t *KDTree) Count() int { return t.count }

func insertNode(n *node, p geoindex.Point, depth int) *node {
	if n == nil {
		return &node{point: p}
	}
	if axis(depth) == 0 {
		if p.Lat < n.point.Lat {
			n.left = insertNode(n.left, p, depth+1)
		} else {
			n.right = insertNode(n.right, p, depth+1)
		}
	} else {
		if p.Lng < n.point.Lng {
			n.left = insertNode(n.left, p, depth+1)
		} else {
			n.right = insertNode(n.right, p, depth+1)
		}
	}
	return n
}

func buildBalanced(pts []geoindex.Point, depth int) *node {
	if len(pts) == 0 {
		return nil
	}
	ax := axis(depth)
	if ax == 0 {
		sort.Slice(pts, func(i, j int) bool { return pts[i].Lat < pts[j].Lat })
	} else {
		sort.Slice(pts, func(i, j int) bool { return pts[i].Lng < pts[j].Lng })
	}
	mid := len(pts) / 2
	n := &node{point: pts[mid]}
	n.left = buildBalanced(pts[:mid], depth+1)
	n.right = buildBalanced(pts[mid+1:], depth+1)
	return n
}

func searchRange(n *node, lat, lng, radiusKm float64, depth int, out *[]geoindex.Result) {
	if n == nil {
		return
	}
	d := geo.DistanceKm(lat, lng, n.point.Lat, n.point.Lng)
	if d <= radiusKm {
		*out = append(*out, geoindex.Result{Point: n.point, Distance: d})
	}

	var planeDist float64
	if axis(depth) == 0 {
		planeDist = absDegToKm(lat - n.point.Lat)
	} else {
		planeDist = absDegToKmLng(lng-n.point.Lng, lat)
	}

	var near, far *node
	if (axis(depth) == 0 && lat < n.point.Lat) || (axis(depth) == 1 && lng < n.point.Lng) {
		near, far = n.left, n.right
	} else {
		near, far = n.right, n.left
	}

	searchRange(near, lat, lng, radiusKm, depth+1, out)
	if planeDist <= radiusKm {
		searchRange(far, lat, lng, radiusKm, depth+1, out)
	}
}

type maxHeap struct {
	items []geoindex.Result
	k     int
}

func (h *maxHeap) maxDist() float64 {
	if len(h.items) < h.k {
		return 1e18
	}
	return h.items[0].Distance
}

func (h *maxHeap) push(r geoindex.Result) {
	if len(h.items) < h.k {
		h.items = append(h.items, r)
		heapifyUp(h.items, len(h.items)-1)
	} else if r.Distance < h.items[0].Distance {
		h.items[0] = r
		heapifyDown(h.items, 0)
	}
}

func knnSearch(n *node, lat, lng float64, k, depth int, h *maxHeap) {
	if n == nil {
		return
	}
	h.k = k
	d := geo.DistanceKm(lat, lng, n.point.Lat, n.point.Lng)
	h.push(geoindex.Result{Point: n.point, Distance: d})

	var planeDist float64
	if axis(depth) == 0 {
		planeDist = absDegToKm(lat - n.point.Lat)
	} else {
		planeDist = absDegToKmLng(lng-n.point.Lng, lat)
	}

	var near, far *node
	if (axis(depth) == 0 && lat < n.point.Lat) || (axis(depth) == 1 && lng < n.point.Lng) {
		near, far = n.left, n.right
	} else {
		near, far = n.right, n.left
	}

	knnSearch(near, lat, lng, k, depth+1, h)
	if planeDist < h.maxDist() {
		knnSearch(far, lat, lng, k, depth+1, h)
	}
}

func heapifyUp(items []geoindex.Result, i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if items[i].Distance > items[parent].Distance {
			items[i], items[parent] = items[parent], items[i]
			i = parent
		} else {
			break
		}
	}
}

func heapifyDown(items []geoindex.Result, i int) {
	n := len(items)
	for {
		largest := i
		l, r := 2*i+1, 2*i+2
		if l < n && items[l].Distance > items[largest].Distance {
			largest = l
		}
		if r < n && items[r].Distance > items[largest].Distance {
			largest = r
		}
		if largest == i {
			break
		}
		items[i], items[largest] = items[largest], items[i]
		i = largest
	}
}

func axis(depth int) int { return depth % 2 }

func absDegToKm(dLat float64) float64 {
	if dLat < 0 {
		dLat = -dLat
	}
	return dLat * 111.32
}

func absDegToKmLng(dLng, lat float64) float64 {
	if dLng < 0 {
		dLng = -dLng
	}
	const toRad = 3.14159265358979323846 / 180.0
	cosLat := cosApprox(lat * toRad)
	return dLng * 111.32 * cosLat
}

func cosApprox(x float64) float64 {
	x2 := x * x
	return 1 - x2/2 + x2*x2/24 - x2*x2*x2/720
}
