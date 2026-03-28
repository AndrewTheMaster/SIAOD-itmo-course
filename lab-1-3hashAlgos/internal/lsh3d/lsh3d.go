package lsh3d

import (
	"errors"
	"math"
	"math/rand"
)

type Point3D struct {
	X, Y, Z float64
	ID       int
}

// Candidate — кандидат-дубль с расстоянием до запрошенной точки.
type Candidate struct {
	ID       int
	Distance float64
}

type Pair struct {
	ID1, ID2 int
	Distance float64
}

// Config задаёт параметры p-stable LSH для R³.
type Config struct {
	NumTables int     // L: число независимых хэш-таблиц
	NumFuncs  int     // k: число проекций на одну таблицу (составной ключ)
	BandWidth float64 // w: ширина ячейки проекции (~радиус поиска)
}

// DefaultConfig для облаков в [0, 100)^3, дубли в радиусе ~5.
func DefaultConfig() Config {
	return Config{NumTables: 10, NumFuncs: 3, BandWidth: 5.0}
}

func (c Config) validate() error {
	if c.NumTables < 1 {
		return errors.New("lsh3d: NumTables must be ≥ 1")
	}
	if c.NumFuncs < 1 {
		return errors.New("lsh3d: NumFuncs must be ≥ 1")
	}
	if c.BandWidth <= 0 {
		return errors.New("lsh3d: BandWidth must be > 0")
	}
	return nil
}

// proj хранит один случайный линейный проектор: h(x) = floor((a·x + b) / w).
type proj struct {
	ax, ay, az float64
	b          float64
}

// Index — p-stable LSH индекс для R³.
type Index struct {
	cfg    Config
	tables []map[int64][]Point3D
	projs  [][]proj // [NumTables][NumFuncs]
	all    []Point3D
}

// NewIndex создаёт индекс; проекторы инициализируются с seed=42.
func NewIndex(cfg Config) (*Index, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewSource(42))
	tables := make([]map[int64][]Point3D, cfg.NumTables)
	projs := make([][]proj, cfg.NumTables)
	for t := range tables {
		tables[t] = make(map[int64][]Point3D)
		projs[t] = make([]proj, cfg.NumFuncs)
		for f := range projs[t] {
			projs[t][f] = proj{
				ax: rng.NormFloat64(),
				ay: rng.NormFloat64(),
				az: rng.NormFloat64(),
				b:  rng.Float64() * cfg.BandWidth,
			}
		}
	}
	return &Index{cfg: cfg, tables: tables, projs: projs}, nil
}

// compoundKey объединяет k проекций таблицы t в единый int64-ключ.
func (idx *Index) compoundKey(p Point3D, t int) int64 {
	var key int64
	for i, pr := range idx.projs[t] {
		dot := pr.ax*p.X + pr.ay*p.Y + pr.az*p.Z
		bucket := int64(math.Floor((dot + pr.b) / idx.cfg.BandWidth))
		key = key*2654435761 + bucket*int64(i+1337)
	}
	return key
}

func (idx *Index) Add(p Point3D) {
	for t := range idx.tables {
		k := idx.compoundKey(p, t)
		idx.tables[t][k] = append(idx.tables[t][k], p)
	}
	idx.all = append(idx.all, p)
}

// Query возвращает кандидатов-дублей для точки p (могут быть false positives).
func (idx *Index) Query(p Point3D) []Candidate {
	seen := make(map[int]struct{})
	var result []Candidate
	for t := range idx.tables {
		k := idx.compoundKey(p, t)
		for _, c := range idx.tables[t][k] {
			if c.ID == p.ID {
				continue
			}
			if _, ok := seen[c.ID]; !ok {
				seen[c.ID] = struct{}{}
				result = append(result, Candidate{
					ID:       c.ID,
					Distance: dist3D(p, c),
				})
			}
		}
	}
	return result
}

// FullScanDuplicates возвращает пары точек ближе maxDist через обход LSH-бакетов.
func (idx *Index) FullScanDuplicates(maxDist float64) []Pair {
	type pairKey struct{ a, b int }
	seen := make(map[pairKey]struct{})
	var pairs []Pair
	for t := range idx.tables {
		for _, bucket := range idx.tables[t] {
			for i := 0; i < len(bucket); i++ {
				for j := i + 1; j < len(bucket); j++ {
					a, b := bucket[i], bucket[j]
					if a.ID > b.ID {
						a, b = b, a
					}
					pk := pairKey{a.ID, b.ID}
					if _, ok := seen[pk]; ok {
						continue
					}
					seen[pk] = struct{}{}
					d := dist3D(a, b)
					if d <= maxDist {
						pairs = append(pairs, Pair{a.ID, b.ID, d})
					}
				}
			}
		}
	}
	return pairs
}

func (idx *Index) Count() int { return len(idx.all) }

func dist3D(a, b Point3D) float64 {
	dx, dy, dz := a.X-b.X, a.Y-b.Y, a.Z-b.Z
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}
