package geo

import (
	"errors"
	"strings"
)

// Alphabet — base32-алфавит геохэша (RFC-совместимый).
const Alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

var charToIdx [256]int8

func init() {
	for i := range charToIdx {
		charToIdx[i] = -1
	}
	for i, c := range Alphabet {
		charToIdx[c] = int8(i)
	}
}

// Encode кодирует координаты в строку геохэша длиной precision.
func Encode(lat, lng float64, precision int) string {
	if precision < 1 {
		precision = 1
	}
	if precision > 12 {
		precision = 12
	}

	minLat, maxLat := -90.0, 90.0
	minLng, maxLng := -180.0, 180.0

	result := make([]byte, precision)
	ch := 0
	bitIdx := 4  // в каждом символе 5 бит: 4,3,2,1,0
	evenBit := true // true → обрабатываем долготу (lon), false → широту (lat)
	pos := 0

	for pos < precision {
		if evenBit {
			mid := (minLng + maxLng) / 2
			if lng >= mid {
				ch |= 1 << bitIdx
				minLng = mid
			} else {
				maxLng = mid
			}
		} else {
			mid := (minLat + maxLat) / 2
			if lat >= mid {
				ch |= 1 << bitIdx
				minLat = mid
			} else {
				maxLat = mid
			}
		}
		evenBit = !evenBit

		if bitIdx == 0 {
			result[pos] = Alphabet[ch]
			pos++
			ch = 0
			bitIdx = 4
		} else {
			bitIdx--
		}
	}

	return string(result)
}

// DecodeBounds декодирует геохэш в прямоугольник (minLat, maxLat, minLng, maxLng).
func DecodeBounds(hash string) (minLat, maxLat, minLng, maxLng float64, err error) {
	minLat, maxLat = -90.0, 90.0
	minLng, maxLng = -180.0, 180.0
	evenBit := true

	for _, c := range strings.ToLower(hash) {
		idx := charToIdx[c]
		if idx < 0 {
			return 0, 0, 0, 0, errors.New("geohash: invalid character " + string(c))
		}
		for bits := 4; bits >= 0; bits-- {
			bitN := (int(idx) >> bits) & 1
			if evenBit {
				mid := (minLng + maxLng) / 2
				if bitN == 1 {
					minLng = mid
				} else {
					maxLng = mid
				}
			} else {
				mid := (minLat + maxLat) / 2
				if bitN == 1 {
					minLat = mid
				} else {
					maxLat = mid
				}
			}
			evenBit = !evenBit
		}
	}
	return minLat, maxLat, minLng, maxLng, nil
}

// Decode возвращает центр ячейки и половину размера ячейки по каждой оси.
func Decode(hash string) (lat, lng, latErr, lngErr float64, err error) {
	minLat, maxLat, minLng, maxLng, err := DecodeBounds(hash)
	if err != nil {
		return
	}
	lat = (minLat + maxLat) / 2
	lng = (minLng + maxLng) / 2
	latErr = (maxLat - minLat) / 2
	lngErr = (maxLng - minLng) / 2
	return
}

// Neighbors возвращает до 8 соседних геохэшей.
func Neighbors(hash string) []string {
	lat, lng, latErr, lngErr, err := Decode(hash)
	if err != nil {
		return nil
	}
	prec := len(hash)

	dirs := [][2]float64{
		{lat + 2*latErr, lng},              // N
		{lat + 2*latErr, lng + 2*lngErr},   // NE
		{lat, lng + 2*lngErr},              // E
		{lat - 2*latErr, lng + 2*lngErr},   // SE
		{lat - 2*latErr, lng},              // S
		{lat - 2*latErr, lng - 2*lngErr},   // SW
		{lat, lng - 2*lngErr},              // W
		{lat + 2*latErr, lng - 2*lngErr},   // NW
	}

	seen := make(map[string]struct{}, 8)
	result := make([]string, 0, 8)

	for _, d := range dirs {
		nlat, nlng := clampLat(d[0]), wrapLng(d[1])
		n := Encode(nlat, nlng, prec)
		if _, dup := seen[n]; dup {
			continue
		}
		seen[n] = struct{}{}
		result = append(result, n)
	}
	return result
}

// NeighborsAndSelf возвращает хэш ячейки и её 8 соседей (итого до 9).
func NeighborsAndSelf(hash string) []string {
	return append([]string{hash}, Neighbors(hash)...)
}

func clampLat(lat float64) float64 {
	if lat > 90 {
		return 90
	}
	if lat < -90 {
		return -90
	}
	return lat
}

func wrapLng(lng float64) float64 {
	for lng > 180 {
		lng -= 360
	}
	for lng < -180 {
		lng += 360
	}
	return lng
}
