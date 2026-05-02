package concmap

import (
	"encoding/binary"
	"hash/maphash"
	"math"
	"reflect"
	"unsafe"
)

func newDefaultHasher[K comparable](seed maphash.Seed) func(K) uint64 {
	return func(k K) uint64 {
		return hashReflect(seed, reflect.ValueOf(k))
	}
}

func hashReflect(seed maphash.Seed, v reflect.Value) uint64 {
	switch v.Kind() {
	case reflect.String:
		var h maphash.Hash
		h.SetSeed(seed)
		_, _ = h.WriteString(v.String())
		return h.Sum64()

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var h maphash.Hash
		h.SetSeed(seed)
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(v.Int()))
		_, _ = h.Write(buf[:])
		return h.Sum64()

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		var h maphash.Hash
		h.SetSeed(seed)
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], v.Uint())
		_, _ = h.Write(buf[:])
		return h.Sum64()

	case reflect.Bool:
		var h maphash.Hash
		h.SetSeed(seed)
		if v.Bool() {
			_, _ = h.WriteString("t")
		} else {
			_, _ = h.WriteString("f")
		}
		return h.Sum64()

	case reflect.Float32:
		var h maphash.Hash
		h.SetSeed(seed)
		x := math.Float32bits(float32(v.Float()))
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], x)
		_, _ = h.Write(buf[:])
		return h.Sum64()
	case reflect.Float64:
		var h maphash.Hash
		h.SetSeed(seed)
		x := math.Float64bits(v.Float())
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], x)
		_, _ = h.Write(buf[:])
		return h.Sum64()

	case reflect.Struct:
		var acc uint64
		n := v.NumField()
		for i := 0; i < n; i++ {
			f := v.Type().Field(i)
			if !f.IsExported() {
				panic("concmap: ключ-структура с неэкспортируемыми полями — передайте WithHasher")
			}
			part := hashReflect(seed, v.Field(i))
			acc ^= mix(acc, part)
		}
		return acc

	case reflect.Array:
		var acc uint64
		for i := 0; i < v.Len(); i++ {
			part := hashReflect(seed, v.Index(i))
			acc ^= mix(acc, part)
		}
		return acc

	case reflect.Ptr:
		var h maphash.Hash
		h.SetSeed(seed)
		if v.IsNil() {
			_, _ = h.WriteString("nilptr")
			return h.Sum64()
		}
		ptr := uintptr(v.Pointer())
		var buf [unsafe.Sizeof(uintptr(0))]byte
		switch unsafe.Sizeof(uintptr(0)) {
		case 4:
			binary.LittleEndian.PutUint32(buf[:], uint32(ptr))
		case 8:
			binary.LittleEndian.PutUint64(buf[:], uint64(ptr))
		}
		_, _ = h.Write(buf[:])
		return h.Sum64()

	case reflect.Interface:
		if v.IsNil() {
			var h maphash.Hash
			h.SetSeed(seed)
			_, _ = h.WriteString("nilIface")
			return h.Sum64()
		}
		return hashReflect(seed, v.Elem())

	default:
		panic("concmap: defaultHasher не умеет kind=" + v.Kind().String() + "; задайте WithHasher")
	}
}

func mix(a, b uint64) uint64 {
	// маленький не криптостойкий микшер разрядов
	a ^= b + 0x9e3779b97f4a7c15 + (a << 12) + (a >> 4)
	return a
}
