package perfecthash

import (
	"encoding/binary"
	"errors"
)

// Table — статический индекс string→int для фиксированного набора ключей.
type Table struct {
	index map[string]int
}

// Builder строит таблицу по набору ключей.
type Builder struct{}

// Build строит индекс key→порядковый номер. Ключи должны быть уникальными.
func (b *Builder) Build(keys [][]byte) (*Table, error) {
	idx := make(map[string]int, len(keys))
	for i, k := range keys {
		s := string(k)
		if _, exists := idx[s]; exists {
			return nil, errors.New("perfecthash: duplicate key")
		}
		idx[s] = i
	}
	return &Table{index: idx}, nil
}

// Lookup возвращает индекс ключа и признак его наличия.
func (t *Table) Lookup(key []byte) (int, bool) {
	if t == nil || t.index == nil {
		return 0, false
	}
	i, ok := t.index[string(key)]
	return i, ok
}

// Serialize кодирует таблицу: [n uint32][keyLen uint32, keyBytes, index uint32...].
func (t *Table) Serialize() []byte {
	if t == nil || len(t.index) == 0 {
		return nil
	}
	n := len(t.index)
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(n))

	for k, v := range t.index {
		kb := []byte(k)
		tmp := make([]byte, 4+len(kb)+4)
		binary.LittleEndian.PutUint32(tmp[0:4], uint32(len(kb)))
		copy(tmp[4:4+len(kb)], kb)
		binary.LittleEndian.PutUint32(tmp[4+len(kb):4+len(kb)+4], uint32(v))
		buf = append(buf, tmp...)
	}
	return buf
}

// Deserialize восстанавливает таблицу из байт Serialize.
func Deserialize(data []byte) (*Table, error) {
	if len(data) == 0 {
		return &Table{index: make(map[string]int)}, nil
	}
	if len(data) < 4 {
		return nil, errors.New("perfecthash: invalid data length")
	}
	n := int(binary.LittleEndian.Uint32(data[0:4]))
	idx := make(map[string]int, n)

	pos := 4
	for i := 0; i < n; i++ {
		if pos+4 > len(data) {
			return nil, errors.New("perfecthash: corrupted data (key length)")
		}
		kl := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+kl+4 > len(data) {
			return nil, errors.New("perfecthash: corrupted data (key/index)")
		}
		key := string(data[pos : pos+kl])
		pos += kl
		v := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		idx[key] = v
	}

	return &Table{index: idx}, nil
}

