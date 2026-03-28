package hashfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"syscall"
)

// Store — интерфейс файлового key-value хранилища.
type Store interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Reset() error // сбрасывает все записи без пересоздания файла
	Close() error
}

// Options задаёт параметры открытия/создания хранилища.
type Options struct {
	BucketCount  uint64 // должно быть степенью двойки
	PageSize     uint64 // 0 → системный размер страницы
	MaxValueSize uint32 // 0 → 1 MiB
}

const (
	magicString = "HASHFS01"

	headerSize = 64

	flagTombstone = 1
)

var ErrNotFound = errors.New("hashfs: key not found")

// on-disk header layout (little-endian):
//   0:8   magic
//   8:12  version (uint32)
//  12:16  reserved
//  16:24 bucketCount (uint64)
//  24:32 dataStart (uint64)
//  32:40 tailOffset (uint64)
//  40:64 reserved

const (
	headerOffMagic       = 0
	headerOffVersion     = 8
	headerOffBucketCount = 16
	headerOffDataStart   = 24
	headerOffTail        = 32
)

// record layout:
// [keyLen uint32][valLen uint32][hash uint64][flags uint8][key bytes][value bytes][nextOffset uint64]

type store struct {
	f           *os.File
	mmap        []byte
	pageSize    uint64
	bucketCount uint64
	maxValue    uint32
}

// Open открывает или создаёт файловое хранилище по указанному пути.
func Open(path string, opts Options) (Store, error) {
	if opts.BucketCount == 0 || (opts.BucketCount&(opts.BucketCount-1)) != 0 {
		return nil, fmt.Errorf("hashfs: BucketCount must be power of two, got %d", opts.BucketCount)
	}

	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = uint64(os.Getpagesize())
	}
	maxVal := opts.MaxValueSize
	if maxVal == 0 {
		maxVal = 1 << 20 // 1 MiB по умолчанию
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	st := &store{
		f:           f,
		pageSize:    pageSize,
		bucketCount: opts.BucketCount,
		maxValue:    maxVal,
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	if info.Size() == 0 {
		if err := st.initNewFile(); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else {
		if err := st.loadExistingFile(); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	return st, nil
}

func (s *store) initNewFile() error {
	mmSize := alignUp(headerSize+8*s.bucketCount, s.pageSize)
	if err := s.f.Truncate(int64(mmSize)); err != nil {
		return err
	}

	dataStart := uint64(mmSize)
	tail := dataStart

	b, err := syscall.Mmap(int(s.f.Fd()), 0, int(mmSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	s.mmap = b

	copy(s.mmap[headerOffMagic:headerOffMagic+len(magicString)], []byte(magicString))
	binary.LittleEndian.PutUint32(s.mmap[headerOffVersion:headerOffVersion+4], 1)
	binary.LittleEndian.PutUint64(s.mmap[headerOffBucketCount:headerOffBucketCount+8], s.bucketCount)
	binary.LittleEndian.PutUint64(s.mmap[headerOffDataStart:headerOffDataStart+8], dataStart)
	binary.LittleEndian.PutUint64(s.mmap[headerOffTail:headerOffTail+8], tail)

	for i := uint64(0); i < s.bucketCount; i++ {
		pos := headerSize + i*8
		binary.LittleEndian.PutUint64(s.mmap[pos:pos+8], 0)
	}

	return nil
}

func (s *store) loadExistingFile() error {
	hdr := make([]byte, headerSize)
	if _, err := s.f.ReadAt(hdr, 0); err != nil {
		return err
	}
	if string(hdr[headerOffMagic:headerOffMagic+len(magicString)]) != magicString {
		return fmt.Errorf("hashfs: bad magic")
	}

	fileBucketCount := binary.LittleEndian.Uint64(hdr[headerOffBucketCount : headerOffBucketCount+8])
	if fileBucketCount == 0 {
		return fmt.Errorf("hashfs: zero bucketCount in header")
	}
	s.bucketCount = fileBucketCount

	mmSize := alignUp(headerSize+8*s.bucketCount, s.pageSize)

	b, err := syscall.Mmap(int(s.f.Fd()), 0, int(mmSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	s.mmap = b

	return nil
}

func (s *store) Close() error {
	var err1, err2 error
	if s.mmap != nil {
		err1 = syscall.Munmap(s.mmap)
	}
	if s.f != nil {
		err2 = s.f.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func (s *store) bucketHead(idx uint64) uint64 {
	pos := headerSize + idx*8
	return binary.LittleEndian.Uint64(s.mmap[pos : pos+8])
}

func (s *store) setBucketHead(idx uint64, off uint64) {
	pos := headerSize + idx*8
	binary.LittleEndian.PutUint64(s.mmap[pos:pos+8], off)
}

func (s *store) tailOffset() uint64 {
	return binary.LittleEndian.Uint64(s.mmap[headerOffTail : headerOffTail+8])
}

func (s *store) setTailOffset(off uint64) {
	binary.LittleEndian.PutUint64(s.mmap[headerOffTail:headerOffTail+8], off)
}

func (s *store) hashKey(key []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(key)
	return h.Sum64()
}

func (s *store) Put(key, value []byte) error {
	if uint32(len(value)) > s.maxValue {
		return fmt.Errorf("hashfs: value too large (%d > %d)", len(value), s.maxValue)
	}
	h := s.hashKey(key)
	idx := h & (s.bucketCount - 1)

	head := s.bucketHead(idx)

	recOff, err := s.appendRecord(key, value, h, 0, head)
	if err != nil {
		return err
	}
	s.setBucketHead(idx, recOff)
	return nil
}

func (s *store) Delete(key []byte) error {
	h := s.hashKey(key)
	idx := h & (s.bucketCount - 1)
	head := s.bucketHead(idx)

	recOff, err := s.appendRecord(key, nil, h, flagTombstone, head)
	if err != nil {
		return err
	}
	s.setBucketHead(idx, recOff)
	return nil
}

func (s *store) Get(key []byte) ([]byte, error) {
	h := s.hashKey(key)
	idx := h & (s.bucketCount - 1)

	off := s.bucketHead(idx)
	for off != 0 {
		hdr, err := s.readRecordHeader(off)
		if err != nil {
			return nil, err
		}

		if hdr.hash == h && hdr.keyLen == uint32(len(key)) {
			recKey, recVal, next, err := s.readRecord(off, hdr)
			if err != nil {
				return nil, err
			}
			if equalBytes(recKey, key) {
				if hdr.flags&flagTombstone != 0 {
					return nil, ErrNotFound
				}
				return recVal, nil
			}
			off = next
			continue
		}

		_, _, next, err := s.readRecord(off, hdr)
		if err != nil {
			return nil, err
		}
		off = next
	}

	return nil, ErrNotFound
}

type recordHeader struct {
	keyLen uint32
	valLen uint32
	hash   uint64
	flags  uint8
}

func (s *store) appendRecord(key, value []byte, hash uint64, flags uint8, next uint64) (uint64, error) {
	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	recSize := 4 + 4 + 8 + 1 + int(keyLen) + int(valLen) + 8 // keyLen+valLen+hash+flags+data+next
	buf := make([]byte, recSize)

	binary.LittleEndian.PutUint32(buf[0:4], keyLen)
	binary.LittleEndian.PutUint32(buf[4:8], valLen)
	binary.LittleEndian.PutUint64(buf[8:16], hash)
	buf[16] = flags

	pos := 17
	copy(buf[pos:pos+int(keyLen)], key)
	pos += int(keyLen)
	copy(buf[pos:pos+int(valLen)], value)
	pos += int(valLen)
	binary.LittleEndian.PutUint64(buf[pos:pos+8], next)

	tail := s.tailOffset()
	if _, err := s.f.WriteAt(buf, int64(tail)); err != nil {
		return 0, err
	}

	newTail := tail + uint64(recSize)
	s.setTailOffset(newTail)

	return tail, nil
}

func (s *store) readRecordHeader(off uint64) (recordHeader, error) {
	var hdrBuf [17]byte
	if _, err := s.f.ReadAt(hdrBuf[:], int64(off)); err != nil {
		return recordHeader{}, err
	}
	return recordHeader{
		keyLen: binary.LittleEndian.Uint32(hdrBuf[0:4]),
		valLen: binary.LittleEndian.Uint32(hdrBuf[4:8]),
		hash:   binary.LittleEndian.Uint64(hdrBuf[8:16]),
		flags:  hdrBuf[16],
	}, nil
}

func (s *store) readRecord(off uint64, hdr recordHeader) (key []byte, value []byte, next uint64, err error) {
	keyLen := int(hdr.keyLen)
	valLen := int(hdr.valLen)

	dataSize := keyLen + valLen + 8
	buf := make([]byte, dataSize)
	if _, err = s.f.ReadAt(buf, int64(off)+17); err != nil {
		return nil, nil, 0, err
	}

	key = make([]byte, keyLen)
	copy(key, buf[0:keyLen])

	value = make([]byte, valLen)
	copy(value, buf[keyLen:keyLen+valLen])

	next = binary.LittleEndian.Uint64(buf[keyLen+valLen : keyLen+valLen+8])
	return key, value, next, nil
}

func alignUp(x, align uint64) uint64 {
	if align == 0 {
		return x
	}
	r := x % align
	if r == 0 {
		return x
	}
	return x + align - r
}

// Reset обнуляет бакеты и сбрасывает tail — хранилище логически пусто.
func (s *store) Reset() error {
	for i := uint64(0); i < s.bucketCount; i++ {
		s.setBucketHead(i, 0)
	}
	dataStart := binary.LittleEndian.Uint64(s.mmap[headerOffDataStart : headerOffDataStart+8])
	s.setTailOffset(dataStart)
	return nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

