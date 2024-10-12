package utils

import (
	"encoding/binary"
	"time"
)

// ValueStruct 只持久化具体的value值和过期时间
type ValueStruct struct {
	Value     []byte
	ExpiresAt uint64
}

// EncodedSize 是 ValueStruct 编码后的大小
func (e *ValueStruct) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

// 对 value 进行解码得到 ValueStruct
func (e *ValueStruct) DecodeValue(buf []byte) {
	var sz int
	e.ExpiresAt, sz = binary.Uvarint(buf)
	e.Value = buf[sz:]
}

// 对 value 进行编码，并将编码后的字节写入byte
// 这里将过期时间和value的值一起编码
func (e *ValueStruct) EncodeValue(buf []byte) uint32 {
	sz := binary.PutUvarint(buf[:], e.ExpiresAt)
	n := copy(buf[sz:], e.Value)
	return uint32(sz + n)
}

// 计算以 Varint（可变长度整数）格式编码一个 uint64 数值时所需的字节数
func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// Entry 最外层写入的结构体
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Version      uint64
	Offset       uint32
	Hlen         int
	ValThreshold int64
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e *Entry) Entry() *Entry {
	return e
}

func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

func (e *Entry) Size() int64 {
	return int64(len(e.Key) + len(e.Value))
}

func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}
