package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

type Arena struct {
	n   uint32 // 当前Arena已分配出去的内存大小 offset
	buf []byte // Arena申请的内存空间
}

const MaxNodeSize = int(unsafe.Sizeof(node{}))

const offsetSize = int(unsafe.Sizeof(uint32(0)))
const nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

func newArena(n int64) *Arena {
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	// 原子操作，在已占有的内存空间数值上 + 要分配的内存大小
	offset := atomic.AddUint32(&s.n, sz)
	// 分配的内存空间不足以放得下一个新节点时
	if len(s.buf)-int(offset) < MaxNodeSize {
		// Arena的空间double
		growBy := uint32(len(s.buf))
		if growBy < 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(s.buf)+int(growBy))
		// RCU操作，全量copy到新buf上，然后设置为新的Arena内存池
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - sz
}

func (s *Arena) putNode(height int) uint32 {
	unusedsize := (defaultMaxLevel - height) * offsetSize
	l := uint32(MaxNodeSize - unusedsize + nodeAlign)
	n := s.allocate(uint32(l))
	// 内存对齐操作
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

// 将  Key 值存储到 Arena 当中
// 并且将指针返回，返回的指针值应被存储在 Node 节点中
func (s *Arena) putKey(key []byte) uint32 {
	l := len(key)
	offset := s.allocate(uint32(l))
	bufset := s.buf[offset : offset+uint32(l)]
	AssertTrue(l == copy(bufset, key))
	return offset
}

// 将 Value 值存储到 Arena 当中
// 并且将指针返回，返回的指针值应被存储在 Node 节点中
func (s *Arena) putValue(v ValueStruct) uint32 {
	offset := s.allocate(v.EncodedSize())
	v.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getValue(offset uint32, size uint32) (v ValueStruct) {
	v.DecodeValue(s.buf[offset : offset+uint32(size)])
	return
}

// 用element在内存中的地址 - arena首字节的内存地址，得到在arena中的偏移量
func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed!"))
	}
}
