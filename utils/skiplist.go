package utils

import (
	"bytes"
	"sync"
	"sync/atomic"
)

const defaultMaxLevel = 20

type node struct {
	score float64 // 加快查找，只在内存中生效，因此不需要持久化
	value uint64  // 将value的off和size组装成一个uint64，实现原子化的操作
	// 尽量减少 key 相关的内存占用
	keyOffset uint32 // 不可变， 不需要加锁访问
	keySize   uint16 // 不可变， 不需要加锁访问

	height uint16 // 这个节点所处的层级，同时代表了这个节点有几个 next 指针

	levels [defaultMaxLevel]uint32 // //先按照最大高度声明，往arena中放置的时候，会计算实际高度和内存消耗
}

// 用来计算节点在 h 层数下的 next 节点
func (e *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&e.levels[h])
}

// 用来对value值进行编解码
// value = valueSize | valueOffset
func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putValue(v), v.EncodedSize())
	//这里的elem是根据内存中的地址来读取的，不是arena中的offset
	elem := arena.getNode(nodeOffset)
	elem.score = calcScore(key)
	elem.keyOffset = keyOffset
	elem.keySize = uint16(len(key))
	elem.height = uint16(height)
	elem.value = val

	return elem
}

func (e *node) key(arena *Arena) []byte {
	return arena.getKey(e.keyOffset, e.keySize)
}

type SkipList struct {
	maxLevel   int          //sl的最大高度
	lock       sync.RWMutex //读写锁，用来实现并发安全的sl
	currHeight int32        //sl当前的最大高度
	headOffset uint32       //头结点在arena当中的偏移量
	arena      *Arena
}

func NewSkipList(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	//引入一个空的头结点，因此Key和Value都是空的
	head := newNode(arena, nil, ValueStruct{}, defaultMaxLevel)
	ho := arena.getNodeOffset(head)
	return &SkipList{
		currHeight: 1,
		headOffset: ho,
		arena:      arena,
	}
}

// 拿到某个节点，在某个高度上的next节点
// 如果该节点已经是该层最后一个节点（该节点的level[height]将是0），会返回nil
func (list *SkipList) getNext(e *node, height int) *node {
	return list.arena.getNode(e.getNextOffset(height))
}

// Add 用于向跳表中插入一个新的元素
// 它会从当前跳表的最大高度开始查找插入位置，找到后根据随机层数生成一个新节点并插入到对应的位置
func (list *SkipList) Add(data *Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()

	score := calcScore(data.Key)
	var elem *node
	value := ValueStruct{
		Value: data.Value,
	}
	// 从当前最大高度开始
	max := list.currHeight
	// 拿到头节点，从第一个开始
	prevElem := list.arena.getNode(list.headOffset)
	// 用于记录访问路径
	var prevElemHeaders [defaultMaxLevel]*node

	for i := max - 1; i >= 0; i-- {
		// 记录当前层的前驱节点
		prevElemHeaders[i] = prevElem
		for next := list.getNext(prevElem, int(i)); next != nil; next = list.getNext(prevElem, int(i)) {
			if comp := list.compare(score, data.Key, next); comp <= 0 {
				if comp == 0 {
					vo := list.arena.putValue(value) // 更新已有节点的值
					encV := encodeValue(vo, value.EncodedSize())
					next.value = encV
					return nil
				}
				break // 找到插入位置
			}
			prevElem = next // 继续向后查找
			prevElemHeaders[i] = prevElem
		}
		topLevel := prevElem.levels[i]
		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
			prevElemHeaders[i] = prevElem
		}
	}
	// 随机生成新节点的层数
	level := list.randLevel()
	elem = newNode(list.arena, data.Key, ValueStruct{Value: data.Value}, level)
	off := list.arena.getNodeOffset(elem)
	for i := 0; i < level; i++ {
		// 插入新节点到跳表
		elem.levels[i] = prevElemHeaders[i].levels[i]
		prevElemHeaders[i].levels[i] = off
	}
	return nil
}

// Search 函数从跳表的头节点开始，根据 key 逐层查找对应的节点，并返回 Entry
func (list *SkipList) Search(key []byte) (e *Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()

	if list.arena.Size() == 0 {
		return nil
	}

	score := calcScore(key)

	prevElem := list.arena.getNode(list.headOffset)
	i := list.currHeight

	for i >= 0 {
		for next := list.getNext(prevElem, int(i)); next != nil; next = list.getNext(prevElem, int(i)) {
			if comp := list.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					// 找到目标节点并解码 value
					vo, vSize := decodeValue(next.value)
					return &Entry{Key: key, Value: list.arena.getValue(vo, vSize).Value}
				}
				break
			}
			prevElem = next
		}
		topLevel := prevElem.levels[i]
		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {

		}
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)
	if l > 8 {
		l = 8
	}
	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - 8*i)
		hash |= uint64(key[i]) << shift
	}
	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *node) int {
	if score == next.score {
		return bytes.Compare(key, next.key(list.arena))
	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	if list.maxLevel <= 1 {
		return 1
	}
	i := 1
	for ; i < list.maxLevel; i++ {
		if RandN(1000)%2 == 0 {
			return i
		}
	}
	return i
}

func (list *SkipList) Size() int64 {
	return list.arena.Size()
}

type SkipListIter struct {
	list *SkipList
	nd   *node
	lock sync.RWMutex
}

func (list *SkipList) NewSkipListIterator() Iterator {
	return &SkipListIter{
		list: list,
	}
}

func (iter *SkipListIter) Next() {
	AssertTrue(iter.Valid())
	//只在最底层遍历就行
	iter.nd = iter.list.getNext(iter.nd, 0)
}

func (iter *SkipListIter) Valid() bool {
	return iter.nd != nil
}

// Rewind 方法用于将迭代器重置到跳表的起始位置
func (iter *SkipListIter) Rewind() {
	head := iter.list.arena.getNode(iter.list.headOffset)
	iter.nd = iter.list.getNext(head, 0)
}

func (iter *SkipListIter) Item() Item {
	vo, vs := decodeValue(iter.nd.value)
	return &Entry{
		Key:       iter.list.arena.getKey(iter.nd.keyOffset, iter.nd.keySize),
		Value:     iter.list.arena.getValue(vo, vs).Value,
		ExpiresAt: iter.list.arena.getValue(vo, vs).ExpiresAt,
	}
}

func (iter *SkipListIter) Close() error {
	return nil
}

func (iter *SkipListIter) Seek(key []byte) {
	return
}
