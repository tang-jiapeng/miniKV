package utils

// 迭代器
type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
}
type Item interface {
	Entry() *Entry
}
type Options struct {
	Prefix []byte
	IsAsc  bool
}
