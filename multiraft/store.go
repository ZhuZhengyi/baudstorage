package multiraft

// Store is the interface defined the abstract and necessary methods for storage operation.
type Store interface {
	Put(key, val []byte) ([]byte, error)
	Get(key []byte) ([]byte, error)
	Del(key []byte) ([]byte, error)
}