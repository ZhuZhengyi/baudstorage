package multiraft

// Store is the interface defined the abstract and necessary methods for storage operation.
type Store interface {
	Set(key interface{}, val interface{}) error
	Get(key interface{}) (interface{}, error)
	Del(key interface{}) error
}