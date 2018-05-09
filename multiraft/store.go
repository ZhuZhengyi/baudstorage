package multiraft

// Store is the interface defined the abstract and necessary methods for storage operation.
type Store interface {
	Set()
	Get()
	Del()
}