package metanode

import "sync/atomic"

// ServiceState is a type alias for service status management.
type ServiceState = uint32

const (
	stateReady ServiceState = iota
	stateRunning
)

func TrySwitchState(state *ServiceState, from, to ServiceState) bool {
	return atomic.CompareAndSwapUint32(state, from, to)
}

func SetState(state *ServiceState, target ServiceState) {
	atomic.StoreUint32(state, target)
}
