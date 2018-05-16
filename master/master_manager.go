package master

import "github.com/tiglabs/raft/proto"

func (m *Master) handleLeaderChange(leader uint64) {

}

func (m *Master) handlePeerChange(confChange *proto.ConfChange) (err error) {
	return
}

func (m *Master) handleApply(command []byte) (err error) {
	return
}

func (m *Master) loadMetadata() {

}
