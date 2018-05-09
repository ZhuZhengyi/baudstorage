package multiraft

import (
	"fmt"
)

//Address ...
type Address struct {
	Heartbeat string
	Replicate string
	Grpc      string
	Http      string
}

//AddrDatabase ...
var AddrDatabase = make(map[uint64]*Address)

//AddrInit ...
func AddrInit(peerAddress []string) (err error) {
	fmt.Println("PeerAddrs:")
	for _, peerAddr := range peerAddress {
		id, ip, port, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		AddrDatabase[id] = &Address{
			Http:      fmt.Sprintf("%s:%d", ip, port),
			Heartbeat: fmt.Sprintf("%s:%d", ip, HeartbeatPort),
			Replicate: fmt.Sprintf("%s:%d", ip, ReplicatePort),
			Grpc:      fmt.Sprintf("%s:%d", ip, GRpcPort),
		}
		fmt.Println(AddrDatabase[id])
	}
	return nil
}

//TODO
// 获取节点需要监听的地址
//func (am *addressManager) GetHeartListen(nodeID uint64) string {

// 获取节点需要监听的地址
//func (am *addressManager) GetReplicateListen(nodeID uint64) string {
