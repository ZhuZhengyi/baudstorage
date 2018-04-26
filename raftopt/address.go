package raftopt

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

const (
	HeartbeatPort = 9901
	ReplicatePort = 9902
	GrpcPort      = 9903
)

//AddrInit ...
func AddrInit(peerAddrs []string) (err error) {
	fmt.Println("PeerAddrs:")
	for _, peerAddr := range peerAddrs {
		id, ip, port, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		AddrDatabase[id] = &Address{
			Http:      fmt.Sprintf("%s:%d", ip, port),
			Heartbeat: fmt.Sprintf("%s:%d", ip, HeartbeatPort),
			Replicate: fmt.Sprintf("%s:%d", ip, ReplicatePort),
			Grpc:      fmt.Sprintf("%s:%d", ip, GrpcPort),
		}
		fmt.Println(AddrDatabase[id])
	}
	return nil
}
