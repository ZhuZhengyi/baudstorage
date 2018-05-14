package datanode

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/tiglabs/baudstorage/proto"
)

var (
	ErrBadNodes          = errors.New("BadNodesErr")
	ErrArgLenUnmatch     = errors.New("ArgLenUnmatchErr")
	ErrAddrsNodesUnmatch = errors.New("AddrsNodesUnmatchErr")
)

type Packet struct {
	proto.Packet
	goals    uint8
	nextConn net.Conn
	nextAddr string
	addrs    []string
	isReturn bool
}

func (p *Packet) UnmarshalAddrs() (addrs []string, err error) {
	if len(p.Arg) < int(p.Arglen) {
		return nil, ErrArgLenUnmatch
	}
	str := string(p.Arg[:int(p.Arglen)])
	goalAddrs := strings.SplitN(str, proto.AddrSplit, -1)
	p.goals = uint8(len(goalAddrs) - 1)
	if p.goals > 0 {
		addrs = goalAddrs[:int(p.goals)]
	}
	if p.Nodes < 0 {
		err = ErrBadNodes
		return
	}
	copy(p.addrs, addrs)

	return
}

func (p *Packet) GetNextAddr(addrs []string) error {
	sub := p.goals - p.Nodes
	if sub < 0 || sub > p.goals || (sub == p.goals && p.Nodes != 0) {
		return ErrAddrsNodesUnmatch
	}
	if sub == p.goals && p.Nodes == 0 {
		return nil
	}

	p.nextAddr = fmt.Sprint(addrs[sub])

	return nil
}

func (p *Packet) IsTransitPkg() bool {
	r := p.Nodes > 0
	return r
}

func (p *Packet) actionMesg(action, remote string, start int64, err error) (m string) {
	if err == nil {
		m = fmt.Sprintf("id[%v] act[%v] remote[%v] op[%v] local[%v] size[%v] "+
			" cost[%v] isTransite[%v] ",
			p.GetUniqLogId(), action, remote, proto.GetOpMesg(p.Opcode), proto.GetOpMesg(p.Resultcode), p.Size,
			(time.Now().UnixNano()-start)/1e6, p.IsTransitPkg())

	} else {
		m = fmt.Sprintf("id[%v] act[%v] remote[%v] op[%v] local[%v] size[%v] "+
			", err[%v] isTransite[%v]", p.GetUniqLogId(), action,
			remote, proto.GetOpMesg(p.Opcode), proto.GetOpMesg(p.Resultcode), p.Size, err.Error(),
			p.IsTransitPkg())
	}

	return
}
