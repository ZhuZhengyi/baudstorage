package main

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	. "github.com/tiglabs/baudstorage/sdk"
)

const (
	SimNamespace  = "simserver"
	SimMasterPort = "8900"
	SimMetaAddr   = "localhost"
	SimMetaPort   = "8910"

	SimLogFlags = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile
)

var globalMP = []MetaPartition{
	{"mp001", 1, 100, nil},
	{"mp002", 101, 200, nil},
	{"mp003", 210, 300, nil},
	{"mp004", 301, 400, nil},
}

type MasterServer struct {
	ns map[string]*NamespaceView
}

type MetaServer struct {
	sync.RWMutex
	inodes  map[uint64]*Inode
	currIno uint64
}

type Inode struct {
	sync.RWMutex
	ino   uint64
	mode  uint32
	dents map[string]*Dentry
}

type Dentry struct {
	name string
	ino  uint64
	mode uint32
}

func main() {
	log.SetFlags(SimLogFlags)
	log.Println("Staring Sim Server ...")
	runtime.GOMAXPROCS(runtime.NumCPU())

	var wg sync.WaitGroup

	ms := NewMasterServer()
	ms.Start(&wg)

	mt := NewMetaServer()
	mt.Start(&wg)

	wg.Wait()
}

// Master Server

func NewMasterServer() *MasterServer {
	return &MasterServer{
		ns: make(map[string]*NamespaceView),
	}
}

func (m *MasterServer) Start(wg *sync.WaitGroup) {
	nv := &NamespaceView{
		Name:           SimNamespace,
		MetaPartitions: make([]*MetaPartition, 0),
	}

	for _, p := range globalMP {
		mp := NewMetaPartition(p.PartitionID, p.Start, p.End, SimMetaAddr+":"+SimMetaPort)
		nv.MetaPartitions = append(nv.MetaPartitions, mp)
	}

	m.ns[nv.Name] = nv

	wg.Add(1)
	go func() {
		defer wg.Done()
		http.HandleFunc("/client/namespace", m.handleClientNS)
		if err := http.ListenAndServe(":"+SimMasterPort, nil); err != nil {
			log.Println(err)
		} else {
			log.Println("Done!")
		}
	}()
}

func (m *MasterServer) handleClientNS(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")
	nv, ok := m.ns[name]
	if !ok {
		http.Error(w, "No such namespace!", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(nv)
	if err != nil {
		http.Error(w, "JSON marshal failed!", http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func NewMetaPartition(id string, start, end uint64, member string) *MetaPartition {
	return &MetaPartition{
		PartitionID: id,
		Start:       start,
		End:         end,
		Members:     []string{member, member, member},
	}
}

// Meta Server

func NewMetaServer() *MetaServer {
	return &MetaServer{
		inodes:  make(map[uint64]*Inode),
		currIno: proto.ROOT_INO,
	}
}

func NewInode(ino uint64, mode uint32) *Inode {
	return &Inode{
		ino:   ino,
		mode:  mode,
		dents: make(map[string]*Dentry),
	}
}

func NewDentry(name string, ino uint64, mode uint32) *Dentry {
	return &Dentry{
		name: name,
		ino:  ino,
		mode: mode,
	}
}

func (m *MetaServer) Start(wg *sync.WaitGroup) {
	// Create root inode
	i := NewInode(proto.ROOT_INO, proto.ModeDir)
	m.inodes[i.ino] = i

	ln, err := net.Listen("tcp", ":"+SimMetaPort)
	if err != nil {
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go m.servConn(conn)
		}
	}()
}

func (m *MetaServer) servConn(conn net.Conn) {
	defer conn.Close()

	for {
		p := &proto.Packet{}
		if err := p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
			log.Println("servConn ReadFromConn:", err)
			return
		}

		if err := m.handlePacket(conn, p); err != nil {
			log.Println("servConn handlePacket:", err)
			return
		}
	}
}

func (m *MetaServer) handlePacket(conn net.Conn, p *proto.Packet) (err error) {
	switch p.Opcode {
	case proto.OpMetaCreateInode:
		err = m.opCreateInode(conn, p)
		//	case proto.OpMetaCreateDentry:
		//		err = m.opCreateDentry(conn, p)
		//	case proto.OpMetaDeleteInode:
		//		err = m.opDeleteInode(conn, p)
		//	case proto.OpMetaDeleteDentry:
		//		err = m.opDeleteDentry(conn, p)
		//	case proto.OpMetaReadDir:
		//		err = m.opReadDir(conn, p)
		//	case proto.OpMetaOpen:
		//		err = m.opOpen(conn, p)
		//	case proto.OpMetaCreateMetaRange:
		//		err = m.opCreateMetaRange(conn, p)
	default:
		err = errors.New("unknown Opcode: ")
	}
	return
}

func (m *MetaServer) opCreateInode(conn net.Conn, p *proto.Packet) error {
	req := &proto.CreateInodeRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		log.Println("opCreateInode Unmarshal, err = ", err)
		return err
	}

	ino := m.allocIno()
	i := NewInode(ino, req.Mode)
	m.addInode(i)

	resp := &proto.CreateInodeResponse{
		Info: NewInodeInfo(ino, req.Mode),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		log.Println("opCreateInode Marshal, err = ", err)
		goto errOut
	}

	p.Data = data
	p.Size = uint32(len(data))
	p.ResultCode = proto.OpOk
	err = p.WriteToConn(conn)
	if err != nil {
		goto errOut
	}

	return nil

errOut:
	m.deleteInode(i)
	return err
}

func NewInodeInfo(ino uint64, mode uint32) *proto.InodeInfo {
	return &proto.InodeInfo{
		Inode:      ino,
		Type:       mode,
		Size:       0,
		ModifyTime: time.Now(),
		AccessTime: time.Now(),
		CreateTime: time.Now(),
		Extents:    make([]string, 0),
	}
}

func (m *MetaServer) addInode(i *Inode) {
	m.Lock()
	defer m.Unlock()
	m.inodes[i.ino] = i
}

func (m *MetaServer) deleteInode(i *Inode) {
	m.Lock()
	defer m.Unlock()
	delete(m.inodes, i.ino)
}

func (m *MetaServer) allocIno() uint64 {
	return atomic.AddUint64(&m.currIno, 1)
}
