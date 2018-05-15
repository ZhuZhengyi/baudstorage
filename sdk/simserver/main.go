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
		err = m.handleCreateInode(conn, p)
	case proto.OpMetaCreateDentry:
		err = m.handleCreateDentry(conn, p)
	case proto.OpMetaDeleteInode:
		err = m.handleDeleteInode(conn, p)
	case proto.OpMetaDeleteDentry:
		err = m.handleDeleteDentry(conn, p)
	case proto.OpMetaLookup:
		err = m.handleLookup(conn, p)
	case proto.OpMetaReadDir:
		err = m.handleReadDir(conn, p)
	case proto.OpMetaInodeGet:
		err = m.handleInodeGet(conn, p)
	default:
		err = errors.New("unknown Opcode: ")
	}
	return
}

func (m *MetaServer) handleCreateInode(conn net.Conn, p *proto.Packet) error {
	req := &proto.CreateInodeRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	ino := m.allocIno()
	inode := NewInode(ino, req.Mode)

	resp := &proto.CreateInodeResponse{
		Info: NewInodeInfo(ino, req.Mode),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		p.Resultcode = proto.OpErr
		goto out
	}

	m.addInode(inode)
	p.Data = data
	p.Size = uint32(len(data))
	p.Resultcode = proto.OpOk

out:
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleCreateDentry(conn net.Conn, p *proto.Packet) error {
	req := &proto.CreateDentryRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	dentry := NewDentry(req.Name, req.Inode, req.Mode)
	p.Data = nil
	p.Size = 0

	parent := m.getInode(req.ParentID)
	if parent == nil {
		p.Resultcode = proto.OpNotExistErr
		goto out
	}

	if found := parent.addDentry(dentry); found != nil {
		p.Resultcode = proto.OpExistErr
		goto out
	}

	p.Resultcode = proto.OpOk
out:
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleDeleteInode(conn net.Conn, p *proto.Packet) error {
	req := &proto.DeleteInodeRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	ino := req.Inode
	p.Data = nil
	p.Size = 0

	inode := m.deleteInode(ino)
	if inode == nil {
		p.Resultcode = proto.OpNotExistErr
	} else {
		p.Resultcode = proto.OpOk
	}

	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleDeleteDentry(conn net.Conn, p *proto.Packet) error {
	req := &proto.DeleteDentryRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	var (
		data  []byte
		child *Dentry
		resp  *proto.DeleteDentryResponse
	)

	parent := m.getInode(req.ParentID)
	if parent == nil {
		p.Resultcode = proto.OpErr
		goto out
	}

	child = parent.deleteDentry(req.Name)
	if child == nil {
		p.Resultcode = proto.OpNotExistErr
		goto out
	}

	resp = &proto.DeleteDentryResponse{
		Inode: child.ino,
	}
	data, err = json.Marshal(resp)

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleLookup(conn net.Conn, p *proto.Packet) error {
	req := &proto.LookupRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	var (
		data   []byte
		dentry *Dentry
		resp   *proto.LookupResponse
	)

	parent := m.getInode(req.ParentID)
	if parent == nil {
		p.Resultcode = proto.OpErr
		goto out
	}

	dentry = parent.getDentry(req.Name)
	if dentry == nil {
		p.Resultcode = proto.OpNotExistErr
		goto out
	}

	resp = &proto.LookupResponse{
		Inode: dentry.ino,
		Mode:  dentry.mode,
	}

	data, _ = json.Marshal(resp)
	p.Resultcode = proto.OpOk

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleReadDir(conn net.Conn, p *proto.Packet) error {
	return nil
}

func (m *MetaServer) handleInodeGet(conn net.Conn, p *proto.Packet) error {
	return nil
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

func (m *MetaServer) addInode(i *Inode) *Inode {
	m.Lock()
	defer m.Unlock()
	inode, ok := m.inodes[i.ino]
	if ok {
		return inode
	}
	m.inodes[i.ino] = i
	return nil
}

func (m *MetaServer) deleteInode(ino uint64) *Inode {
	m.Lock()
	defer m.Unlock()
	inode, ok := m.inodes[ino]
	if ok {
		delete(m.inodes, ino)
		return inode
	}
	return nil
}

func (m *MetaServer) getInode(ino uint64) *Inode {
	m.RLock()
	defer m.RUnlock()
	i, ok := m.inodes[ino]
	if !ok {
		return nil
	}
	return i
}

func (m *MetaServer) allocIno() uint64 {
	return atomic.AddUint64(&m.currIno, 1)
}

func (i *Inode) addDentry(d *Dentry) *Dentry {
	i.Lock()
	defer i.Unlock()
	dentry, ok := i.dents[d.name]
	if ok {
		return dentry
	}
	i.dents[d.name] = d
	return nil
}

func (i *Inode) deleteDentry(name string) *Dentry {
	i.Lock()
	defer i.Unlock()
	dentry, ok := i.dents[name]
	if ok {
		delete(i.dents, name)
		return dentry
	}
	return nil
}

func (i *Inode) getDentry(name string) *Dentry {
	i.Lock()
	defer i.Unlock()
	dentry, ok := i.dents[name]
	if ok {
		return dentry
	}
	return nil
}
