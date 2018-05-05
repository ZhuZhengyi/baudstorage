package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"hash/crc32"
)

const (
	MockDataDir = "data"
	MockLogDir  = "log"
)

type MockServer struct {
	datadir string
	storage *storage.ExtentStore
}

func NewMockServer(datadir string) (m *MockServer, err error) {
	m = new(MockServer)
	_, err = log.NewLog(MockLogDir, "mock", log.DebugLevel)
	if err != nil {
		return nil, errors.Annotatef(err, "NewMock server error")

	}
	m.datadir = datadir
	m.storage, err = storage.NewExtentStore(datadir, storage.ReBootStoreMode)
	if err != nil {
		return nil, errors.Annotatef(err, "NewMock server error")
	}
	return
}

func (m *MockServer) volGroupView() (views []*sdk.VolGroup) {
	views = make([]*sdk.VolGroup, 0)
	for i := 1; i <= 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		hosts := make([]string, 3)
		for j := 0; j < 3; j++ {
			hosts[j] = "127.0.0.1:9000"
		}
		v := &sdk.VolGroup{VolId: uint32(i), Goal: 3,
			Status: uint8((rand.Int()%2 + 1)), Hosts: hosts}
		views = append(views, v)
	}

	return
}

func (m *MockServer) packErrorBody(request *proto.Packet, err error) {
	log.LogError(fmt.Sprintf("request [%v]Action[%v] error[%v]", request.GetUniqLogId(),
		proto.GetOpMesg(request.Opcode), err.Error()))
	request.Data = make([]byte, len(err.Error()))
	copy(request.Data, ([]byte)(err.Error()))
	request.Size = uint32(len(request.Data))
	request.Opcode = proto.OpIntraGroupNetErr

	return
}

func (m *MockServer) operator(request *proto.Packet, connect net.Conn) {
	var err error

	defer func() {
		log.LogDebug(request.ActionMesg(util.GetFuncTrace(), "remote", time.Now().UnixNano(), err))
	}()
	switch request.Opcode {
	case proto.OpCreateFile:
		request.FileID, err = m.storage.Create()
		if err != nil {
			m.packErrorBody(request, err)
			request.WriteToConn(connect)
			return
		}
		request.PackOkReply()
		request.WriteToConn(connect)
	case proto.OpWrite:
		crc := crc32.ChecksumIEEE(request.Data[:request.Size])
		err = m.storage.Write(uint64(request.FileID), request.Offset, int64(request.Size), request.Data, crc)
		if err != nil {
			m.packErrorBody(request, err)
			request.WriteToConn(connect)
			return
		}
		request.PackOkReply()
		request.WriteToConn(connect)

	}

	return

}

const (
	ConnBufferSize     = 4096
	NoReadDeadlineTime = -1
)

func (s *MockServer) readFromCliAndDeal(connect net.Conn) (err error) {
	pkg := proto.NewPacket()
	if err = pkg.ReadFromConn(connect, NoReadDeadlineTime); err != nil {
		goto errDeal
	}
	s.operator(pkg, connect)

	return nil
errDeal:
	conn_tag := fmt.Sprintf("connection[%v <----> %v] ", connect.LocalAddr(), connect.RemoteAddr().String())
	if err == io.EOF {
		err = fmt.Errorf("%v was closed by peer[%v]", conn_tag, connect.RemoteAddr().String())
	}
	if err == nil {
		err = fmt.Errorf("msghandler(%v) requestCh is full", conn_tag)
	}
	log.LogInfo(err.Error())
	return

}

func (s *MockServer) serveConn(conn net.Conn) {
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)

	for {
		if err := s.readFromCliAndDeal(c); err != nil {
			break
		}
	}
	c.Close()
	return
}

func (s *MockServer) clientview(w http.ResponseWriter, r *http.Request) {
	views := s.volGroupView()
	body, _ := json.Marshal(views)
	w.Write(body)
}

func (s *MockServer) listenAndServe() (err error) {
	log.LogInfo("Start: listenAndServe")
	l, err := net.Listen("tcp", ":"+"9000")
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}
	go func() {
		http.HandleFunc("/client/view", s.clientview)
		http.ListenAndServe(":7778", nil)
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.LogError("failed to accept, err:", err)
			break
		}
		go s.serveConn(conn)
	}

	return l.Close()
}

func main() {
	m, err := NewMockServer(MockDataDir)
	if err != nil {
		fmt.Println(err)
	}
	m.listenAndServe()

}
