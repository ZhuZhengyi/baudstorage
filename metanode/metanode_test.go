package metanode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
)

func TestValidNodeID(t *testing.T) {
	logDir := "testlog"
	defer os.RemoveAll(logDir)
	_, err := log.NewLog(logDir, "MetaNode", log.DebugLevel)
	if err != nil {
		t.Fatalf("util/log module test failed: %s", err.Error())
	}
	httpServe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {
		data, err := json.Marshal(&proto.RegisterMetaNodeResp{
			ID: 55555,
		})
		if err != nil {
			w.WriteHeader(http.StatusNotImplemented)
			return
		}
		w.Write(data)
		return
	}))
	defer httpServe.Close()
	masterAddr := httpServe.Listener.Addr().String()
	m := &MetaNode{}
	err = m.validNodeID()
	if err == nil {
		t.Fatalf("master addrs is empty, ")
	}
	if err.Error() == "masterAddrs is empty" {
		t.Logf("empty master addrs test success!")
	}
	t.Logf("master Addr: %s", masterAddr)
	m.masterAddrs = "127.0.0.1:10234;127.0.0.1:22666;" + masterAddr
	if err = m.validNodeID(); err != nil {
		t.Fatalf("validNodeID: %s failed!", err.Error())
	}
	if m.nodeId != 55555 {
		t.Fatalf("valideNodeID: want nodeID=5555, have nodeID=%d failed!",
			m.nodeId)
	}
	t.Logf("valideNodeID success!")
}
