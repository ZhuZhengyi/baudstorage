package master

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/tiglabs/baudstorage/util/log"
)

func (m *Master) addDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		err      error
	)
	if nodeAddr, err = parseAddMetaNodePara(r); err != nil {
		goto errDeal
	}

	if err = m.cluster.addDataNode(nodeAddr); err != nil {
		goto errDeal
	}
	io.WriteString(w, fmt.Sprintf("addDataNode %v successed\n", nodeAddr))
	return
errDeal:
	logMsg := getReturnMessage("addDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) getDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		dataNode *DataNode
		body     []byte
		err      error
	)
	if nodeAddr, err = parseGetDataNodePara(r); err != nil {
		goto errDeal
	}

	if dataNode, err = m.cluster.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}
	if body, err = json.Marshal(dataNode); err != nil {
		goto errDeal
	}
	io.WriteString(w, string(body))

	return
errDeal:
	logMsg := getReturnMessage("getDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) dataNodeOffline(w http.ResponseWriter, r *http.Request) {
	var (
		node        *DataNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseDataNodeOfflinePara(r); err != nil {
		goto errDeal
	}

	if node, err = m.cluster.getDataNode(offLineAddr); err != nil {
		goto errDeal
	}
	m.cluster.dataNodeOffLine(node)
	rstMsg = fmt.Sprintf("dataNodeOffline node [%v] has offline SUCCESS", offLineAddr)
	io.WriteString(w, rstMsg)
	log.LogWarn(rstMsg)
	return
errDeal:
	logMsg := getReturnMessage("dataNodeOffline", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func parseGetDataNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func parseDataNodeOfflinePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}
