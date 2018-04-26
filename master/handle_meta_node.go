package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"io"
	"net/http"
	"strings"
)

func (m *Master) addMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		zoneName string
		err      error
	)
	if zoneName, nodeAddr, err = parseAddMetaNodePara(r); err != nil {
		goto errDeal
	}

	if err = m.cluster.addMetaNode(nodeAddr, zoneName); err != nil {
		goto errDeal
	}
	io.WriteString(w, fmt.Sprintf("addMetaNode %v successed\n", nodeAddr))
	return
errDeal:
	logMsg := getReturnMessage("addMetaNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func parseAddMetaNodePara(r *http.Request) (zoneName, nodeAddr string, err error) {
	r.ParseForm()

	if zoneName = r.FormValue(ParaZoneName); zoneName == "" {
		err = paraNotFound(ParaZoneName)
		return
	}

	if arr := strings.Split(zoneName, UnderlineSeparator); len(arr) != 2 {
		err = paraFormatInvalid(zoneName)
		return
	}
	if nodeAddr = r.FormValue(ParaNodeAddr); nodeAddr == "" {
		err = paraNotFound(ParaNodeAddr)
	}
	return
}

func (m *Master) getMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		metaNode *MetaNode
		body     []byte
		err      error
	)
	if nodeAddr, err = parseGetMetaNodePara(r); err != nil {
		goto errDeal
	}

	if metaNode, _, _, err = m.cluster.topology.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if body, err = json.Marshal(metaNode); err != nil {
		goto errDeal
	}
	io.WriteString(w, string(body))
	return
errDeal:
	logMsg := getReturnMessage("getDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) metaNodeOffline(w http.ResponseWriter, r *http.Request) {
	var (
		metaNode    *MetaNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseDataNodeOfflinePara(r); err != nil {
		goto errDeal
	}

	if metaNode, _, err = m.cluster.getMetaNodeFromCluster(offLineAddr); err != nil {
		goto errDeal
	}
	m.cluster.metaNodeOffLine(metaNode)
	rstMsg = fmt.Sprintf("metaNodeOffline metaNode [%v] has offline SUCCESS", offLineAddr)
	io.WriteString(w, rstMsg)
	log.LogWarn(rstMsg)
	return
errDeal:
	logMsg := getReturnMessage("metaNodeOffline", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func parseGetMetaNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}
