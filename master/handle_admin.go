package master

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"io/ioutil"
	"strings"
)

type ClusterView struct {
	Name           string
	Applied        uint64
	MaxVolID       uint64
	MaxMetaNodeID  uint64
	MaxPartitionID uint64
	Namespaces     []string
	MetaNodes      []NodeView
	DataNodes      []NodeView
}

type NodeView struct {
	Addr   string
	Status bool
}

func (m *Master) getCluster(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		err  error
	)
	cv := &ClusterView{
		Name:           m.cluster.Name,
		Applied:        m.fsm.applied,
		MaxVolID:       m.cluster.idAlloc.volID,
		MaxMetaNodeID:  m.cluster.idAlloc.metaNodeID,
		MaxPartitionID: m.cluster.idAlloc.partitionID,
		Namespaces:     make([]string, 0),
		MetaNodes:      make([]NodeView, 0),
		DataNodes:      make([]NodeView, 0),
	}

	cv.Namespaces = m.cluster.getAllNamespaces()
	cv.MetaNodes = m.cluster.getAllMetaNodes()
	cv.DataNodes = m.cluster.getAllDataNodes()
	if body, err = json.Marshal(cv); err != nil {
		goto errDeal
	}
	io.WriteString(w, string(body))
	return

errDeal:
	logMsg := getReturnMessage("getCluster", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) getIpAndClusterName(w http.ResponseWriter, r *http.Request) {
	cInfo := &proto.ClusterInfo{Cluster: m.cluster.Name, Ip: strings.Split(r.RemoteAddr, ":")[0]}
	bytes, err := json.Marshal(cInfo)
	if err != nil {
		goto errDeal
	}
	w.Write(bytes)
	return
errDeal:
	rstMsg := getReturnMessage("getIpAndClusterName", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(rstMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) createVol(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg string
		nsName string
		ns     *NameSpace
		count  int
		err    error
	)

	if count, nsName, err = parseCreateVolPara(r); err != nil {
		goto errDeal
	}

	if ns, err = m.cluster.getNamespace(nsName); err != nil {
		goto errDeal
	}
	for i := 0; i < count; i++ {
		if count < len(ns.volGroups.volGroups) {
			break
		}
		if _, err = m.cluster.createVolGroup(nsName); err != nil {
			goto errDeal
		}
	}
	rstMsg = fmt.Sprintf(" createVol success")
	io.WriteString(w, rstMsg)

	return
errDeal:
	rstMsg = getReturnMessage("createVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(rstMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) getVol(w http.ResponseWriter, r *http.Request) {
	var (
		nsName string
		ns     *NameSpace
		body   []byte
		vol    *VolGroup
		vr     *VolResponse
		volID  uint64
		err    error
	)
	if volID, nsName, err = parseVolIDAndNamespace(r); err != nil {
		goto errDeal
	}

	if ns, err = m.cluster.getNamespace(nsName); err != nil {
		goto errDeal
	}
	if vol, err = ns.getVolGroupByVolID(volID); err != nil {
		goto errDeal
	}
	vr = vol.convertToVolResponse()
	if body, err = json.Marshal(vr); err != nil {
		goto errDeal
	}
	io.WriteString(w, string(body))

	return
errDeal:
	logMsg := getReturnMessage("getVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) loadVol(w http.ResponseWriter, r *http.Request) {
	var (
		nsName string
		ns     *NameSpace
		msg    string
		v      *VolGroup
		volID  uint64
		err    error
	)

	if volID, nsName, err = parseVolIDAndNamespace(r); err != nil {
		goto errDeal
	}

	if ns, err = m.cluster.getNamespace(nsName); err != nil {
		goto errDeal
	}
	if v, err = ns.getVolGroupByVolID(volID); err != nil {
		goto errDeal
	}

	m.cluster.loadVolAndCheckResponse(v, false)
	msg = fmt.Sprintf(AdminLoadVol+"volID :%v  LoadVol success", volID)
	io.WriteString(w, msg)
	log.LogInfo(msg)

	return
errDeal:
	logMsg := getReturnMessage(AdminLoadVol, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) volOffline(w http.ResponseWriter, r *http.Request) {
	var (
		nsName string
		ns     *NameSpace
		rstMsg string
		vg     *VolGroup
		addr   string
		volID  uint64
		err    error
	)

	if addr, volID, nsName, err = parseVolOfflinePara(r); err != nil {
		goto errDeal
	}
	if ns, err = m.cluster.getNamespace(nsName); err != nil {
		goto errDeal
	}
	if vg, err = ns.getVolGroupByVolID(volID); err != nil {
		goto errDeal
	}
	m.cluster.volOffline(addr, nsName, vg, HandleVolOfflineErr)
	rstMsg = fmt.Sprintf(AdminVolOffline+"volID :%v  on node:%v  has offline success", volID, addr)
	io.WriteString(w, rstMsg)
	log.LogWarn(rstMsg)
	return
errDeal:
	logMsg := getReturnMessage(AdminVolOffline, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) createNamespace(w http.ResponseWriter, r *http.Request) {
	var (
		name       string
		err        error
		msg        string
		replicaNum int
	)

	if name, replicaNum, err = parseCreateNamespacePara(r); err != nil {
		goto errDeal
	}
	if err = m.cluster.createNamespace(name, uint8(replicaNum)); err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf("create namespace[%v] successed\n", name)
	io.WriteString(w, msg)
	log.LogInfo(msg)
	return

errDeal:
	logMsg := getReturnMessage("createNamespace", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) addDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		err      error
	)
	if nodeAddr, err = parseAddDataNodePara(r); err != nil {
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

func (m *Master) dataNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		dataNode *DataNode
		code     = http.StatusOK
		tr       *proto.AdminTask
		err      error
	)

	if tr, err = parseTaskResponse(r); err != nil {
		code = http.StatusBadRequest
		goto errDeal
	}

	if dataNode, err = m.cluster.getDataNode(tr.OperatorAddr); err != nil {
		code = http.StatusInternalServerError
		goto errDeal
	}

	m.cluster.dealDataNodeTaskResponse(dataNode.HttpAddr, tr)

	return

errDeal:
	logMsg := getReturnMessage("dataNodeTaskResponse", r.RemoteAddr, err.Error(),
		http.StatusBadRequest)
	HandleError(logMsg, code, w)
	return
}

func (m *Master) addMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, err = parseAddMetaNodePara(r); err != nil {
		goto errDeal
	}

	if id, err = m.cluster.addMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	io.WriteString(w, fmt.Sprintf("addMetaNode %v successed,id(%v)", nodeAddr, id))
	return
errDeal:
	logMsg := getReturnMessage("addMetaNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func parseAddMetaNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func parseAddDataNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
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

	if metaNode, err = m.cluster.getMetaNode(nodeAddr); err != nil {
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

func (m *Master) metaPartitionOffline(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID      uint64
		nsName, nodeAddr string
		msg              string
		err              error
	)
	if nodeAddr, nsName, partitionID, err = parseMetaPartitionOffline(r); err != nil {
		goto errDeal
	}

	if err = m.cluster.metaPartitionOffline(nsName, nodeAddr, partitionID); err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf(AdminLoadMetaPartition+"partitionID :%v  metaPartitionOffline success", partitionID)
	io.WriteString(w, msg)
	return
errDeal:
	logMsg := getReturnMessage(AdminMetaPartitionOffline, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) loadMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		nsName      string
		ns          *NameSpace
		msg         string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)

	if partitionID, nsName, err = parsePartitionIDAndNamespace(r); err != nil {
		goto errDeal
	}

	if ns, err = m.cluster.getNamespace(nsName); err != nil {
		goto errDeal
	}
	if mp, err = ns.getMetaPartitionById(partitionID); err != nil {
		goto errDeal
	}

	m.cluster.loadMetaPartitionAndCheckResponse(mp, false)
	msg = fmt.Sprintf(AdminLoadMetaPartition+"partitionID :%v  LoadVol success", partitionID)
	io.WriteString(w, msg)
	log.LogInfo(msg)

	return
errDeal:
	logMsg := getReturnMessage(AdminLoadMetaPartition, r.RemoteAddr, err.Error(), http.StatusBadRequest)
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

	if metaNode, err = m.cluster.getMetaNode(offLineAddr); err != nil {
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

func (m *Master) metaNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		metaNode *MetaNode
		code     = http.StatusOK
		tr       *proto.AdminTask
		err      error
	)

	if tr, err = parseTaskResponse(r); err != nil {
		code = http.StatusBadRequest
		goto errDeal
	}

	if metaNode, err = m.cluster.getMetaNode(tr.OperatorAddr); err != nil {
		code = http.StatusInternalServerError
		goto errDeal
	}

	m.cluster.dealMetaNodeTaskResponse(metaNode.Addr, tr)

	return

errDeal:
	logMsg := getReturnMessage("dataNodeTaskResponse", r.RemoteAddr, err.Error(),
		http.StatusBadRequest)
	HandleError(logMsg, code, w)
	return
}

func (m *Master) handleAddRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRaftNodePara(r)
	if err != nil {
		goto errDeal
	}

	if err = m.cluster.addRaftNode(id, addr); err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf("add  raft node id :%v, addr:%v successed \n", id, addr)
	io.WriteString(w, msg)
	return
errDeal:
	logMsg := getReturnMessage("add raft node", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) handleRemoveRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRaftNodePara(r)
	if err != nil {
		goto errDeal
	}
	err = m.cluster.removeRaftNode(id, addr)
	if err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf("remove  raft node id :%v,adr:%v successed\n", id, addr)
	io.WriteString(w, msg)
	return
errDeal:
	logMsg := getReturnMessage("remove raft node", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func parseRaftNodePara(r *http.Request) (id uint64, host string, err error) {
	r.ParseForm()
	var idStr string
	if idStr = r.FormValue(ParaId); idStr == "" {
		err = paraNotFound(ParaId)
		return
	}

	if id, err = strconv.ParseUint(idStr, 10, 64); err != nil {
		return
	}
	if host = r.FormValue(ParaNodeAddr); host == "" {
		err = paraNotFound(ParaNodeAddr)
		return
	}

	if arr := strings.Split(host, ColonSplit); len(arr) < 2 {
		err = UnMatchPara
		return
	}
	return
}

func parseGetMetaNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func parseGetDataNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func parseDataNodeOfflinePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func parseTaskResponse(r *http.Request) (tr *proto.AdminTask, err error) {
	var body []byte
	r.ParseForm()

	if body, err = ioutil.ReadAll(r.Body); err != nil {
		return
	}
	tr = &proto.AdminTask{}
	err = json.Unmarshal(body, tr)
	return
}

func parseCreateNamespacePara(r *http.Request) (name string, replicaNum int, err error) {
	r.ParseForm()
	if name, err = checkNamespace(r); err != nil {
		return
	}
	if replicaStr := r.FormValue(ParaReplicas); replicaStr == "" {
		err = paraNotFound(ParaReplicas)
		return
	} else if replicaNum, err = strconv.Atoi(replicaStr); err != nil || replicaNum < 2 {
		err = UnMatchPara
	}
	return
}

func parseCreateVolPara(r *http.Request) (count int, name string, err error) {
	r.ParseForm()
	if countStr := r.FormValue(ParaCount); countStr == "" {
		err = paraNotFound(ParaCount)
		return
	} else if count, err = strconv.Atoi(countStr); err != nil || count == 0 {
		err = UnMatchPara
		return
	}
	if name, err = checkNamespace(r); err != nil {
		return
	}
	return
}

func parseVolIDAndNamespace(r *http.Request) (volID uint64, name string, err error) {
	r.ParseForm()
	if volID, err = checkVolGroupID(r); err != nil {
		return
	}
	if name, err = checkNamespace(r); err != nil {
		return
	}
	return
}

func checkVolGroupID(r *http.Request) (volID uint64, err error) {
	var value string
	if value := r.FormValue(ParaVolGroup); value == "" {
		err = paraNotFound(ParaVolGroup)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseVolOfflinePara(r *http.Request) (nodeAddr string, volID uint64, name string, err error) {
	r.ParseForm()
	if volID, err = checkVolGroupID(r); err != nil {
		return
	}
	if nodeAddr, err = checkNodeAddr(r); err != nil {
		return
	}

	if name, err = checkNamespace(r); err != nil {
		return
	}
	return
}

func checkNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if nodeAddr = r.FormValue(ParaNodeAddr); nodeAddr == "" {
		err = paraNotFound(ParaNodeAddr)
		return
	}
	return
}

func parsePartitionIDAndNamespace(r *http.Request) (partitionID uint64, nsName string, err error) {
	r.ParseForm()
	if partitionID, err = checkMetaPartitionID(r); err != nil {
		return
	}
	if nsName, err = checkNamespace(r); err != nil {
		return
	}
	return
}

func parseMetaPartitionOffline(r *http.Request) (nsName, nodeAddr string, partitionID uint64, err error) {
	r.ParseForm()
	if partitionID, err = checkMetaPartitionID(r); err != nil {
		return
	}
	if nsName, err = checkNamespace(r); err != nil {
		return
	}
	if nodeAddr, err = checkNodeAddr(r); err != nil {
		return
	}
	return
}
