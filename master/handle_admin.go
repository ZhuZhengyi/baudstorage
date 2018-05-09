package master

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/tiglabs/baudstorage/util/log"
)

func (m *Master) createVol(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg string
		count  int
		err    error
	)

	if count, err = parseCreateVolPara(r); err != nil {
		goto errDeal
	}
	for i := 0; i < count; i++ {
		if _, err = m.cluster.createVolGroup(); err != nil {
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
		body  []byte
		vol   *VolGroup
		vr    *VolResponse
		volID uint64
		err   error
	)
	if volID, err = parseVolGroupID(r); err != nil {
		goto errDeal
	}
	if vol, err = m.cluster.getVolGroupByVolID(volID); err != nil {
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
		c     *Cluster
		msg   string
		v     *VolGroup
		volID uint64
		err   error
	)

	if volID, err = parseVolGroupID(r); err != nil {
		goto errDeal
	}

	if v, err = c.getVolGroupByVolID(volID); err != nil {
		goto errDeal
	}

	c.loadVolAndCheckResponse(v, false)
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
		rstMsg string
		vg     *VolGroup
		addr   string
		volID  uint64
		err    error
	)

	if addr, volID, err = parseVolOfflinePara(r); err != nil {
		goto errDeal
	}

	if vg, err = m.cluster.getVolGroupByVolID(volID); err != nil {
		goto errDeal
	}
	m.cluster.volOffline(addr, vg, HandleVolOfflineErr)
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

func parseCreateNamespacePara(r *http.Request) (name string, replicaNum int, err error) {
	r.ParseForm()
	if name, err = checkNamespace(r); err != nil {
		return
	}
	if replicaStr := r.FormValue(ParaReplicas); replicaStr == "" {
		err = paraNotFound(ParaReplicas)
		return
	} else if replicaNum, err = strconv.Atoi(replicaStr); err != nil || replicaNum == 0 {
		err = UnMatchPara
	}
	return
}

func parseCreateVolPara(r *http.Request) (count int, err error) {
	r.ParseForm()
	if countStr := r.FormValue(ParaCount); countStr == "" {
		err = paraNotFound(ParaCount)
		return
	} else if count, err = strconv.Atoi(countStr); err != nil || count == 0 {
		err = UnMatchPara
		return
	}
	return
}

func parseVolGroupID(r *http.Request) (volID uint64, err error) {
	r.ParseForm()
	return checkVolGroupID(r)
}

func checkVolGroupID(r *http.Request) (volID uint64, err error) {
	var value string
	if value := r.FormValue(ParaVolGroup); value == "" {
		err = paraNotFound(ParaVolGroup)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseVolOfflinePara(r *http.Request) (nodeAddr string, volID uint64, err error) {
	r.ParseForm()
	if volID, err = checkVolGroupID(r); err != nil {
		return
	}
	if nodeAddr, err = checkNodeAddr(r); err != nil {
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
