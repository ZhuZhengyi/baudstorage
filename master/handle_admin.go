package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"io"
	"net/http"
	"strconv"
	"strings"
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
	if vol, err = m.cluster.getVolByVolID(volID); err != nil {
		goto errDeal
	}
	vr = m.newVolResponseForGetVol(vol)
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

func (m *Master) newVolResponseForGetVol(v *VolGroup) (vr *VolResponse) {
	var (
		zone *Zone
		err  error
	)
	vr = new(VolResponse)
	v.Lock()
	defer v.Unlock()
	vr.VolID = v.VolID
	vr.Status = v.status
	vr.ReplicaNum = v.replicaNum
	vr.Hosts = make([]string, len(v.PersistenceHosts))
	copy(vr.Hosts, v.PersistenceHosts)
	vr.Zones = make([]string, 0)
	for _, volHostAddr := range v.PersistenceHosts {
		if _, zone, err = m.cluster.getDataNodeFromCluster(volHostAddr); err != nil {
			continue
		}
		vr.Zones = append(vr.Zones, zone.Name)
	}
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

	if v, err = c.getVolByVolID(volID); err != nil {
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
		v      *VolGroup
		addr   string
		volID  uint64
		err    error
	)

	if addr, volID, err = parseVolOfflinePara(r); err != nil {
		goto errDeal
	}

	if v, err = m.cluster.getVolByVolID(volID); err != nil {
		goto errDeal
	}

	v.volOffLine(addr, HandleVolOfflineErr)
	rstMsg = fmt.Sprintf(AdminVolOffline+"volID :%v  on node:%v  has offline success", volID, addr)
	io.WriteString(w, rstMsg)
	log.LogWarn(rstMsg)
	return
errDeal:
	logMsg := getReturnMessage(AdminVolOffline, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) handleAddZone(w http.ResponseWriter, r *http.Request) {
	var (
		msg      string
		zoneName string
		err      error
	)

	if zoneName, err = parseAddZonePara(r); err != nil {
		goto errDeal
	}

	if err = m.cluster.addZone(zoneName); err != nil {
		goto errDeal
	}

	msg = fmt.Sprintf("AddZone zone name:%v successed\n", zoneName)
	io.WriteString(w, msg)
	log.LogInfo(msg)

	return
errDeal:
	logMsg := getReturnMessage("AddZone", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, http.StatusBadRequest, w)
	return
}

func (m *Master) createNamespace(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		err  error
		msg  string
	)

	if name, err = parseCreateNamespacePara(r); err != nil {
		goto errDeal
	}
	if err = m.cluster.createNamespace(name); err != nil {
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

func parseCreateNamespacePara(r *http.Request) (name string, err error) {
	r.ParseForm()
	return checkNamespace(r)
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

func parseAddZonePara(r *http.Request) (zoneName string, err error) {
	r.ParseForm()
	if zoneName = r.FormValue(ParaZoneName); zoneName == "" {
		err = paraNotFound(ParaZoneName)
		return
	}

	if arr := strings.Split(zoneName, UnderlineSeparator); len(arr) != 2 {
		err = ParaFormatInvalid
		return
	}

	return
}
