package master

import (
	"encoding/json"
	"github.com/juju/errors"
	"net/http"
	"regexp"
	"strconv"
)

type VolResponse struct {
	VolID      uint64
	Status     uint8
	ReplicaNum uint8
	VolType    string
	Hosts      []string
}

type VolsView struct {
	Vols []*VolResponse
}

func NewVolsView() (volsView *VolsView) {
	volsView = new(VolsView)
	volsView.Vols = make([]*VolResponse, 0)
	return
}

type MetaPartitionView struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
}

type NamespaceView struct {
	Name           string
	MetaPartitions []*MetaPartitionView
	VolGroups      []*VolResponse
}

func NewNamespaceView(name string) (view *NamespaceView) {
	view = new(NamespaceView)
	view.Name = name
	view.MetaPartitions = make([]*MetaPartitionView, 0)
	view.VolGroups = make([]*VolResponse, 0)
	return
}

func NewMetaGroupView(partitionID uint64, start, end uint64) (mpView *MetaPartitionView) {
	mpView = new(MetaPartitionView)
	mpView.PartitionID = partitionID
	mpView.Start = start
	mpView.End = end
	mpView.Members = make([]string, 0)
	return
}

func (m *Master) getVols(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code int
		name string
		ns   *NameSpace
		ok   bool
		err  error
	)
	if name, err = parseGetNamespacePara(r); err != nil {
		goto errDeal
	}
	if ns, ok = m.cluster.namespaces[name]; !ok {
		err = NamespaceNotFound
		goto errDeal
	}

	if body, err = ns.getVolsView(); err != nil {
		code = http.StatusMethodNotAllowed
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getVols", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func (m *Master) getNamespace(w http.ResponseWriter, r *http.Request) {
	var (
		body      []byte
		code      int
		err       error
		name      string
		namespace *NameSpace
		ok        bool
	)
	if name, err = parseGetNamespacePara(r); err != nil {
		goto errDeal
	}
	if namespace, ok = m.cluster.namespaces[name]; !ok {
		err = NamespaceNotFound
		goto errDeal
	}
	if body, err = json.Marshal(getNamespaceView(namespace)); err != nil {
		code = http.StatusMethodNotAllowed
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getNamespace", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func getNamespaceView(ns *NameSpace) (view *NamespaceView) {
	view = NewNamespaceView(ns.Name)
	for _, mp := range ns.MetaPartitions {
		view.MetaPartitions = append(view.MetaPartitions, getMetaPartitionView(mp))
	}
	view.VolGroups = ns.volGroups.GetVolsView(0)
	return
}

func getMetaPartitionView(mp *MetaPartition) (mpView *MetaPartitionView) {
	mpView = NewMetaGroupView(mp.PartitionID, mp.Start, mp.End)
	for _, metaReplica := range mp.Replicas {
		mpView.Members = append(mpView.Members, metaReplica.Addr)
		if metaReplica.IsLeader {
			mpView.LeaderAddr = metaReplica.Addr
		}
	}
	return
}

func (m *Master) getMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		body        []byte
		code        int
		err         error
		name        string
		partitionID uint64
		namespace   *NameSpace
		mp          *MetaPartition
		ok          bool
	)
	if name, partitionID, err = parseGetMetaPartitionPara(r); err != nil {
		goto errDeal
	}
	if namespace, ok = m.cluster.namespaces[name]; !ok {
		err = NamespaceNotFound
		goto errDeal
	}
	if mp, ok = namespace.MetaPartitions[partitionID]; !ok {
		err = MetaGroupNotFound
		goto errDeal
	}
	if body, err = json.Marshal(mp); err != nil {
		code = http.StatusMethodNotAllowed
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getMetaPartition", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func parseGetMetaPartitionPara(r *http.Request) (name string, partitionID uint64, err error) {
	r.ParseForm()
	if name, err = checkNamespace(r); err != nil {
		return
	}
	if partitionID, err = checkMetaPartitionID(r); err != nil {
		return
	}
	return
}

func checkMetaPartitionID(r *http.Request) (partitionID uint64, err error) {
	var value string
	if value = r.FormValue(ParaId); value == "" {
		err = paraNotFound(ParaId)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseGetNamespacePara(r *http.Request) (name string, err error) {
	r.ParseForm()
	return checkNamespace(r)
}

func checkNamespace(r *http.Request) (name string, err error) {
	if name = r.FormValue(ParaName); name == "" {
		err = paraNotFound(name)
	}

	pattern := "^[a-zA-Z0-9]{3,256}$"
	reg, err := regexp.Compile(pattern)
	if err == nil {
		return "", err
	}

	if !reg.MatchString(name) {
		return "", errors.New("name can only be number and letters")
	}

	return
}
