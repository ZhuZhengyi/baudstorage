package master

import (
	"encoding/json"
	"net/http"
)

type VolResponse struct {
	VolID      uint64
	Status     uint8
	ReplicaNum uint8
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

type MetaGroupView struct {
	GroupID uint64
	Start   uint64
	End     uint64
	Members []string
}

type NamespaceView struct {
	Name       string
	MetaGroups []*MetaGroupView
	VolGroups  []*VolResponse
}

func NewNamespaceView(name string) (view *NamespaceView) {
	view = new(NamespaceView)
	view.Name = name
	view.MetaGroups = make([]*MetaGroupView, 0)
	view.VolGroups = make([]*VolResponse, 0)
	return
}

func NewMetaGroupView(groupID uint64, start, end uint64) (mgView *MetaGroupView) {
	mgView = new(MetaGroupView)
	mgView.GroupID = groupID
	mgView.Start = start
	mgView.End = end
	mgView.Members = make([]string, 0)
	return
}

func (m *Master) getVols(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code int
		err  error
	)
	if body, err = m.cluster.getVolsView(); err != nil {
		code = http.StatusMethodNotAllowed
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getVols", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, code, w)
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
	HandleError(logMsg, code, w)
	return
}

func getNamespaceView(ns *NameSpace) (view *NamespaceView) {
	view = NewNamespaceView(ns.Name)
	for _, metaGroup := range ns.MetaGroups {
		view.MetaGroups = append(view.MetaGroups, getMetaGroupView(metaGroup))
	}
	view.VolGroups = ns.volGroups.GetVolsView(0)
	return
}

func getMetaGroupView(metaGroup *MetaGroup) (mgView *MetaGroupView) {
	mgView = NewMetaGroupView(metaGroup.GroupID, metaGroup.Start, metaGroup.End)
	for _, metaRange := range metaGroup.Members {
		mgView.Members = append(mgView.Members, metaRange.Addr)
	}
	return
}

func (m *Master) getMetaGroup(w http.ResponseWriter, r *http.Request) {
	var (
		body          []byte
		code          int
		err           error
		name, groupId string
		namespace     *NameSpace
		metaGroup     *MetaGroup
		ok            bool
	)
	if name, groupId, err = parseGetMetaGroupPara(r); err != nil {
		goto errDeal
	}
	if namespace, ok = m.cluster.namespaces[name]; !ok {
		err = NamespaceNotFound
		goto errDeal
	}
	if metaGroup, ok = namespace.MetaGroups[groupId]; !ok {
		err = MetaGroupNotFound
		goto errDeal
	}
	if body, err = json.Marshal(getMetaGroupView(metaGroup)); err != nil {
		code = http.StatusMethodNotAllowed
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getMetaGroup", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, code, w)
	return
}

func parseGetMetaGroupPara(r *http.Request) (name, groupId string, err error) {
	r.ParseForm()
	if name, err = checkNamespace(r); err != nil {
		return
	}
	if groupId := r.FormValue(ParaId); groupId == "" {
		err = paraNotFound(ParaId)
		return
	}
	return
}

func parseGetNamespacePara(r *http.Request) (name string, err error) {
	r.ParseForm()
	return checkNamespace(r)
}

func checkNamespace(r *http.Request) (name string, err error) {
	if name := r.FormValue(ParaName); name == "" {
		err = paraNotFound(name)
	}
	return
}
