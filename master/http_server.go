package master

import (
	"fmt"
	"net/http"

	"github.com/tiglabs/baudstorage/util/log"
)

const (
	// Admin APIs
	AdminGetVol          = "admin/getVol"
	AdminLoadVol         = "admin/loadVol"
	AdminCreateVol       = "admin/createVol"
	AdminVolOffline      = "admin/volOffline"
	AdminCreateNamespace = "admin/createNamespace"

	// Client APIs
	ClientVols      = "client/vols"
	ClientNamespace = "client/namespace"
	ClientMetaGroup = "client/metaGroup"

	// Node APIs
	AddDataNode               = "dataNode/add"
	AddMetaNode               = "metaNode/add"
	DataNodeOffline           = "admin/dataNodeOffline"
	MetaNodeOffline           = "admin/metaNodeOffline"
	GetDataNode               = "admin/getDataNode"
	GetMetaNode               = "admin/getMetaNode"
	AdminLoadMetaPartition    = "admin/loadMetaPartition"
	AdminMetaPartitionOffline = "admin/metaPartitionOffline"

	// Operation response
	MetaNodeResponse = "metaNode/response" // Method: 'POST', ContentType: 'application/json'
	DataNodeResponse = "dataNode/response" // Method: 'POST', ContentType: 'application/json'
)

func (m *Master) startHttpService() (err error) {
	go func() {
		m.handleFunctions()
		http.ListenAndServe(m.config.GetString(HttpPort), nil)
	}()
	return
}

func (m *Master) handleFunctions() (err error) {
	http.Handle(RootUrlPath, m.handlerWithInterceptor())
	return
}

func (m *Master) handlerWithInterceptor() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			//todo preInterceptor
			m.ServeHTTP(w, r)
		})
}

func (m *Master) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case AdminCreateVol:
		m.createVol(w, r)
	case GetDataNode:
		m.getDataNode(w, r)
	case GetMetaNode:
		m.getMetaNode(w, r)
	case AdminGetVol:
		m.getVol(w, r)
	case AdminLoadVol:
		m.loadVol(w, r)
	case AdminVolOffline:
		m.volOffline(w, r)
	case AdminCreateNamespace:
		m.createNamespace(w, r)
	case DataNodeOffline:
		m.dataNodeOffline(w, r)
	case MetaNodeOffline:
		m.metaNodeOffline(w, r)
	case AddDataNode:
		m.addDataNode(w, r)
	case AddMetaNode:
		m.addMetaNode(w, r)
	case ClientVols:
		m.getVols(w, r)
	case ClientNamespace:
		m.getNamespace(w, r)
	case ClientMetaGroup:
		m.getMetaPartition(w, r)
	case DataNodeResponse:
		m.dataNodeTaskResponse(w, r)
	case MetaNodeResponse:
		m.metaNodeTaskResponse(w, r)
	case AdminLoadMetaPartition:
		m.loadMetaPartition(w, r)
	case AdminMetaPartitionOffline:
		m.metaPartitionOffline(w, r)
	default:

	}
}

func getReturnMessage(requestType, remoteAddr, message string, code int) (logMsg string) {
	logMsg = fmt.Sprintf("type[%s] From [%s] Deal [%d] Because [%s] ", requestType, remoteAddr, code, message)

	return
}

func HandleError(message string, code int, w http.ResponseWriter) {
	log.LogError(message)
	http.Error(w, message, code)
}
