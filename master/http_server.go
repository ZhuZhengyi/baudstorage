package master

import (
	"fmt"
	"net/http"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/util/log"
)

const (
	// Admin APIs
	AdminGetCluster           = "/admin/getCluster"
	AdminGetDataPartition     = "/dataPartition/get"
	AdminLoadDataPartition    = "/dataPartition/load"
	AdminCreateDataPartition  = "/dataPartition/create"
	AdminDataPartitionOffline = "/dataPartition/offline"
	AdminCreateNamespace      = "/admin/createNamespace"
	AdminGetIp                = "/admin/getIp"

	// Client APIs
	ClientVols          = "/client/dataPartitions"
	ClientNamespace     = "/client/namespace"
	ClientMetaPartition = "/client/metaPartition"

	// Node APIs
	AddDataNode               = "/dataNode/add"
	DataNodeOffline           = "/dataNode/offline"
	GetDataNode               = "/dataNode/get"
	AddMetaNode               = "/metaNode/add"
	MetaNodeOffline           = "/metaNode/offline"
	GetMetaNode               = "/metaNode/get"
	AdminLoadMetaPartition    = "/metaPartition/load"
	AdminMetaPartitionOffline = "/metaPartition/offline"

	// Operation response
	MetaNodeResponse = "/metaNode/response" // Method: 'POST', ContentType: 'application/json'
	DataNodeResponse = "/dataNode/response" // Method: 'POST', ContentType: 'application/json'
)

func (m *Master) startHttpService() (err error) {
	go func() {
		m.handleFunctions()
		http.ListenAndServe(ColonSplit+m.port, nil)
	}()
	return
}

func (m *Master) handleFunctions() {
	http.HandleFunc(AdminGetIp, m.getIpAndClusterName)
	http.Handle(AdminGetDataPartition, m.handlerWithInterceptor())
	http.Handle(AdminCreateDataPartition, m.handlerWithInterceptor())
	http.Handle(AdminLoadDataPartition, m.handlerWithInterceptor())
	http.Handle(AdminDataPartitionOffline, m.handlerWithInterceptor())
	http.Handle(AdminCreateNamespace, m.handlerWithInterceptor())
	http.Handle(AddDataNode, m.handlerWithInterceptor())
	http.Handle(AddMetaNode, m.handlerWithInterceptor())
	http.Handle(DataNodeOffline, m.handlerWithInterceptor())
	http.Handle(MetaNodeOffline, m.handlerWithInterceptor())
	http.Handle(GetDataNode, m.handlerWithInterceptor())
	http.Handle(GetMetaNode, m.handlerWithInterceptor())
	//http.Handle(AdminLoadMetaPartition, m.handlerWithInterceptor())
	http.Handle(AdminMetaPartitionOffline, m.handlerWithInterceptor())
	http.Handle(ClientVols, m.handlerWithInterceptor())
	http.Handle(ClientNamespace, m.handlerWithInterceptor())
	http.Handle(ClientMetaPartition, m.handlerWithInterceptor())
	http.Handle(DataNodeResponse, m.handlerWithInterceptor())
	http.Handle(MetaNodeResponse, m.handlerWithInterceptor())
	http.Handle(AdminGetCluster, m.handlerWithInterceptor())
	return
}

func (m *Master) handlerWithInterceptor() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if m.partition.IsLeader() {
				m.ServeHTTP(w, r)
			} else {
				http.Error(w, m.leaderInfo.addr, http.StatusForbidden)
			}
		})
}

func (m *Master) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case AdminGetCluster:
		m.getCluster(w, r)
	case AdminCreateDataPartition:
		m.createDataPartition(w, r)
	case GetDataNode:
		m.getDataNode(w, r)
	case GetMetaNode:
		m.getMetaNode(w, r)
	case AdminGetDataPartition:
		m.getDataPartition(w, r)
	case AdminLoadDataPartition:
		m.loadDataPartition(w, r)
	case AdminDataPartitionOffline:
		m.dataPartitionOffline(w, r)
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
		m.getDataPartitions(w, r)
	case ClientNamespace:
		m.getNamespace(w, r)
	case ClientMetaPartition:
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

func HandleError(message string, err error, code int, w http.ResponseWriter) {
	log.LogErrorf("errMsg:%v errStack:%v", message, errors.ErrorStack(err))
	http.Error(w, message, code)
}
