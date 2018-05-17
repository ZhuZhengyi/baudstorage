package datanode

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"util/ump"
)

var (
	taskRecvCh = make(chan *Cmd, DefaultTaskChanLen)
	taskRespCh = make(chan *CmdResp, DefaultTaskChanLen)
)

func (s *DataNode) handleCmds() {
	for {
		select {
		case cmd := <-taskRecvCh:
			resp := s.processCmd(cmd)
			taskRespCh <- resp
		}
	}
	ump.Alarm(UmpKeyCmds, UmpDetailCmds)
}

func (s *DataNode) replyCmdResp(resp *CmdResp) (msg []byte, err error) {
	data, err := MarshalCmdResp(resp)
	if err != nil {
		glog.LogError("Failed to marshal CmdResp, err: ", err.Error())
		return
	}

	glog.LogInfo("reply cmdresp, response data: ", string(data))
	msg, err = PostToMaster(data, "/node/taskresponse?cluster="+s.clusterId)

	return
}

func (s *DataNode) handleCmdResps() {
	for {
		select {
		case resp := <-taskRespCh:
			rpl, err := s.replyCmdResp(resp)
			if err != nil {
				glog.LogError("Failed to post cmd response to master, and put cmdreps back to resp chan, err: ", err.Error())
			}
			glog.LogInfo("reply cmdresp, reply msg: ", string(rpl))
		}
	}
	ump.Alarm(UmpKeyCmds, UmpDetailCmds)
}

func rmRepeat(resp *CmdResp, cmdResps []*CmdResp) (replace bool) {
	for i, r := range cmdResps {
		if r.ID == resp.ID {
			cmdResps[i] = resp
			replace = true
			break
		}
	}
	return
}

func getRequestBody(r *http.Request) (body []byte, err error) {
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		return nil, fmt.Errorf(LogTask+" Read Body Error:%v ", err.Error())
	}
	return
}

func getReturnMessage(requestType, remoteAddr, message string, code int) (logMsg string) {
	logMsg = fmt.Sprintf("type[%s] From [%s] Deal [%d] Because [%s] ", requestType, remoteAddr, code, message)
	return
}

func handleError(message string, code int, w http.ResponseWriter) {
	glog.LogError(message)
	http.Error(w, message, code)
}

func HandleTasks(w http.ResponseWriter, r *http.Request) {
	var cmd *Cmd
	reqBody, err := getRequestBody(r)
	if err != nil {
		glog.LogError(LogTask, "Failed to get Cmd data from http request from master, err: ", err.Error())
		goto errDeal
	}
	cmd, err = UnmarshalCmd(reqBody)
	if err != nil {
		glog.LogError(LogTask, "Failed to get Cmd data from http request from master, err: ", err.Error())
		goto errDeal
	}
	taskRecvCh <- cmd
	return

errDeal:
	logMsg := getReturnMessage(LogTask, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	handleError(logMsg, http.StatusBadRequest, w)
}
