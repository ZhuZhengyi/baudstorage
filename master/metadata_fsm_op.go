package master

import (
	"encoding/json"
	"github.com/tiglabs/baudstorage/proto"
)

type Metadata struct {
	Op uint32 `json:"op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

func (m *Metadata) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Metadata) Unmarshal(data []byte) (err error) {
	return json.Unmarshal(data, m)
}

func (c *Cluster) addVolHosts() {

}

func (mf *MetadataFsm) CreateNameSpace(request proto.CreateNameSpaceRequest) (response proto.CreateNameSpaceResponse) {
	//	var (
	//		data     []byte
	//		err      error
	//		raftResp *raft.Future
	//	)
	//	//key:ns#name,value:nil
	//	kv := &kvp.Kv{Opt: OptSetNamespace}
	//	kv.K = encodeNameSpaceKey(request.Name)
	//	if data, err = pbproto.Marshal(kv); err != nil {
	//		err = fmt.Errorf("action[CreateNameSpace],marshal kv:%v,err:%v", kv, err.Error())
	//		goto errDeal
	//	}
	//	raftResp = mf.RaftStoreFsm.GetRaftServer().Submit(ClusterGroupID, data)
	//	if _, err = raftResp.Response(); err != nil {
	//		err = fmt.Errorf("action[CreateNameSpace],raft submit err:%v", err.Error())
	//		goto errDeal
	//	}
	//	response.Status = OK
	//	return
	//errDeal:
	//	response.Status = Failed
	//	response.Result = err.Error()
	return

}

func encodeNameSpaceKey(name string) string {
	return PrefixNameSpace + KeySeparator + name
}

func (mf *MetadataFsm) CreateMetaRange(request proto.CreateMetaPartitionRequest) (response proto.CreateMetaPartitionResponse) {
	return
}
