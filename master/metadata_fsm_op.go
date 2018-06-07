package master

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	bsProto "github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

const (
	OpSyncAddMetaNode         uint32 = 0x01
	OpSyncAddDataNode         uint32 = 0x02
	OpSyncAddDataPartition    uint32 = 0x03
	OpSyncAddNamespace        uint32 = 0x04
	OpSyncAddMetaPartition    uint32 = 0x05
	OpSyncUpdateDataPartition uint32 = 0x06
	OpSyncUpdateMetaPartition uint32 = 0x07
	OpSyncDeleteDataNode      uint32 = 0x08
	OpSyncDeleteMetaNode      uint32 = 0x09
)

const (
	KeySeparator         = "#"
	MetaNodeAcronym      = "mn"
	DataNodeAcronym      = "dn"
	DataPartitionAcronym = "vg"
	MetaPartitionAcronym = "mp"
	NamespaceAcronym     = "ns"
	MetaNodePrefix       = KeySeparator + MetaNodeAcronym + KeySeparator
	DataNodePrefix       = KeySeparator + DataNodeAcronym + KeySeparator
	DataPartitionPrefix  = KeySeparator + DataPartitionAcronym + KeySeparator
	NamespacePrefix      = KeySeparator + NamespaceAcronym + KeySeparator
	MetaPartitionPrefix  = KeySeparator + MetaPartitionAcronym + KeySeparator
)

type MetaPartitionValue struct {
	PartitionID uint64
	ReplicaNum  uint8
	Start       uint64
	End         uint64
	Hosts       string
	Peers       []bsProto.Peer
}

func newMetaPartitionValue(mp *MetaPartition) (mpv *MetaPartitionValue) {
	mpv = &MetaPartitionValue{
		PartitionID: mp.PartitionID,
		ReplicaNum:  mp.CurReplicaNum,
		Start:       mp.Start,
		End:         mp.End,
		Hosts:       mp.hostsToString(),
		Peers:       mp.Peers,
	}
	return
}

type DataPartitionValue struct {
	PartitionID   uint64
	ReplicaNum    uint8
	Hosts         string
	PartitionType string
}

func newDataPartitionValue(vg *DataPartition) (dpv *DataPartitionValue) {
	dpv = &DataPartitionValue{
		PartitionID:   vg.PartitionID,
		ReplicaNum:    vg.ReplicaNum,
		Hosts:         vg.HostsToString(),
		PartitionType: vg.PartitionType,
	}
	return
}

type Metadata struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

func (m *Metadata) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Metadata) Unmarshal(data []byte) (err error) {
	return json.Unmarshal(data, m)
}

//key=#vg#nsName#partitionID,value=json.Marshal(DataPartitionValue)
func (c *Cluster) syncAddDataPartition(nsName string, dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncAddDataPartition, nsName, dp)
}

func (c *Cluster) syncUpdateDataPartition(nsName string, dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncUpdateDataPartition, nsName, dp)
}

func (c *Cluster) putDataPartitionInfo(opType uint32, nsName string, dp *DataPartition) (err error) {
	metadata := new(Metadata)
	metadata.Op = opType
	metadata.K = DataPartitionPrefix + nsName + KeySeparator + strconv.FormatUint(dp.PartitionID, 10)
	dpv := newDataPartitionValue(dp)
	metadata.V, err = json.Marshal(dpv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) submit(metadata *Metadata) (err error) {
	cmd, err := metadata.Marshal()
	if err != nil {
		return errors.New(err.Error())
	}
	if _, err = c.partition.Submit(cmd); err != nil {
		msg := fmt.Sprintf("action[metadata_submit] err:%v", err.Error())
		return errors.New(msg)
	}
	return
}

//key=#ns#nsName#dpReplicaNum,value=nil
func (c *Cluster) syncAddNamespace(ns *NameSpace) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddNamespace
	metadata.K = NamespacePrefix + ns.Name + KeySeparator + strconv.FormatUint(uint64(ns.dpReplicaNum), 10)
	return c.submit(metadata)
}

////key=#mp#nsName#metaPartitionID,value=json.Marshal(MetaPartitionValue)
func (c *Cluster) syncAddMetaPartition(nsName string, mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncAddMetaPartition, nsName, mp)
}

func (c *Cluster) syncUpdateMetaPartition(nsName string, mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncUpdateMetaPartition, nsName, mp)
}

func (c *Cluster) putMetaPartitionInfo(opType uint32, nsName string, mp *MetaPartition) (err error) {
	metadata := new(Metadata)
	metadata.Op = opType
	partitionID := strconv.FormatUint(mp.PartitionID, 10)
	metadata.K = MetaPartitionPrefix + nsName + KeySeparator + partitionID
	mpv := newMetaPartitionValue(mp)
	if metadata.V, err = json.Marshal(mpv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

//key=#mn#id#addr,value = nil
func (c *Cluster) syncAddMetaNode(metaNode *MetaNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddMetaNode
	metadata.K = MetaNodePrefix + strconv.FormatUint(metaNode.ID, 10) + KeySeparator + metaNode.Addr
	return c.submit(metadata)
}

func (c *Cluster) syncDeleteMetaNode(metaNode *MetaNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncDeleteMetaNode
	metadata.K = MetaNodePrefix + strconv.FormatUint(metaNode.ID, 10) + KeySeparator + metaNode.Addr
	return c.submit(metadata)
}

//key=#dn#httpAddr,value = nil
func (c *Cluster) syncAddDataNode(dataNode *DataNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddDataNode
	metadata.K = DataNodePrefix + dataNode.Addr
	return c.submit(metadata)
}

func (c *Cluster) syncDeleteDataNode(dataNode *DataNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncDeleteDataNode
	metadata.K = DataNodePrefix + dataNode.Addr
	return c.submit(metadata)
}

func (c *Cluster) addRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfAddNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[addRaftNode] error: " + err.Error())
	}
	return nil
}

func (c *Cluster) removeRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfRemoveNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[removeRaftNode] error: " + err.Error())
	}
	return nil
}

func (c *Cluster) handleApply(cmd *Metadata) (err error) {
	if cmd == nil {
		return fmt.Errorf("metadata can't be null")
	}
	if c.partition.IsLeader() {
		return
	}
	switch cmd.Op {
	case OpSyncAddDataNode:
		c.applyAddDataNode(cmd)
	case OpSyncAddMetaNode:
		err = c.applyAddMetaNode(cmd)
	case OpSyncAddNamespace:
		c.applyAddNamespace(cmd)
	case OpSyncAddMetaPartition:
		c.applyAddMetaPartition(cmd)
	case OpSyncAddDataPartition:
		c.applyAddDataPartition(cmd)
	case OpSyncDeleteMetaNode:
		c.applyDeleteMetaNode(cmd)
	case OpSyncDeleteDataNode:
		c.applyDeleteDataNode(cmd)
	}
	return
}

func (c *Cluster) applyDeleteDataNode(cmd *Metadata) {
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == DataNodeAcronym {
		c.dataNodes.Delete(keys[2])
	}
}

func (c *Cluster) applyDeleteMetaNode(cmd *Metadata) {
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == DataNodeAcronym {
		c.metaNodes.Delete(keys[3])
	}
}

func (c *Cluster) applyAddDataNode(cmd *Metadata) {
	keys := strings.Split(cmd.K, KeySeparator)

	if keys[1] == DataNodeAcronym {
		dataNode := NewDataNode(keys[2], c.Name)
		c.dataNodes.Store(dataNode.Addr, dataNode)
	}
}

func (c *Cluster) applyAddMetaNode(cmd *Metadata) (err error) {
	keys := strings.Split(cmd.K, KeySeparator)
	var (
		id uint64
	)
	if keys[1] == MetaNodeAcronym {
		addr := keys[3]
		if _, err = c.getMetaNode(addr); err != nil {
			metaNode := NewMetaNode(addr, c.Name)
			if id, err = strconv.ParseUint(keys[2], 10, 64); err != nil {
				return
			}
			metaNode.ID = id
			c.metaNodes.Store(metaNode.Addr, metaNode)
		}
	}
	return nil
}

func (c *Cluster) applyAddNamespace(cmd *Metadata) {
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == NamespaceAcronym {
		replicaNum, _ := strconv.ParseUint(keys[3], 10, 8)
		ns := NewNameSpace(keys[2], uint8(replicaNum))
		c.namespaces[ns.Name] = ns
	}
}

func (c *Cluster) applyAddMetaPartition(cmd *Metadata) {
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == MetaPartitionAcronym {
		mpv := &MetaPartitionValue{}
		if err := json.Unmarshal(cmd.V, mpv); err != nil {
			log.LogError(fmt.Sprintf("action[applyAddMetaPartition] failed,err:%v", err))
		}
		mp := NewMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, keys[2])
		mp.Peers = mpv.Peers
		mp.PersistenceHosts = strings.Split(mpv.Hosts, UnderlineSeparator)
		ns, _ := c.getNamespace(keys[2])
		ns.MetaPartitions[mp.PartitionID] = mp
	}
}

func (c *Cluster) applyAddDataPartition(cmd *Metadata) {
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == DataPartitionAcronym {
		dpv := &DataPartitionValue{}
		json.Unmarshal(cmd.V, dpv)
		dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, dpv.PartitionType)
		ns, _ := c.getNamespace(keys[2])
		dp.PersistenceHosts = strings.Split(dpv.Hosts, UnderlineSeparator)
		ns.dataPartitions.putDataPartition(dp)
	}
}

func (c *Cluster) decodeDataPartitionKey(key string) (acronym, nsName string) {
	return c.decodeAcronymAndNsName(key)
}

func (c *Cluster) decodeMetaPartitionKey(key string) (acronym, nsName string) {
	return c.decodeAcronymAndNsName(key)
}

func (c *Cluster) decodeNamespaceKey(key string) (acronym, nsName string, replicaNum uint8, err error) {
	arr := strings.Split(key, KeySeparator)
	acronym = arr[1]
	nsName = arr[2]
	replicaNumUint8, err := strconv.ParseUint(arr[3], 10, 8)
	replicaNum = uint8(replicaNumUint8)
	return
}

func (c *Cluster) decodeAcronymAndNsName(key string) (acronym, nsName string) {
	arr := strings.Split(key, KeySeparator)
	acronym = arr[1]
	nsName = arr[2]
	return
}

func (c *Cluster) loadDataNodes() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(DataNodePrefix)
	it.Seek(prefixKey)

	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		keys := strings.Split(string(encodedKey.Data()), KeySeparator)
		dataNode := NewDataNode(keys[2], c.Name)
		c.dataNodes.Store(dataNode.Addr, dataNode)
		encodedKey.Free()
	}
	return
}

func (c *Cluster) decodeMetaNodeKey(key string) (nodeID uint64, addr string, err error) {
	keys := strings.Split(key, KeySeparator)
	addr = keys[3]
	nodeID, err = strconv.ParseUint(keys[2], 10, 64)
	return
}

func (c *Cluster) loadMetaNodes() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(MetaNodePrefix)
	it.Seek(prefixKey)

	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		nodeID, addr, err := c.decodeMetaNodeKey(string(encodedKey.Data()))
		if err != nil {
			err = fmt.Errorf("action[loadMetaNodes] err:%v", err.Error())
			return err
		}
		metaNode := NewMetaNode(addr, c.Name)
		metaNode.ID = nodeID
		c.metaNodes.Store(addr, metaNode)
		encodedKey.Free()
	}
	return
}

func (c *Cluster) loadNamespaces() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(NamespacePrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		_, nsName, replicaNum, err := c.decodeNamespaceKey(string(encodedKey.Data()))
		if err != nil {
			err = fmt.Errorf("action[loadNamespaces] err:%v", err.Error())
			return err
		}
		ns := NewNameSpace(nsName, replicaNum)
		c.namespaces[nsName] = ns
		encodedKey.Free()
	}
	return
}

func (c *Cluster) loadMetaPartitions() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(MetaPartitionPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		encodedValue := it.Value()
		_, nsName := c.decodeMetaPartitionKey(string(encodedKey.Data()))
		ns, err := c.getNamespace(nsName)
		if err != nil {
			err = fmt.Errorf("action[loadMetaPartitions] err:%v", err.Error())
			return err
		}
		mpv := &MetaPartitionValue{}
		if err = json.Unmarshal(encodedValue.Data(), mpv); err != nil {
			err = fmt.Errorf("action[decodeMetaPartitionValue],value:%v,err:%v", encodedValue.Data(), err)
			return err
		}
		mp := NewMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, nsName)
		mp.setPersistenceHosts(strings.Split(mpv.Hosts, UnderlineSeparator))
		mp.setPeers(mpv.Peers)
		ns.MetaPartitions[mp.PartitionID] = mp
		encodedKey.Free()
		encodedValue.Free()
	}
	return
}

func (c *Cluster) loadDataPartitions() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(DataPartitionPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		encodedValue := it.Value()
		_, nsName := c.decodeDataPartitionKey(string(encodedKey.Data()))
		ns, err := c.getNamespace(nsName)
		if err != nil {
			err = fmt.Errorf("action[loadDataPartitions] err:%v", err.Error())
			return err
		}
		vgv := &DataPartitionValue{}
		if err = json.Unmarshal(encodedValue.Data(), vgv); err != nil {
			err = fmt.Errorf("action[decodeVolValue],value:%v,err:%v", encodedValue.Data(), err)
			return err
		}
		vg := newDataPartition(vgv.PartitionID, vgv.ReplicaNum, vgv.PartitionType)
		vg.PersistenceHosts = strings.Split(vgv.Hosts, UnderlineSeparator)
		ns.dataPartitions.putDataPartition(vg)
		encodedKey.Free()
		encodedValue.Free()
	}
	return
}
