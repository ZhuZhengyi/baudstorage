package master

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	OpSyncAddMetaNode         uint32 = 0x01
	OpSyncAddDataNode         uint32 = 0x02
	OpSyncAddVolGroup         uint32 = 0x03
	OpSyncAddNamespace        uint32 = 0x04
	OpSyncAddMetaPartition    uint32 = 0x05
	OpSyncUpdateVolGroup      uint32 = 0x06
	OpSyncUpdateMetaPartition uint32 = 0x07
)

const (
	KeySeparator         = "#"
	MetaNodeAcronym      = "mn"
	DataNodeAcronym      = "dn"
	VolGroupAcronym      = "vg"
	MetaPartitionAcronym = "mp"
	NamespaceAcronym     = "ns"
	MetaNodePrefix       = KeySeparator + MetaNodeAcronym + KeySeparator
	DataNodePrefix       = KeySeparator + DataNodeAcronym + KeySeparator
	VolGroupPrefix       = KeySeparator + VolGroupAcronym + KeySeparator
	NamespacePrefix      = KeySeparator + NamespaceAcronym + KeySeparator
	MetaPartitionPrefix  = KeySeparator + MetaPartitionAcronym + KeySeparator
)

type MetaPartitionValue struct {
	PartitionID uint64
	ReplicaNum  uint8
	Start       uint64
	End         uint64
	Hosts       string
}

func newMetaPartitionValue(mp *MetaPartition) (mpv *MetaPartitionValue) {
	mpv = &MetaPartitionValue{
		PartitionID: mp.PartitionID,
		ReplicaNum:  mp.replicaNum,
		Start:       mp.Start,
		End:         mp.End,
		Hosts:       mp.hostsToString(),
	}
	return
}

type VolGroupValue struct {
	VolID      uint64
	ReplicaNum uint8
	Hosts      string
}

func newVolGroupValue(vg *VolGroup) (vgv *VolGroupValue) {
	vgv = &VolGroupValue{
		VolID:      vg.VolID,
		ReplicaNum: vg.replicaNum,
		Hosts:      vg.VolHostsToString(),
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

//key=#vg#nsName#volID,value=json.Marshal(VolGroupValue)
func (c *Cluster) syncAddVolGroup(nsName string, vg *VolGroup) (err error) {
	return c.putVolGroupInfo(OpSyncAddVolGroup, nsName, vg)
}

func (c *Cluster) syncUpdateVolGroup(nsName string, vg *VolGroup) (err error) {
	return c.putVolGroupInfo(OpSyncUpdateVolGroup, nsName, vg)
}

func (c *Cluster) putVolGroupInfo(opType uint32, nsName string, vg *VolGroup) (err error) {
	metadata := new(Metadata)
	metadata.Op = opType
	metadata.K = VolGroupPrefix + nsName + KeySeparator + strconv.FormatUint(vg.VolID, 10)
	vgv := newVolGroupValue(vg)
	metadata.V, err = json.Marshal(vgv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) submit(metadata *Metadata) (err error) {
	cmd, err := metadata.Marshal()
	if err != nil {
		return
	}
	if _, err := c.partition.Submit(cmd); err != nil {
		return
	}
	return
}

//key=#ns#nsName#volReplicaNum,value=nil
func (c *Cluster) syncAddNamespace(ns *NameSpace) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddNamespace
	metadata.K = NamespacePrefix + ns.Name + strconv.FormatUint(uint64(ns.volReplicaNum), 10)
	return c.submit(metadata)
}

////key=#mp#nsName#partitionID,value=json.Marshal(MetaPartitionValue)
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
		return
	}
	return c.submit(metadata)
}

//key=#mn#id#addr,value = nil
func (c *Cluster) syncAddMetaNode(metaNode *MetaNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddMetaNode
	metadata.K = MetaNodePrefix + strconv.FormatUint(metaNode.id, 10) + metaNode.Addr
	return c.submit(metadata)
}

//key=#dn#httpAddr,value = nil
func (c *Cluster) syncAddDataNode(dataNode *DataNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddDataNode
	metadata.K = DataNodePrefix + dataNode.HttpAddr
	return c.submit(metadata)
}

func (c *Cluster) handleApply(cmd *Metadata) (err error) {
	if cmd == nil {
		return fmt.Errorf("metadata can't be null")
	}

	switch cmd.Op {
	case OpSyncAddDataNode:
		c.applyAddDataNode(cmd)
	case OpSyncAddMetaNode:
		c.applyAddMetaNode(cmd)
	case OpSyncAddNamespace:
		c.applyAddNamespace(cmd)
	case OpSyncAddMetaPartition:
		c.applyAddMetaPartition(cmd)
	case OpSyncAddVolGroup:
		c.applyAddVolGroup(cmd)
	}
	return
}

func (c *Cluster) applyAddDataNode(cmd *Metadata) {
	keys := strings.Split(cmd.K, KeySeparator)

	if keys[1] == DataNodeAcronym {
		dataNode := NewDataNode(keys[2])
		c.dataNodes.Store(dataNode.HttpAddr, dataNode)
	}
}

func (c *Cluster) applyAddMetaNode(cmd *Metadata) {
	keys := strings.Split(cmd.K, KeySeparator)

	if keys[1] == MetaNodeAcronym {
		metaNode := NewMetaNode(keys[2])
		c.dataNodes.Store(metaNode.Addr, metaNode)
	}
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
		json.Unmarshal(cmd.V, mpv)
		//todo add replicaNum
		mp := NewMetaPartition(mpv.PartitionID, mpv.Start, mpv.End)
		ns, _ := c.getNamespace(keys[2])
		ns.MetaPartitions[mp.PartitionID] = mp
	}
}

func (c *Cluster) applyAddVolGroup(cmd *Metadata) {
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == VolGroupAcronym {
		vgv := &VolGroupValue{}
		json.Unmarshal(cmd.V, vgv)
		vg := newVolGroup(vgv.VolID, vgv.ReplicaNum)
		ns, _ := c.getNamespace(keys[2])
		ns.volGroups.volGroups = append(ns.volGroups.volGroups, vg)

	}
}
