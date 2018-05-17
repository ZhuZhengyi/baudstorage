package master

import (
	"encoding/json"
	"strconv"
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
	MetaNodePrefix      = "#mn#"
	DataNodePrefix      = "#dn#"
	VolGroupPrefix      = "#vg#"
	NamespacePrefix     = "#ns#"
	MetaPartitionPrefix = "#mp#"
)

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

/**
key=#vg#nsName#volID,value=replicaNum#hosts
*/
func (c *Cluster) syncAddVolGroup(nsName string, vg *VolGroup) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddVolGroup
	metadata.K = VolGroupPrefix + nsName + KeySeparator + strconv.FormatUint(vg.VolID, 10)
	metadata.V = []byte(strconv.FormatUint(uint64(vg.replicaNum), 10) + KeySeparator + vg.VolHostsToString())
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

func (c *Cluster) syncUpdateVolGroupHosts(nsName string, vg *VolGroup) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncUpdateVolGroup
	metadata.K = VolGroupPrefix + nsName + KeySeparator + strconv.FormatUint(vg.VolID, 10)
	metadata.V = []byte(strconv.FormatUint(uint64(vg.replicaNum), 10) + KeySeparator + vg.VolHostsToString())
	return c.submit(metadata)
}

func (c *Cluster) syncAddNamespace(ns *NameSpace) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddNamespace
	metadata.K = NamespacePrefix + ns.Name
	return c.submit(metadata)
}

func (c *Cluster) syncAddMetaPartition(nsName string, mp *MetaPartition) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddMetaPartition
	metadata.K = MetaPartitionPrefix + nsName + KeySeparator + strconv.FormatUint(mp.PartitionID, 10)
	metadata.V = []byte(strconv.FormatUint(uint64(mp.replicaNum), 10) + KeySeparator + mp.hostsToString())
	return c.submit(metadata)
}

func (c *Cluster) syncUpdateMetaPartition(nsName string, mp *MetaPartition) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncUpdateMetaPartition
	metadata.K = MetaPartitionPrefix + nsName + KeySeparator + strconv.FormatUint(mp.PartitionID, 10)
	metadata.V = []byte(strconv.FormatUint(uint64(mp.replicaNum), 10) + KeySeparator + mp.hostsToString())
	return c.submit(metadata)
}

func (c *Cluster) syncAddMetaNode(metaNode *MetaNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddMetaNode
	metadata.K = MetaNodePrefix + strconv.FormatUint(metaNode.id, 10)
	metadata.V = []byte(metaNode.Addr)
	return c.submit(metadata)
}

func (c *Cluster) syncAddDataNode(dataNode *DataNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddDataNode
	metadata.K = DataNodePrefix + dataNode.HttpAddr
	return c.submit(metadata)
}
