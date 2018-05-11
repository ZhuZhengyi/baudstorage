package master

import (
	"fmt"

	"github.com/juju/errors"
)

var (
	NoAvailVol          = errors.New("no avail vol")
	VolNotFound         = errors.New("vol not found")
	DataNodeNotFound    = errors.New("data node not found")
	MetaNodeNotFound    = errors.New("meta node not found")
	NamespaceNotFound   = errors.New("namespace not found")
	MetaGroupNotFound   = errors.New("metaGroup not found")
	VolLocationNotFound = errors.New("volume server not found")
	UnMatchPara         = errors.New("para not unmatched")

	DisOrderArrayErr              = errors.New("dis order array is nil")
	VolReplicationExcessError     = errors.New("vol Replication Excess error")
	VolReplicationLackError       = errors.New("vol Replication Lack error")
	VolReplicationHasMissOneError = errors.New("vol replication has miss one ,cannot miss any one")
	VolPersistedNotAnyReplicates  = errors.New("volume persisted not have any replicates")
	NoHaveAnyDataNodeToWrite      = errors.New("No have any data node for create volume")
	NoHaveAnyMetaNodeToWrite      = errors.New("No have any meta node for create meta range")
	CannotOffLineErr              = errors.New("cannot offline because avail vol replicate <0")
	NoAnyDataNodeForCreateVol     = errors.New("no have enough data server for create vol")
	NoAnyMetaNodeForCreateVol     = errors.New("no have enough meta server for create meta range")
)

func paraNotFound(name string) (err error) {
	return errors.New(fmt.Sprintf("parameter %v not found", name))
}

func elementNotFound(name string) (err error) {
	return errors.New(fmt.Sprintf("%v not found", name))
}

func taskNotFound(id string) (err error) {
	return elementNotFound(fmt.Sprintf("task %v", id))
}

func metaGroupNotFound(id uint64) (err error) {
	return elementNotFound(fmt.Sprintf("meta group %v", id))
}

func metaRangeNotFound(addr string) (err error) {
	return elementNotFound(fmt.Sprintf("meta range %v", addr))
}

func hasExist(name string) (err error) {
	err = errors.New(fmt.Sprintf("%v has exist", name))
	return
}
