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

	NoZoneForCreateVol           = errors.New("no zone from create vol")
	DisOrderArrayErr             = errors.New("dis order array is nil")
	VolReplicationExcessError    = errors.New("vol Replication Excess error")
	VolReplicationLackError      = errors.New("vol Replication Lack error")
	VolPersistedNotAnyReplicates = errors.New("volume persisted not have any replicates")
	NoHaveAnyDataNodeToWrite     = errors.New("No have any data node for create volume")
)

func paraNotFound(name string) (err error) {
	return errors.New(fmt.Sprintf("parameter %v not found", name))
}

func elementNotFound(name string) (err error) {
	return errors.New(fmt.Sprintf("%v not found", name))
}

func hasExist(name string) (err error) {
	err = errors.New(fmt.Sprintf("%v has exist", name))
	return
}
