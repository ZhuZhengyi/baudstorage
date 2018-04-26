package master

import (
	"fmt"
	"github.com/juju/errors"
)

var (
	RegionNotFound    = errors.New("region not found")
	ZoneNotFound      = errors.New("zone not found")
	NoAvailVol        = errors.New("no avail vol")
	VolNotFound       = errors.New("vol not found")
	DataNodeNotFound  = errors.New("data node not found")
	MetaNodeNotFound  = errors.New("meta node not found")
	NamespaceNotFound = errors.New("namespace not found")
	MetaGroupNotFound = errors.New("metaGroup not found")
	UnMatchPara       = errors.New("para not unmatched")
	ParaFormatInvalid = errors.New("the parameter format is not correct")

	NoZoneForCreateVol = errors.New("no zone from create vol")
	DisOrderArrayErr   = errors.New("dis order array is nil")

	GetAvailHostExcludeSpecifyErr = "GetAvailHostExcludeSpecifyErr "
	ClusterNotHaveAnyNodeToWrite  = errors.New("cluster not have any node for create volume")
)

func paraNotFound(name string) (err error) {
	return errors.New(fmt.Sprintf("parameter %v not found", name))
}

func elementNotFound(name string) (err error) {
	return errors.New(fmt.Sprintf("%v not found", name))
}

func paraFormatInvalid(name string) (err error) {
	err = errors.New(fmt.Sprintf("%v format invalid", name))
	return
}

func hasExist(name string) (err error) {
	err = errors.New(fmt.Sprintf("%v has exist", name))
	return
}
