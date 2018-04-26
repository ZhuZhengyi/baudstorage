package metanode

import (
	"sync"
	"strings"
	"github.com/juju/errors"
)

type MetaRangeGroup struct {
	// Key: string
	// Val: MetaGroup
	metaRangeMap sync.Map
}

func (g *MetaRangeGroup) StoreMetaRange(namespace string, mr *MetaRange) {
	if mr != nil && len(strings.TrimSpace(namespace)) > 0 {
		g.metaRangeMap.Store(namespace, mr)
	}
}

func (g *MetaRangeGroup) LoadMetaRange(namespace string) (mr *MetaRange, err error) {
	val, ok := g.metaRangeMap.Load(namespace)
	if !ok {
		err = errors.New("unknown namespace: " + namespace)
		return
	}
	mr = val.(*MetaRange)
	return
}
