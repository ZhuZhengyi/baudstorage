package metanode

import (
	"sync"
	"strings"
	"github.com/juju/errors"
)

// MetaRangeGroup manage all MetaRange and make mapping between namespace and MetaRange.
type MetaRangeGroup struct {
	// Key: string
	// Val: MetaGroup
	metaRangeMap sync.Map
}

// StoreMeteRange try make mapping between namespace and MetaRange.
func (g *MetaRangeGroup) StoreMetaRange(namespace string, mr *MetaRange) {
	if mr != nil && len(strings.TrimSpace(namespace)) > 0 {
		if _, err := g.LoadMetaRange(namespace); err != nil {
			g.metaRangeMap.Store(namespace, mr)
		}
	}
}

// LoadMetaRange returns MetaRange with specified namespace if the mapping exist or report an error.
func (g *MetaRangeGroup) LoadMetaRange(namespace string) (mr *MetaRange, err error) {
	val, ok := g.metaRangeMap.Load(namespace)
	if !ok {
		err = errors.New("unknown namespace: " + namespace)
		return
	}
	mr = val.(*MetaRange)
	return
}
