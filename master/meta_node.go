package master

type MetaNode struct {
	Addr       string
	ZoneName   string `json:"Zone"`
	metaRanges []*MetaRange
	isActive   bool
	sender     *AdminTaskSender
}

func NewMetaNode(addr, zoneName string) (node *MetaNode) {
	return &MetaNode{
		Addr:     addr,
		ZoneName: zoneName,
		sender:   NewAdminTaskSender(addr),
	}
}

func (metaNode *MetaNode) clean() {
	metaNode.sender.exitCh <- struct{}{}
}
