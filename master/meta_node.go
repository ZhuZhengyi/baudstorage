package master

type MetaNode struct {
	Addr       string
	metaRanges []*MetaRange
	isActive   bool
	sender     *AdminTaskSender
}

func NewMetaNode(addr string) (node *MetaNode) {
	return &MetaNode{
		Addr:   addr,
		sender: NewAdminTaskSender(addr),
	}
}

func (metaNode *MetaNode) clean() {
	metaNode.sender.exitCh <- struct{}{}
}
