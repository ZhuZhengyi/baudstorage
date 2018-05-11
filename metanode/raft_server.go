package metanode

// StartRaftServer init address resolver and raft server instance.
func (m *MetaNode) startRaftServer() (err error) {
	//TODO: collect peers information from metaRanges

	if err = m.createRaftServer(); err != nil {
		return
	}
	for _, mr := range m.metaRangeManager.metaRangeMap {
		if err = m.createPartition(mr); err != nil {
			return
		}
	}
	return
}

func (m *MetaNode) createRaftServer() (err error) {
	return
}

func (m MetaNode) createPartition(mr *MetaRange) (err error) {
	return
}
