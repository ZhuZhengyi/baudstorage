package master

type MetadataSnapshot struct {
	fsm *MetadataFsm
}

func (ms *MetadataSnapshot) Next() ([]byte, error) {
	panic("implement me")
}

