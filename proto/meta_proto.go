package proto

type CreateNameSpaceRequest struct {
	Name string
}

type CreateNameSpaceResponse struct {
	Status int
	Result string
}

type Peer struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}
type CreateMetaPartitionRequest struct {
	MetaId      string
	NsName      string
	Start       uint64
	End         uint64
	PartitionID uint64
	Members     []Peer
}

type CreateMetaPartitionResponse struct {
	NsName      string
	PartitionID uint64
	Status      uint8
	Result      string
}
