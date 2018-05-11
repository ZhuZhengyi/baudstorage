package proto

type CreateNameSpaceRequest struct {
	Name string
}

type CreateNameSpaceResponse struct {
	Status int
	Result string
}

type Peer struct {
	id   uint64
	addr string
}
type CreateMetaRangeRequest struct {
	MetaId  string
	NsName  string
	Start   uint64
	End     uint64
	GroupId uint64
	Members []Peer
}

type CreateMetaRangeResponse struct {
	NsName  string
	GroupId uint64
	Status  uint8
	Result  string
}
