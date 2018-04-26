package proto

type CreateNameSpaceRequest struct {
	Name string
}

type CreateNameSpaceResponse struct {
	Status int
	Result string
}

type CreateMetaRangeRequest struct {
	MetaId  string
	Start   uint64
	End     uint64
	GroupId uint64
	Members []string
}

type CreateMetaRangeResponse struct {
	Status uint8
	Result string
}
