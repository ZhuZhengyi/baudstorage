package metanode

import (
	"github.com/tiglabs/baudstorage/proto"
)

type (
	//client -> metaNode create inode request struct
	createInoReq = proto.CreateInodeRequest
	//metaNode -> client create inode response struct
	createInoResp = proto.CreateInodeResponse
	//client -> metaNode delete inode request struct
	deleteInoReq = proto.DeleteInodeRequest
	//metaNode -> client delete inode response struc
	deleteInoResp = proto.DeleteInodeResponse
	//client -> metaNode create dentry request struct
	createDentryReq = proto.CreateDentryRequest
	//metaNode -> client create dentry response struct
	createDentryResp = proto.CreateDentryResponse
	//client -> metaNode delete dentry request struct
	deleteDentryReq = proto.DeleteDentryRequest
	//metaNode -> client delete dentry response struct
	deleteDentryResp = proto.DeleteDentryResponse
	//client -> metaNode update inode name request struct
	updateInoNameReq = proto.UpdateInodeNameRequest
	//metaNode -> client update inode name response struct
	updateInoNameResp = proto.UpdateInodeNameResponse
	//client -> metaNode read dir request struct
	readdirReq = proto.ReadDirRequest
	//metaNode -> client read dir response struct
	readdirResp = proto.ReadDirResponse
	//client -> metaNode open file request struct
	openReq = proto.OpenRequest
	//metaNode -> client open file response struct
	openResp = proto.OpenResponse
)
