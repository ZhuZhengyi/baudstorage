package meta

import (
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
)

// API implementations
//

func (mw *MetaWrapper) icreate(mc *MetaConn, mode uint32) (status int, info *proto.InodeInfo, err error) {
	req := &proto.CreateInodeRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Mode:        mode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateInode
	err = packet.MarshalData(req)
	if err != nil {
		log.LogError(err)
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		log.LogError(err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogError(err)
		log.LogErrorf("data = [%v]\n", string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) idelete(mc *MetaConn, inode uint64) (status int, err error) {
	req := &proto.DeleteInodeRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteInode
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}
	return parseStatus(packet.ResultCode), nil
}

func (mw *MetaWrapper) dcreate(mc *MetaConn, parentID uint64, name string, inode uint64, mode uint32) (status int, err error) {
	req := &proto.CreateDentryRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		ParentID:    parentID,
		Inode:       inode,
		Name:        name,
		Mode:        mode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateDentry
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}
	return parseStatus(packet.ResultCode), nil
}

func (mw *MetaWrapper) ddelete(mc *MetaConn, parentID uint64, name string) (status int, inode uint64, err error) {
	req := &proto.DeleteDentryRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		ParentID:    parentID,
		Name:        name,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteDentry
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		return
	}

	resp := new(proto.DeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		return
	}
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) lookup(mc *MetaConn, parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
	req := &proto.LookupRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		ParentID:    parentID,
		Name:        name,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaLookup
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("lookup: ResultCode(%v)", packet.ResultCode)
		return
	}

	resp := new(proto.LookupResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		return
	}
	return statusOK, resp.Inode, resp.Mode, nil
}

func (mw *MetaWrapper) iget(mc *MetaConn, inode uint64) (status int, info *proto.InodeInfo, err error) {
	req := &proto.InodeGetRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Inode:       inode,
	}

	log.LogDebugf("iget request: Namespace(%v) PartitionID(%v) Inode(%v)", mw.namespace, mc.id, inode)

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaInodeGet
	err = packet.MarshalData(req)
	if err != nil {
		log.LogError(err)
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		log.LogError(err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("iget: status not OK (%v)", packet.ResultCode)
		return
	}

	resp := new(proto.InodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogError(err)
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) readdir(mc *MetaConn, parentID uint64) (status int, children []proto.Dentry, err error) {
	req := &proto.ReadDirRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		ParentID:    parentID,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaReadDir
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		children = make([]proto.Dentry, 0)
		return
	}

	resp := new(proto.ReadDirResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		return
	}
	return statusOK, resp.Children, nil
}

func (mw *MetaWrapper) appendExtentKey(mc *MetaConn, inode uint64, extent proto.ExtentKey) (status int, err error) {
	req := &proto.AppendExtentKeyRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Inode:       inode,
		Extent:      extent,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaExtentsAdd
	err = packet.MarshalData(req)
	if err != nil {
		log.LogError(err)
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		log.LogError(err)
		return
	}
	if packet.ResultCode != proto.OpOk {
		log.LogErrorf("ResultCode(%v)\n", packet.ResultCode)
	}
	return parseStatus(packet.ResultCode), nil
}

func (mw *MetaWrapper) getExtents(mc *MetaConn, inode uint64) (status int, extents []proto.ExtentKey, err error) {
	req := &proto.GetExtentsRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaExtentsList
	err = packet.MarshalData(req)
	if err != nil {
		log.LogError(err)
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		log.LogError(err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		extents = make([]proto.ExtentKey, 0)
		return
	}

	resp := new(proto.GetExtentsResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		return
	}
	return statusOK, resp.Extents, nil
}
