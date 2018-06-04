package meta

import (
	"github.com/juju/errors"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/ump"
)

// API implementations
//

func (mw *MetaWrapper) icreate(mc *MetaConn, mode uint32) (status int, info *proto.InodeInfo, err error) {
	defer func() {
		if err != nil {
			log.LogError(errors.ErrorStack(err))
		}
	}()

	req := &proto.CreateInodeRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Mode:        mode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateInode
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mc.send(packet)
	if err != nil {
		err = errors.Annotatef(err, "icreate: req(%v)", *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("icreate: req(%v) result(%v)", *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "icreate: PacketData(%v)", string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) idelete(mc *MetaConn, inode uint64) (status int, extents []proto.ExtentKey, err error) {
	defer func() {
		if err != nil {
			log.LogError(errors.ErrorStack(err))
		}
	}()

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

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mc.send(packet)
	if err != nil {
		err = errors.Annotatef(err, "idelete: req(%v)", *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("idelete: req(%v) result(%v)", *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.DeleteInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "idelete: PacketData(%v)", string(packet.Data))
		return
	}
	log.LogDebugf("idelete: response(%v)", *resp)
	return statusOK, resp.Extents, nil
}

func (mw *MetaWrapper) dcreate(mc *MetaConn, parentID uint64, name string, inode uint64, mode uint32) (status int, err error) {
	defer func() {
		if err != nil {
			log.LogError(errors.ErrorStack(err))
		}
	}()

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

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mc.send(packet)
	if err != nil {
		err = errors.Annotatef(err, "dcreate: req(%v)", *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("dcreate: req(%v) result(%v)", *req, packet.GetResultMesg())
	}
	return
}

func (mw *MetaWrapper) ddelete(mc *MetaConn, parentID uint64, name string) (status int, inode uint64, err error) {
	defer func() {
		if err != nil {
			log.LogError(errors.ErrorStack(err))
		}
	}()

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

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mc.send(packet)
	if err != nil {
		err = errors.Annotatef(err, "ddelete: req(%v)", *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("ddelete: req(%v) result(%v)", *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.DeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "ddelete: PacketData(%v)", string(packet.Data))
		return
	}
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) lookup(mc *MetaConn, parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
	defer func() {
		if err != nil {
			log.LogError(errors.ErrorStack(err))
		}
	}()

	log.LogDebugf("lookup: partitionID(%v) parent(%v) name(%v)", mc.id, parentID, name)
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

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mc.send(packet)
	if err != nil {
		err = errors.Annotatef(err, "lookup: req(%v)", *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		if status != statusNoent {
			log.LogErrorf("lookup: req(%v) result(%v)", *req, packet.GetResultMesg())
		}
		return
	}

	resp := new(proto.LookupResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "lookup: PacketData(%v)", string(packet.Data))
		return
	}
	return statusOK, resp.Inode, resp.Mode, nil
}

func (mw *MetaWrapper) iget(mc *MetaConn, inode uint64) (status int, info *proto.InodeInfo, err error) {
	defer func() {
		if err != nil {
			log.LogError(errors.ErrorStack(err))
		}
	}()

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
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mc.send(packet)
	if err != nil {
		err = errors.Annotatef(err, "iget: req(%v)", *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("iget: req(%v) result(%v)", *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.InodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "iget: PacketData(%v)", string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) readdir(mc *MetaConn, parentID uint64) (status int, children []proto.Dentry, err error) {
	defer func() {
		if err != nil {
			log.LogError(errors.ErrorStack(err))
		}
	}()

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

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mc.send(packet)
	if err != nil {
		err = errors.Annotatef(err, "readdir: req(%v)", *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		children = make([]proto.Dentry, 0)
		log.LogErrorf("readdir: req(%v) result(%v)", *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.ReadDirResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "readdir: PacketData(%v)", string(packet.Data))
		return
	}
	return statusOK, resp.Children, nil
}

func (mw *MetaWrapper) appendExtentKey(mc *MetaConn, inode uint64, extent proto.ExtentKey) (status int, err error) {
	defer func() {
		if err != nil {
			log.LogError(errors.ErrorStack(err))
		}
	}()

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
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mc.send(packet)
	if err != nil {
		err = errors.Annotatef(err, "appendExtentKey: req(%v)", *req)
		return
	}
	if packet.ResultCode != proto.OpOk {
		log.LogErrorf("appendExtentKey: result(%v)", packet.GetResultMesg())
	}
	return parseStatus(packet.ResultCode), nil
}

func (mw *MetaWrapper) getExtents(mc *MetaConn, inode uint64) (status int, extents []proto.ExtentKey, err error) {
	defer func() {
		if err != nil {
			log.LogError(errors.ErrorStack(err))
		}
	}()

	req := &proto.GetExtentsRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaExtentsList
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mc.send(packet)
	if err != nil {
		err = errors.Annotatef(err, "getExtents: req(%v)", *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		extents = make([]proto.ExtentKey, 0)
		log.LogErrorf("getExtents: result(%v)", packet.GetResultMesg())
		return
	}

	resp := new(proto.GetExtentsResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "getExtents: PacketData(%v)", string(packet.Data))
		return
	}
	return statusOK, resp.Extents, nil
}
