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
		err = errors.Annotatef(err, "icreate: mc(%v) req(%v)", mc, *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("icreate: mc(%v) req(%v) result(%v)", mc, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "icreate: mc(%v) PacketData(%v)", mc, string(packet.Data))
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
		err = errors.Annotatef(err, "idelete: mc(%v) req(%v)", mc, *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("idelete: mc(%v) req(%v) result(%v)", mc, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.DeleteInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "idelete: mc(%v) PacketData(%v)", mc, string(packet.Data))
		return
	}
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
		err = errors.Annotatef(err, "dcreate: mc(%v) req(%v)", mc, *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("dcreate: mc(%v) req(%v) result(%v)", mc, *req, packet.GetResultMesg())
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
		err = errors.Annotatef(err, "ddelete: mc(%v) req(%v)", mc, *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("ddelete: mc(%v) req(%v) result(%v)", mc, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.DeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "ddelete: mc(%v) PacketData(%v)", mc, string(packet.Data))
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

	log.LogDebugf("lookup: mc(%v) parent(%v) name(%v)", mc, parentID, name)
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
		err = errors.Annotatef(err, "lookup: mc(%v) req(%v)", mc, *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		if status != statusNoent {
			log.LogErrorf("lookup: mc(%v) req(%v) result(%v)", mc, *req, packet.GetResultMesg())
		}
		return
	}

	resp := new(proto.LookupResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "lookup: mc(%v) PacketData(%v)", mc, string(packet.Data))
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

	log.LogDebugf("iget request: mc(%v) ino(%v)", mc, inode)

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
		err = errors.Annotatef(err, "iget: mc(%v) req(%v)", mc, *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("iget: mc(%v) req(%v) result(%v)", mc, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.InodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "iget: mc(%v) PacketData(%v)", mc, string(packet.Data))
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
		err = errors.Annotatef(err, "readdir: mc(%v) req(%v)", mc, *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		children = make([]proto.Dentry, 0)
		log.LogErrorf("readdir: mc(%v) req(%v) result(%v)", mc, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.ReadDirResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "readdir: mc(%v) PacketData(%v)", mc, string(packet.Data))
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
		err = errors.Annotatef(err, "appendExtentKey: mc(%v) req(%v)", mc, *req)
		return
	}
	if packet.ResultCode != proto.OpOk {
		log.LogErrorf("appendExtentKey: mc(%v) result(%v)", mc, packet.GetResultMesg())
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
		err = errors.Annotatef(err, "getExtents: mc(%v) req(%v)", mc, *req)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		extents = make([]proto.ExtentKey, 0)
		log.LogErrorf("getExtents: mc(%v) result(%v)", mc, packet.GetResultMesg())
		return
	}

	resp := new(proto.GetExtentsResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		err = errors.Annotatef(err, "getExtents: mc(%v) PacketData(%v)", mc, string(packet.Data))
		return
	}
	return statusOK, resp.Extents, nil
}
