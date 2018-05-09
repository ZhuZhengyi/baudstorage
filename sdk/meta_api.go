package sdk

import (
	"syscall"
	"time"

	"github.com/tiglabs/baudstorage/proto"
)

// TODO: High-level API, i.e. work with absolute path

// Low-level API, i.e. work with inode

func (mw *MetaWrapper) Create_ll(parentID uint64, name string, mode uint32) (status int, info *proto.InodeInfo, err error) {
	parentConn, err := mw.connect(parentID)
	if err != nil {
		return
	}
	defer mw.putConn(parentConn, err)

	var inodeConn *MetaConn
	var inodeCreated bool
	// Create Inode
	for {
		// Reset timer for each select
		t := time.NewTicker(CreateInodeTimeout)
		select {
		case <-t.C:
			break
		case groupid := <-mw.allocMeta:
			mp := mw.getMetaPartitionByID(groupid)
			if mp == nil {
				continue
			}
			// establish the connection
			inodeConn, err = mw.getConn(mp)
			if err != nil {
				continue
			}
			status, info, err = mw.icreate(inodeConn, mode)
			if err == nil && status == int(proto.OpOk) {
				// create inode is successful, and keep the connection
				mw.allocMeta <- groupid
				inodeCreated = true
				break
			}
			// break the connection
			mw.putConn(inodeConn, err)
		}
	}

	if !inodeCreated {
		return -1, nil, syscall.ENOMEM
	}

	status, err = mw.dcreate(parentConn, parentID, name, info.Inode, mode)
	if err != nil || status != int(proto.OpOk) {
		mw.idelete(inodeConn, info.Inode) //TODO: deal with error
	}
	mw.putConn(inodeConn, err)
	return
}

func (mw *MetaWrapper) Lookup_ll(parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
	mc, err := mw.connect(parentID)
	if err != nil {
		return
	}
	defer mw.putConn(mc, err)

	status, inode, mode, err = mw.lookup(mc, parentID, name)
	return
}

func (mw *MetaWrapper) InodeGet_ll(inode uint64) (status int, info *proto.InodeInfo, err error) {
	mc, err := mw.connect(inode)
	if err != nil {
		return
	}
	defer mw.putConn(mc, err)

	status, info, err = mw.iget(mc, inode)
	return
}

func (mw *MetaWrapper) Delete_ll(parentID uint64, name string) (status int, err error) {
	parentConn, err := mw.connect(parentID)
	if err != nil {
		return
	}
	defer mw.putConn(parentConn, err)

	status, inode, err := mw.ddelete(parentConn, parentID, name)
	if err != nil || status != int(proto.OpOk) {
		return
	}

	inodeConn, err := mw.connect(inode)
	if err != nil {
		return
	}
	defer mw.putConn(inodeConn, err)

	mw.idelete(inodeConn, inode) //TODO: deal with error
	return
}

func (mw *MetaWrapper) Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string) (status int, err error) {
	srcParentConn, err := mw.connect(srcParentID)
	if err != nil {
		return
	}
	defer mw.putConn(srcParentConn, err)
	dstParentConn, err := mw.connect(dstParentID)
	if err != nil {
		return
	}
	defer mw.putConn(dstParentConn, err)

	// look up for the ino
	status, inode, mode, err := mw.lookup(srcParentConn, srcParentID, srcName)
	if err != nil || status != int(proto.OpOk) {
		return
	}
	// create dentry in dst parent
	status, err = mw.dcreate(dstParentConn, dstParentID, dstName, inode, mode)
	if err != nil || status != int(proto.OpOk) {
		return
	}
	// delete dentry from src parent
	status, _, err = mw.ddelete(srcParentConn, srcParentID, srcName)
	if err != nil || status != int(proto.OpOk) {
		mw.ddelete(dstParentConn, dstParentID, dstName) //TODO: deal with error
	}
	return
}

func (mw *MetaWrapper) ReadDir_ll(parentID uint64) (children []proto.Dentry, err error) {
	mc, err := mw.connect(parentID)
	if err != nil {
		return
	}
	defer mw.putConn(mc, err)

	children, err = mw.readdir(mc, parentID)
	return
}
