package meta

import (
	"syscall"

	"github.com/tiglabs/baudstorage/proto"
)

// TODO: High-level API, i.e. work with absolute path

// Low-level API, i.e. work with inode

func (mw *MetaWrapper) Create_ll(parentID uint64, name string, mode uint32) (*proto.InodeInfo, error) {
	var (
		status       int
		err          error
		inodeCreated bool
		info         *proto.InodeInfo
		inodeConn    *MetaConn
	)

	parentConn, err := mw.connect(parentID)
	if err != nil {
		return nil, syscall.EAGAIN
	}
	defer mw.putConn(parentConn, err)

	// Create Inode

	mp := mw.getPartitionByInode(mw.currStart)
	if mp == nil {
		return nil, syscall.ENOMEM
	}

	for {
		inodeConn, err = mw.getConn(mp)
		if err != nil {
			break
		}

		status, info, err = mw.icreate(inodeConn, mode)
		if err == nil && status == statusOK {
			// create inode is successful, and keep the connection
			inodeCreated = true
			break
		}
		mw.putConn(inodeConn, err)

		if err != nil || status != statusFull {
			break
		}

		mp = mw.getNextPartition(mw.currStart)
		if mp == nil {
			break
		}
		mw.currStart = mp.Start
	}

	if !inodeCreated {
		return nil, syscall.ENOMEM
	}

	status, err = mw.dcreate(parentConn, parentID, name, info.Inode, mode)
	if err != nil || status != statusOK {
		mw.idelete(inodeConn, info.Inode) //TODO: deal with error
		mw.putConn(inodeConn, err)
		if status == statusExist {
			return nil, syscall.EEXIST
		} else {
			return nil, syscall.ENOMEM
		}
	}
	mw.putConn(inodeConn, err)
	return info, nil
}

func (mw *MetaWrapper) Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error) {
	mc, err := mw.connect(parentID)
	if err != nil {
		return 0, 0, syscall.EAGAIN
	}
	defer mw.putConn(mc, err)

	status, inode, mode, err := mw.lookup(mc, parentID, name)
	if err != nil || status != statusOK {
		return 0, 0, syscall.ENOENT
	}
	return inode, mode, nil
}

func (mw *MetaWrapper) InodeGet_ll(inode uint64) (info *proto.InodeInfo, err error) {
	mc, err := mw.connect(inode)
	if err != nil {
		return nil, syscall.EAGAIN
	}
	defer mw.putConn(mc, err)

	status, info, err := mw.iget(mc, inode)
	if err != nil || status != statusOK {
		return nil, syscall.ENOENT
	}
	return info, nil
}

func (mw *MetaWrapper) Delete_ll(parentID uint64, name string) error {
	parentConn, err := mw.connect(parentID)
	if err != nil {
		return syscall.EAGAIN
	}
	defer mw.putConn(parentConn, err)

	status, inode, err := mw.ddelete(parentConn, parentID, name)
	if err != nil || status != statusOK {
		return syscall.ENOENT
	}

	//FIXME: dentry is deleted successfully but inode is not
	inodeConn, err := mw.connect(inode)
	if err != nil {
		return nil
	}
	defer mw.putConn(inodeConn, err)

	mw.idelete(inodeConn, inode)
	return nil
}

func (mw *MetaWrapper) Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string) error {
	srcParentConn, err := mw.connect(srcParentID)
	if err != nil {
		return syscall.EAGAIN
	}
	defer mw.putConn(srcParentConn, err)
	dstParentConn, err := mw.connect(dstParentID)
	if err != nil {
		return syscall.EAGAIN
	}
	defer mw.putConn(dstParentConn, err)

	// look up for the ino
	status, inode, mode, err := mw.lookup(srcParentConn, srcParentID, srcName)
	if err != nil || status != statusOK {
		return syscall.ENOENT
	}
	// create dentry in dst parent
	status, err = mw.dcreate(dstParentConn, dstParentID, dstName, inode, mode)
	if err != nil || status != statusOK {
		return syscall.EEXIST
	}
	// delete dentry from src parent
	status, _, err = mw.ddelete(srcParentConn, srcParentID, srcName)
	if err != nil || status != statusOK {
		mw.ddelete(dstParentConn, dstParentID, dstName) //TODO: deal with error
		return syscall.EAGAIN
	}
	return nil
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