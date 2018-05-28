package fs

import (
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/util/log"
)

type Dir interface {
	fs.Node
	fs.NodeCreater
	fs.NodeForgetter
	fs.NodeMkdirer
	fs.NodeRemover
	fs.NodeFsyncer
	fs.NodeRequestLookuper
	fs.HandleReadDirAller
	fs.NodeRenamer
}

type directory struct {
	Inode
	super  Super
	parent Dir
}

func NewDir(s Super, p Dir) Dir {
	dir := new(directory)
	dir.super = s
	dir.parent = p
	//dir.blksize = BLKSIZE_DEFAULT
	//dir.nlink = DIR_NLINK_DEFAULT
	return dir
}

func (d *directory) Attr(ctx context.Context, a *fuse.Attr) error {
	ino := d.inode.Ino()
	log.LogDebugf("Dir Attr: ino(%v)", ino)
	err := d.inode.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Dir Attr: ino(%v) err(%v)", ino, err.Error())
		return ParseError(err)
	}
	d.inode.FillAttr(a)
	return nil
}

func (d *directory) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	ino := d.inode.Ino()
	log.LogDebugf("Dir Create: ino(%v) name(%v)", ino, req.Name)
	info, err := d.super.MeatWrapper().Create_ll(ino, req.Name, ModeRegular)
	if err != nil {
		log.LogErrorf("Dir Create: ino(%v) name(%v) err(%v)", ino, req.Name, err.Error())
		return nil, nil, ParseError(err)
	}

	child := NewFile(d.super, d)
	child.inode.FillInode(info)
	child.inode.FillAttr(&resp.Attr)

	ino = child.inode.Ino()
	resp.Node = fuse.NodeID(ino)
	// When created, child is considered to be opened. And Open method will
	// not be invoked any more.
	d.super.ExtentClient().Open(ino)
	return child, child, nil
}

func (d *directory) Forget() {
}

func (d *directory) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	ino := d.inode.Ino()
	log.LogDebugf("Dir Mkdir: ino(%v) name(%v)", ino, req.Name)
	info, err := d.super.MetaWrapper().Create_ll(ino, req.Name, ModeDir)
	if err != nil {
		log.LogErrorf("Dir Mkdir: ino(%v) name(%v) err(%v)", ino, req.Name, err.Error())
		return nil, ParseError(err)
	}

	child := NewDir(d.super, d)
	child.inode.FillInode(info)
	return child, nil
}

func (d *directory) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	ino := d.inode.Ino()
	log.LogDebugf("Dir Remove: ino(%v) name(%v)", ino, req.Name)
	err := d.super.MetaWrapper().Delete_ll(ino, req.Name)
	if err != nil {
		log.LogErrorf("Dir Remove: ino(%v) name(%v) err(%v)", ino, req.Name, err.Error())
		return ParseError(err)
	}
	return nil
}

func (d *directory) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (d *directory) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	var (
		child fs.Node
		inode Inode
	)

	log.LogDebugf("Dir Lookup: ino(%v) name(%v)", d.inode.Ino(), req.Name)
	ino, mode, err := d.super.MetaWrapper().Lookup_ll(d.inode.Ino(), req.Name)
	if err != nil {
		log.LogErrorf("Dir Lookup: ino(%v) name(%v) err(%v)", d.inode.Ino(), req.Name, err.Error())
		return nil, ParseError(err)
	}

	if mode == ModeRegular {
		dir := NewFile(d.super, d)
		err = dir.inode.InodeGet(ino)
		child = dir
		inode = dir.inode
	} else if mode == ModeDir {
		file := NewDir(d.super, d)
		file.inode = NewInode()
		err = d.inode.InodeGet(ino)
		child = file
		inode = file.inode
	} else {
		err = syscall.ENOTSUP
	}

	if err != nil {
		log.LogErrorf("Dir Lookup InodeGet error: ino(%v) name(%v) child ino(%v) err(%v)", d.inode.Ino(), req.Name, ino, err.Error())
		return nil, ParseError(err)
	}
	resp.Node = fuse.NodeID(ino)
	inode.FillAttr(&resp.Attr)
	resp.EntryValid = LookupValidDuration
	return child, nil
}

func (d *directory) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	ino := d.inode.Ino()
	log.LogDebugf("Dir Readdir: ino(%v)", ino)
	dirents := make([]fuse.Dirent, 0)
	children, err := d.super.MetaWrapper().ReadDir_ll(ino)
	if err != nil {
		log.LogErrorf("Dir Readdir: ino(%v) err(%v)", ino, err.Error())
		return dirents, ParseError(err)
	}

	for _, child := range children {
		dentry := fuse.Dirent{
			Inode: child.Inode,
			Type:  ParseMode(child.Type),
			Name:  child.Name,
		}
		dirents = append(dirents, dentry)
	}
	return dirents, nil
}

func (d *directory) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	dstDir, ok := newDir.(*directory)
	if !ok {
		return fuse.ENOTSUP
	}

	log.LogDebugf("Dir Rename: srcIno(%v) oldName(%v) dstIno(%v) newName(%v)", d.inode.Ino(), req.OldName, dstDir.inode.Ino(), req.NewName)
	err := d.super.mw.Rename_ll(d.inode.Ino(), req.OldName, dstDir.inode.Ino(), req.NewName)
	if err != nil {
		log.LogErrorf("Dir Rename: srcIno(%v) oldName(%v) dstIno(%v) newName(%v) err(%v)", d.inode.Ino(), req.OldName, dstDir.inode.Ino(), req.NewName, err.Error())
		return ParseError(err)
	}
	return nil
}
