package fs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type Dir struct {
	InodeCommon
	inode Inode
}

//functions that Dir needs to implement
var (
	_ fs.Node                = (*Dir)(nil)
	_ fs.NodeCreater         = (*Dir)(nil)
	_ fs.NodeForgetter       = (*Dir)(nil)
	_ fs.NodeMkdirer         = (*Dir)(nil)
	_ fs.NodeRemover         = (*Dir)(nil)
	_ fs.NodeFsyncer         = (*Dir)(nil)
	_ fs.NodeRequestLookuper = (*Dir)(nil)
	_ fs.HandleReadDirAller  = (*Dir)(nil)

	//TODO:NodeRenamer, NodeSymlinker
)

func NewDir(s *Super, p *Dir) *Dir {
	dir := new(Dir)
	dir.super = s
	dir.parent = p
	dir.blksize = BLKSIZE_DEFAULT
	dir.nlink = DIR_NLINK_DEFAULT
	return dir
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	fillAttr(a, d)
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeRegular)
	if err != nil {
		return nil, nil, err
	}

	child := NewFile(d.super, d)
	fillInode(&child.inode, info)
	resp.Node = fuse.NodeID(child.inode.ino)
	fillAttr(&resp.Attr, child)
	return child, child, nil
}

func (d *Dir) Forget() {
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeDir)
	if err != nil {
		return nil, err
	}

	child := NewDir(d.super, d)
	fillInode(&child.inode, info)
	return child, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	err := d.super.mw.Delete_ll(d.inode.ino, req.Name)
	if err != nil {
		return err
	}
	return nil
}

func (d *Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	ino, mode, err := d.super.mw.Lookup_ll(d.inode.ino, req.Name)
	if err != nil {
		return nil, err
	}

	var child fs.Node
	if mode == ModeRegular {
		dir := NewDir(d.super, d)
		err = d.super.InodeGet(ino, &dir.inode)
		child = dir
	} else if mode == ModeDir {
		file := NewFile(d.super, d)
		err = d.super.InodeGet(ino, &file.inode)
		child = file
	} else {
		err = fuse.ENOTSUP
	}

	if err != nil {
		return nil, err
	}
	resp.Node = fuse.NodeID(ino)
	fillAttr(&resp.Attr, child)
	return child, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dirents := make([]fuse.Dirent, 0)
	children, err := d.super.mw.ReadDir_ll(d.inode.ino)
	if err != nil {
		return dirents, err
	}

	for _, child := range children {
		dentry := fuse.Dirent{
			Inode: child.Inode,
			Type:  fuse.DirentType(child.Type),
			Name:  child.Name,
		}
		dirents = append(dirents, dentry)
	}
	return dirents, nil
}
