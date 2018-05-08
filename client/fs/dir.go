package fs

import (
	"os"

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
	a.Nlink = d.nlink
	a.BlockSize = d.blksize
	a.Mode = os.ModeDir | os.ModePerm
	fillAttr(a, &d.inode)
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	meta := d.super.meta
	status, info, err := meta.Create_ll(d.inode.ino, req.Name, ModeRegular)
	err = ParseResult(status, err)
	if err != nil {
		return nil, nil, err
	}

	child := NewFile(d.super, d)
	fillInode(&child.inode, info)
	return child, child, nil
}

func (d *Dir) Forget() {
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	return nil, fuse.EPERM
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	return fuse.EPERM
}

func (d *Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	return nil, fuse.ENOENT
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return []fuse.Dirent{}, nil
}
