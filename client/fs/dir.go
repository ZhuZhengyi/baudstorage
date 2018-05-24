package fs

import (
	"syscall"

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
	_ fs.NodeRenamer         = (*Dir)(nil)

	//TODO: NodeSymlinker
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
	d.super.logger.Printf("Dir Attr: ino(%v)", d.inode.ino)
	err := d.super.InodeGet(d.inode.ino, &d.inode)
	if err != nil {
		return ParseError(err)
	}
	fillAttr(a, d)
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	d.super.logger.Printf("Dir Create: ino(%v) name(%v)", d.inode.ino, req.Name)
	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeRegular)
	if err != nil {
		return nil, nil, ParseError(err)
	}

	child := NewFile(d.super, d)
	fillInode(&child.inode, info)
	resp.Node = fuse.NodeID(child.inode.ino)
	fillAttr(&resp.Attr, child)
	d.super.ec.Open(child.inode.ino)
	return child, child, nil
}

func (d *Dir) Forget() {
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	d.super.logger.Printf("Dir Mkdir: ino(%v) name(%v)", d.inode.ino, req.Name)
	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeDir)
	if err != nil {
		return nil, ParseError(err)
	}

	child := NewDir(d.super, d)
	fillInode(&child.inode, info)
	return child, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	d.super.logger.Printf("Dir Remove: ino(%v) name(%v)", d.inode.ino, req.Name)
	err := d.super.mw.Delete_ll(d.inode.ino, req.Name)
	if err != nil {
		return ParseError(err)
	}
	return nil
}

func (d *Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	d.super.logger.Printf("Dir Lookup: ino(%v) name(%v)", d.inode.ino, req.Name)
	ino, mode, err := d.super.mw.Lookup_ll(d.inode.ino, req.Name)
	if err != nil {
		return nil, ParseError(err)
	}

	var child fs.Node
	if mode == ModeRegular {
		dir := NewFile(d.super, d)
		err = d.super.InodeGet(ino, &dir.inode)
		child = dir
	} else if mode == ModeDir {
		file := NewDir(d.super, d)
		err = d.super.InodeGet(ino, &file.inode)
		child = file
	} else {
		err = syscall.ENOTSUP
	}

	if err != nil {
		return nil, ParseError(err)
	}
	resp.Node = fuse.NodeID(ino)
	fillAttr(&resp.Attr, child)
	resp.EntryValid = LookupValidDuration
	return child, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.super.logger.Printf("Dir Readdir: ino(%v)\n", d.inode.ino)
	dirents := make([]fuse.Dirent, 0)
	children, err := d.super.mw.ReadDir_ll(d.inode.ino)
	if err != nil {
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

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	dstDir, ok := newDir.(*Dir)
	if !ok {
		return fuse.ENOTSUP
	}

	err := d.super.mw.Rename_ll(d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName)
	if err != nil {
		return ParseError(err)
	}
	return nil
}
