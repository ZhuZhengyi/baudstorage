package fs

import (
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type File struct {
	inode Inode
}

//functions that File needs to implement
var (
	_ fs.Node           = (*File)(nil)
	_ fs.Handle         = (*File)(nil)
	_ fs.NodeForgetter  = (*File)(nil)
	_ fs.NodeOpener     = (*File)(nil)
	_ fs.HandleReleaser = (*File)(nil)
	_ fs.HandleReader   = (*File)(nil)
	_ fs.HandleWriter   = (*File)(nil)
	_ fs.HandleFlusher  = (*File)(nil)
	_ fs.NodeFsyncer    = (*File)(nil)

	//TODO:HandleReadAller, NodeSetattrer
)

func NewFile(s *Super, ino uint64, p *Dir) *File {
	file := new(File)
	inode := &file.inode
	InodeInit(inode, s, ino, p)
	inode.nlink = REGULAR_NLINK_DEFAULT
	return file
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = f.inode.ino
	a.Mode = os.ModePerm
	a.Size = f.inode.size
	a.Nlink = f.inode.nlink
	return nil
}

func (f *File) Forget() {
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return nil, fuse.EPERM
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}
