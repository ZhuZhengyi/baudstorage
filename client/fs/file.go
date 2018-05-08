package fs

import (
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type File struct {
	InodeCommon
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

func NewFile(s *Super, p *Dir) *File {
	file := new(File)
	file.super = s
	file.parent = p
	file.blksize = BLKSIZE_DEFAULT
	file.nlink = REGULAR_NLINK_DEFAULT
	return file
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Nlink = f.nlink
	a.BlockSize = f.blksize
	a.Blocks = a.Size >> 9 // In 512 bytes
	a.Mode = os.ModePerm
	fillAttr(a, &f.inode)
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
