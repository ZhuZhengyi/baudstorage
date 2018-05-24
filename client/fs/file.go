package fs

import (
	"io"

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
	f.super.logger.Printf("Attr: ino(%v)", f.inode.ino)
	err := f.super.InodeGet(f.inode.ino, &f.inode)
	if err != nil {
		return ParseError(err)
	}
	fillAttr(a, f)
	return nil
}

func (f *File) Forget() {
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f.super.logger.Printf("Open: ino(%v)", f.inode.ino)
	f.super.ec.Open(f.inode.ino)
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.super.logger.Printf("Close: ino(%v)", f.inode.ino)
	err := f.super.ec.Close(f.inode.ino)
	if err != nil {
		f.super.logger.Printf("Close returns error (%v)", err.Error())
		return fuse.EIO
	}
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.super.logger.Printf("Read: HandleID(%v) sizeof Data(%v) offset(%v) size(%v) \n", req.Handle, len(resp.Data), req.Offset, req.Size)

	data := make([]byte, req.Size)
	size, err := f.super.ec.Read(f.inode.ino, data, int(req.Offset), req.Size)
	if err != nil {
		if err == io.EOF && size == 0 {
			return nil
		} else {
			f.super.logger.Printf("Read error: (%v) size(%v)", err.Error(), size)
			return fuse.EIO
		}
	}
	if size > req.Size {
		f.super.logger.Printf("Read error: request size(%v) read size(%v)", req.Size, size)
		return fuse.ERANGE
	}
	resp.Data = append(resp.Data, data[:size]...)
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.super.logger.Printf("Write: ino(%v) HandleID(%v) offset(%v) data(%v)\n", f.inode.ino, req.Handle, req.Offset, req.Data)
	size, err := f.super.ec.Write(f.inode.ino, req.Data)
	if err != nil {
		f.super.logger.Printf("Write returns error (%v)", err.Error())
		return fuse.EIO
	}
	resp.Size = size
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	f.super.logger.Printf("Flush: ino(%v) HandleID(%v)\n", f.inode.ino, req.Handle)
	err := f.super.ec.Flush(f.inode.ino)
	if err != nil {
		f.super.logger.Printf("Flush error (%v)", err.Error())
		return fuse.EIO
	}
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f.super.logger.Printf("Fsync: ino(%v) HandleID(%v)\n", f.inode.ino, req.Handle)
	err := f.super.ec.Flush(f.inode.ino)
	if err != nil {
		f.super.logger.Printf("Flush error (%v)", err.Error())
		return fuse.EIO
	}
	return nil
}
