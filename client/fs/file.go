package fs

import (
	"io"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/util/log"
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
	log.LogDebugf("Attr: ino(%v)", f.inode.ino)
	err := f.super.InodeGet(f.inode.ino, &f.inode)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", f.inode.ino, err.Error())
		return ParseError(err)
	}
	fillAttr(a, f)
	return nil
}

func (f *File) Forget() {
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	log.LogDebugf("Open: ino(%v)", f.inode.ino)
	f.super.ec.Open(f.inode.ino)
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	log.LogDebugf("Close: ino(%v)", f.inode.ino)
	err := f.super.ec.Close(f.inode.ino)
	if err != nil {
		log.LogErrorf("Close returns error (%v)", err.Error())
		return fuse.EIO
	}
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.LogDebugf("Read: HandleID(%v) sizeof Data(%v) offset(%v) size(%v) \n", req.Handle, len(resp.Data), req.Offset, req.Size)

	data := make([]byte, req.Size)
	size, err := f.super.ec.Read(f.inode.ino, data, int(req.Offset), req.Size)
	if err != nil {
		if err == io.EOF && size == 0 {
			return nil
		} else {
			log.LogErrorf("Read error: (%v) size(%v)", err.Error(), size)
			return fuse.EIO
		}
	}
	if size > req.Size {
		log.LogErrorf("Read error: request size(%v) read size(%v)", req.Size, size)
		return fuse.ERANGE
	}
	resp.Data = append(resp.Data, data[:size]...)
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	log.LogDebugf("Write: ino(%v) HandleID(%v) offset(%v) len(%v)\n", f.inode.ino, req.Handle, req.Offset, len(req.Data))
	size, err := f.super.ec.Write(f.inode.ino, req.Data)
	if err != nil {
		log.LogErrorf("Write returns error (%v)", err.Error())
		return fuse.EIO
	}
	resp.Size = size
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	log.LogDebugf("Flush: ino(%v) HandleID(%v)\n", f.inode.ino, req.Handle)
	err := f.super.ec.Flush(f.inode.ino)
	if err != nil {
		log.LogErrorf("Flush error (%v)", err.Error())
		return fuse.EIO
	}
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	log.LogDebugf("Fsync: ino(%v) HandleID(%v)\n", f.inode.ino, req.Handle)
	err := f.super.ec.Flush(f.inode.ino)
	if err != nil {
		log.LogErrorf("Flush error (%v)", err.Error())
		return fuse.EIO
	}
	return nil
}
