package fs

import (
	"io"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/util/log"
)

type File struct {
	super *Super
	inode *Inode
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

func NewFile(s *Super, i *Inode) *File {
	return &File{super: s, inode: i}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	ino := f.inode.ino
	log.LogDebugf("Attr: ino(%v)", ino)
	inode, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", ino, err.Error())
		return ParseError(err)
	}
	inode.fillAttr(a)
	return nil
}

func (f *File) Forget() {
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	ino := f.inode.ino
	log.LogDebugf("Open: ino(%v)", ino)
	f.super.ec.Open(ino)
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	ino := f.inode.ino
	log.LogDebugf("Close: ino(%v)", ino)
	err := f.super.ec.Close(ino)
	if err != nil {
		log.LogErrorf("Close: ino(%v) error (%v)", ino, err.Error())
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
	defer func() {
		// Invalidate inode cache
		f.super.ic.Delete(f.inode.ino)
	}()

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
