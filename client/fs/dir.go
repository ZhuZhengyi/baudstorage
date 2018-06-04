package fs

import (
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/util/log"
)

type Dir struct {
	super *Super
	inode *Inode
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

func NewDir(s *Super, i *Inode) *Dir {
	return &Dir{super: s, inode: i}
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	log.LogDebugf("Dir Attr: ino(%v)", d.inode.ino)

	inode, err := d.super.InodeGet(d.inode.ino)
	if err != nil {
		log.LogErrorf("Dir Attr: ino(%v) err(%v)", d.inode.ino, err.Error())
		return ParseError(err)
	}
	inode.fillAttr(a)
	d.inode = inode
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	log.LogDebugf("Dir Create: ino(%v) name(%v)", d.inode.ino, req.Name)

	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeRegular)
	if err != nil {
		log.LogErrorf("Dir Create: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err.Error())
		return nil, nil, ParseError(err)
	}

	inode := NewInode(info)
	d.super.ic.Put(inode)
	inode.fillAttr(&resp.Attr)
	resp.Node = fuse.NodeID(inode.ino)
	d.super.ec.Open(inode.ino)

	child := NewFile(d.super, inode)
	return child, child, nil
}

func (d *Dir) Forget() {
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	log.LogDebugf("Dir Mkdir: ino(%v) name(%v)", d.inode.ino, req.Name)

	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeDir)
	if err != nil {
		log.LogErrorf("Dir Mkdir: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err.Error())
		return nil, ParseError(err)
	}

	inode := NewInode(info)
	d.super.ic.Put(inode)
	child := NewDir(d.super, inode)
	return child, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	log.LogDebugf("Dir Remove: ino(%v) name(%v)", d.inode.ino, req.Name)

	extents, err := d.super.mw.Delete_ll(d.inode.ino, req.Name)
	if err != nil {
		log.LogErrorf("Dir Remove: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err.Error())
		return ParseError(err)
	}

	if extents != nil {
		log.LogDebugf("Remove extents: %v", extents)
		d.super.ec.Delete(extents)
	}
	return nil
}

func (d *Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	log.LogDebugf("Dir Lookup: parent(%v) name(%v)", d.inode.ino, req.Name)

	start := time.Now()
	ino, mode, err := d.super.mw.Lookup_ll(d.inode.ino, req.Name)
	if err != nil {
		log.LogErrorf("Dir Lookup: parent(%v) name(%v) err(%v)", d.inode.ino, req.Name, err.Error())
		return nil, ParseError(err)
	}
	elapsed := time.Since(start)
	log.LogDebugf("Lookup cost (%v)ns", elapsed.Nanoseconds())

	inode, err := d.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Dir Lookup: parent(%v) name(%v) ino(%v) err(%v)", d.inode.ino, req.Name, ino, err.Error())
		return nil, ParseError(err)
	}
	inode.fillAttr(&resp.Attr)

	var child fs.Node
	if mode == ModeDir {
		child = NewDir(d.super, inode)
	} else {
		child = NewFile(d.super, inode)
	}

	resp.Node = fuse.NodeID(ino)
	resp.EntryValid = LookupValidDuration
	return child, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.LogDebugf("Dir Readdir: ino(%v)", d.inode.ino)

	dirents := make([]fuse.Dirent, 0)
	children, err := d.super.mw.ReadDir_ll(d.inode.ino)
	if err != nil {
		log.LogErrorf("Dir Readdir: ino(%v) err(%v)", d.inode.ino, err.Error())
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

	log.LogDebugf("Dir Rename: srcIno(%v) oldName(%v) dstIno(%v) newName(%v)", d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName)

	err := d.super.mw.Rename_ll(d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName)
	if err != nil {
		log.LogErrorf("Dir Rename: srcIno(%v) oldName(%v) dstIno(%v) newName(%v) err(%v)", d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName, err.Error())
		return ParseError(err)
	}
	return nil
}
