package fs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/sdk/meta"
	"github.com/tiglabs/baudstorage/sdk/stream"
	"github.com/tiglabs/baudstorage/util/log"
)

type Super struct {
	name string
	mw   *meta.MetaWrapper
	ec   *stream.ExtentClient
}

//functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

func NewSuper(namespace, master string) (s *Super, err error) {
	s = new(Super)
	s.mw, err = meta.NewMetaWrapper(namespace, master)
	if err != nil {
		log.LogErrorf("NewMetaWrapper failed! %v", err.Error())
		return nil, err
	}
	s.name = namespace

	//FIXME:
	s.ec, err = stream.NewExtentClient(namespace, master, s.mw.AppendExtentKey, s.mw.GetExtents)
	//s.ec, err = stream.NewExtentClient(namespace, "localhost:7778", s.mw.AppendExtentKey, s.mw.GetExtents)
	if err != nil {
		log.LogErrorf("NewExtentClient failed! %v", err.Error())
		return nil, err
	}
	return s, nil
}

func (s *Super) Root() (fs.Node, error) {
	root := NewDir(s, nil)
	if err := s.InodeGet(ROOT_INO, &root.inode); err != nil {
		return nil, err
	}
	root.parent = root
	return root, nil
}

func (s *Super) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	return nil
}
