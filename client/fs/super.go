package fs

import (
	"fmt"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/sdk/meta"
	"github.com/tiglabs/baudstorage/sdk/data/stream"
	"github.com/tiglabs/baudstorage/util/log"
)

type Super struct {
	cluster string
	name    string
	ic      *InodeCache
	mw      *meta.MetaWrapper
	ec      *stream.ExtentClient
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

	s.ec, err = stream.NewExtentClient(namespace, master, s.mw.AppendExtentKey, s.mw.GetExtents)
	if err != nil {
		log.LogErrorf("NewExtentClient failed! %v", err.Error())
		return nil, err
	}

	s.name = namespace
	s.cluster = s.mw.Cluster()
	s.ic = NewInodeCache(DefaultInodeExpiration, MaxInodeCache)
	log.LogInfof("NewSuper: cluster(%v) name(%v)", s.cluster, s.name)
	return s, nil
}

func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(RootInode)
	if err != nil {
		return nil, err
	}
	root := NewDir(s, inode)
	return root, nil
}

func (s *Super) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	return nil
}

func (s *Super) umpKey(act string) string {
	return fmt.Sprintf("%s_fuseclient_%s", s.cluster, act)
}
