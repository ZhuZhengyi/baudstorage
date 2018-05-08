package fs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/sdk"
)

type Super struct {
	name string
	meta *sdk.MetaWrapper
}

//functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

func NewSuper(namespace, master string) (s *Super, err error) {
	s = new(Super)
	s.meta, err = sdk.NewMetaWrapper(namespace, master)
	if err != nil {
		return nil, err
	}
	s.name = namespace
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
