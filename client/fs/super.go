package fs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type Super struct {
	Namespace  string
	MasterHost string
}

//functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

func NewSuper(namespace, master string) *Super {
	return &Super{Namespace: namespace, MasterHost: master}
}

func (s *Super) Root() (fs.Node, error) {
	root := NewDir(s, ROOT_INO, nil)
	if err := InodeGet(&root.inode); err != nil {
		return nil, err
	}
	return root, nil
}

func (s *Super) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	return nil
}
