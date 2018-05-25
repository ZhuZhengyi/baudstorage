package metanode

import "testing"

func Test_Dentry(t *testing.T) {
	dentry := &Dentry{
		ParentId: 1000,
		Name:     "test",
		Inode:    56564,
		Type:     0,
	}
	data, err := dentry.Dump()
	if err != nil || len(data) == 0 {
		t.Fatalf("dentry dump: %s", err.Error())
	}

}
