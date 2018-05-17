package meta

import (
	"fmt"
	"math/rand"
	"syscall"
	"testing"

	"github.com/google/uuid"

	"github.com/tiglabs/baudstorage/proto"
)

const (
	SimNamespace  = "simserver"
	SimMasterAddr = "localhost"
	SimMasterPort = "8900"
)

const (
	TestFileCount = 110
)

var gMetaWrapper *MetaWrapper

func init() {
	mw, err := NewMetaWrapper(SimNamespace, SimMasterAddr+":"+SimMasterPort)
	if err != nil {
		fmt.Println(err)
	}
	gMetaWrapper = mw
}

func TestGetNamespace(t *testing.T) {
	for _, mp := range gMetaWrapper.partitions {
		t.Logf("%v", *mp)
	}
}

func TestCreate(t *testing.T) {
	uuid := uuid.New()
	parent, err := gMetaWrapper.Create_ll(proto.ROOT_INO, uuid.String(), proto.ModeDir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < TestFileCount; i++ {
		name := fmt.Sprintf("abc%v", i)
		info, err := gMetaWrapper.Create_ll(parent.Inode, name, proto.ModeRegular)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("inode: %v", *info)
	}
}

func TestInodeGet(t *testing.T) {
	doInodeGet(t, proto.ROOT_INO)
	doInodeGet(t, 2)
	doInodeGet(t, 100)
	doInodeGet(t, 101)
}

func doInodeGet(t *testing.T, ino uint64) {
	info, err := gMetaWrapper.InodeGet_ll(ino)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Getting inode (%v), %v", ino, *info)
}

func TestLookup(t *testing.T) {
	name := fmt.Sprintf("abc%v", rand.Intn(TestFileCount))
	ino, mode, err := gMetaWrapper.Lookup_ll(2, name)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Lookup name(%v) : ino(%v) mode(%v)", name, ino, mode)

	info, err := gMetaWrapper.InodeGet_ll(ino)
	if err != nil {
		t.Fatal(err)
	}

	if mode != info.Mode {
		t.Fatalf("dentry mode(%v), inode mode(%v)", mode, info.Mode)
	}
}

func TestDelete(t *testing.T) {
	name := fmt.Sprintf("abc%v", rand.Intn(TestFileCount))
	err := gMetaWrapper.Delete_ll(2, name)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Delete name(%v)", name)

	_, _, err = gMetaWrapper.Lookup_ll(2, name)
	if err != syscall.ENOENT {
		t.Fatal(err)
	}
}

func TestRename(t *testing.T) {
	uuid := uuid.New()
	parent, err := gMetaWrapper.Create_ll(proto.ROOT_INO, uuid.String(), proto.ModeDir)
	if err != nil {
		t.Fatal(err)
	}

	err = gMetaWrapper.Rename_ll(2, "abc50", parent.Inode, "abc10")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Rename [%v %v] --> [%v %v]", 2, "abc50", parent.Inode, "abc10")

	_, _, err = gMetaWrapper.Lookup_ll(2, "abc50")
	if err != syscall.ENOENT {
		t.Fatal(err)
	}

	_, _, err = gMetaWrapper.Lookup_ll(parent.Inode, "abc10")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadDir(t *testing.T) {
	doReadDir(t, proto.ROOT_INO)
	doReadDir(t, 2)
}

func doReadDir(t *testing.T, ino uint64) {
	t.Logf("ReadDir ino(%v)", ino)
	children, err := gMetaWrapper.ReadDir_ll(ino)
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		t.Log(child)
	}
}
