package metanode

import (
	"bytes"
	"reflect"
	"testing"
)

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
	denTmp := &Dentry{}
	if err = denTmp.Load(data); err != nil {
		t.Fatalf("dentry load: %s", err.Error())
	}
	if !reflect.DeepEqual(denTmp, dentry) {
		t.Fatalf("dentry test failed!")
	}
	expKeyStr := "                1000*test"
	if dentry.GetKey() != expKeyStr {
		t.Fatalf("dentry key test failed!")
	}
	// valid key bytes
	if true {
		haveKeys := dentry.GetKeyBytes()
		expectKeys := []byte(expKeyStr)
		if bytes.Compare(haveKeys, expectKeys) != 0 {
			t.Fatalf("dentry valid key bytes test failed!")
		}

		// valid parse key from bytes
		denTmp = &Dentry{}
		if err = denTmp.ParseKeyBytes(expectKeys); err != nil {
			t.Fatalf("dentry ParseKeyBtytes: %s", err.Error())
		}
		if denTmp.ParentId != dentry.ParentId || denTmp.Name != dentry.Name {
			t.Fatalf("dentry ParseKeyBytes: %s", err.Error())
		}
	}

	// valid values
	expctValue := "56564*0"
	if dentry.GetValue() != expctValue {
		t.Fatalf("dentry value test failed!")
	}

	if true {
		have := dentry.GetValueBytes()
		expct := []byte(expctValue)
		if bytes.Compare(have, expct) != 0 {
			t.Fatalf("dentry value bytes test failed!")
		}
		denTmp = &Dentry{}
		if err = denTmp.ParseValueBytes(expct); err != nil {
			t.Fatalf("dentry ParseValueBytes: %s", err.Error())
		}
		if denTmp.Inode != dentry.Inode || denTmp.Type != dentry.Type {
			t.Fatalf("dentry ParseValueBytes: %s", err.Error())
		}
	}

}

func Test_Inode(t *testing.T) {
	ino := NewInode(1, 0)
	// Test Dump and Load func
	data, err := ino.Dump()
	if err != nil || len(data) == 0 {
		t.Fatalf("inode Dump: %s", err.Error())
	}
	inoTmp := NewInode(0, 0)
	if err = inoTmp.Load(data); err != nil {
		t.Fatalf("inode Load: %s", err.Error())
	}
	if inoTmp.Inode != ino.Inode || inoTmp.Type != ino.Type {
		t.Fatalf("inode Load vlid: %s", err.Error())
	}

	// Test GetKey

}
