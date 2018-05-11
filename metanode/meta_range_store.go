package metanode

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
)

// Load inode info from inode snapshot file
func (mf *MetaRangeFsm) LoadInode() (err error) {
	// Restore btree from ino file
	inoFile := path.Join(mf.metaRange.RootDir, "inode")
	fp, err := os.OpenFile(inoFile, os.O_RDONLY, 0644)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
		}
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
	for {
		var (
			line []byte
			ino  = &Inode{}
		)
		line, _, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}

		if err = json.Unmarshal(line, ino); err != nil {
			return
		}
		if mf.CreateInode(ino) != proto.OpOk {
			err = errors.New("load inode info error!")
			return
		}
		if mf.metaRange.Cursor < ino.Inode {
			mf.metaRange.Cursor = ino.Inode
		}
	}
	return
}

// Load dentry from dentry snapshot file
func (mf *MetaRangeFsm) LoadDentry() (err error) {
	// Restore dentry from dentry file
	dentryFile := path.Join(mf.metaRange.RootDir, "dentry")
	fp, err := os.OpenFile(dentryFile, os.O_RDONLY, 0644)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
		}
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
	for {
		var (
			line   []byte
			dentry = &Dentry{}
		)
		line, _, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		if err = json.Unmarshal(line, dentry); err != nil {
			return
		}
		if mf.CreateDentry(dentry) != proto.OpOk {
			err = errors.New("load dentry info error!")
			return
		}
	}
	return
}

func (mf *MetaRangeFsm) LoadApplyID() (err error) {
	applyIDFile := path.Join(mf.metaRange.RootDir, "applyid")
	data, err := ioutil.ReadFile(applyIDFile)
	if err != nil {
		return
	}
	if len(data) == 0 {
		err = errors.New("read applyid empty error")
		return
	}
	mf.applyID = binary.BigEndian.Uint64(data)
	return
}

func (mf *MetaRangeFsm) StoreApplyID() (err error) {
	filename := path.Join(mf.metaRange.RootDir, "_applyid")
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
	}()
	err = binary.Write(fp, binary.BigEndian, mf.applyID)
	return
}

func (mf *MetaRangeFsm) StoreInodeTree() (err error) {
	filename := path.Join(mf.metaRange.RootDir, "_inode")
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
	}()
	inoTree := mf.GetInodeTree()
	inoTree.Ascend(func(i btree.Item) bool {
		var data []byte
		if data, err = json.Marshal(i); err != nil {
			return false
		}
		if _, err = fp.Write(data); err != nil {
			return false
		}
		data[0] = byte('\n')
		if _, err = fp.Write(data[:1]); err != nil {
			return false
		}
		return true
	})
	return
}

func (mf *MetaRangeFsm) StoreDentryTree() (err error) {
	filename := path.Join(mf.metaRange.RootDir, "_dentry")
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
	}()
	denTree := mf.GetDentryTree()
	denTree.Ascend(func(i btree.Item) bool {
		var data []byte
		data, err = json.Marshal(i)
		if err != nil {
			return false
		}
		if _, err = fp.Write(data); err != nil {
			return false
		}
		data[0] = byte('\n')
		if _, err = fp.Write(data[:1]); err != nil {
			return false
		}
		return true
	})
	return
}
