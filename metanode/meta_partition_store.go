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
func (mf *MetaPartitionFsm) LoadInode() (err error) {
	// Restore btree from ino file
	inoFile := path.Join(mf.metaPartition.RootDir, "inode")
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
		if mf.metaPartition.Cursor < ino.Inode {
			mf.metaPartition.Cursor = ino.Inode
		}
	}
	return
}

// Load dentry from dentry snapshot file
func (mf *MetaPartitionFsm) LoadDentry() (err error) {
	// Restore dentry from dentry file
	dentryFile := path.Join(mf.metaPartition.RootDir, "dentry")
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

func (mf *MetaPartitionFsm) LoadApplyID() (err error) {
	applyIDFile := path.Join(mf.metaPartition.RootDir, "applyid")
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

func (mf *MetaPartitionFsm) StoreApplyID() (err error) {
	filename := path.Join(mf.metaPartition.RootDir, "_applyid")
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

func (mf *MetaPartitionFsm) StoreInodeTree() (err error) {
	filename := path.Join(mf.metaPartition.RootDir, "_inode")
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

func (mf *MetaPartitionFsm) StoreDentryTree() (err error) {
	filename := path.Join(mf.metaPartition.RootDir, "_dentry")
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
