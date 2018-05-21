package metanode

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"github.com/google/btree"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"io"
	"io/ioutil"
	"os"
	"path"
)

const (
	dumpFileInode      = "inode.data"
	dumpFileInodeTmp   = ".inode.tmp"
	dumpFileDentry     = "dentry.data"
	dumpFileDentryTmp  = ".dentry.tmp"
	dumpFileMeta       = "meta.data"
	dumpFileMetaTmp    = ".meta.tmp"
	dumpFileApplyId    = "apply.data"
	dumpFileApplyIdTmp = ".apply.tmp"
)

func (mp *metaPartition) loadMeta() (err error) {
	// Load struct from meta
	metaFile := path.Join(mp.config.DataPath, dumpFileMeta)
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0655)
	if err != nil {
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		return
	}
	mConf := &MetaPartitionConfig{}
	if err = json.Unmarshal(data, mConf); err != nil {
		return
	}
	mp.config = mConf
	return
}

// Load inode info from inode snapshot file
func (mp *metaPartition) loadInode() (err error) {
	// Restore btree from ino file
	inoFile := path.Join(mp.config.DataPath, dumpFileInode)
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
		if mp.createInode(ino) != proto.OpOk {
			err = errors.New("load inode info error")
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
	}
	return
}

// Load dentry from dentry snapshot file
func (mp *metaPartition) loadDentry() (err error) {
	// Restore dentry from dentry file
	dentryFile := path.Join(mp.config.DataPath, dumpFileDentry)
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
		if mp.createDentry(dentry) != proto.OpOk {
			err = errors.New("load dentry info error")
			return
		}
	}
	return
}

func (mp *metaPartition) loadApplyID() (err error) {
	applyIDFile := path.Join(mp.config.DataPath, dumpFileInode)
	data, err := ioutil.ReadFile(applyIDFile)
	if err != nil {
		return
	}
	if len(data) == 0 {
		err = errors.New("read applyid empty error")
		return
	}
	mp.applyID = binary.BigEndian.Uint64(data)
	return
}

func (mp *metaPartition) storeMeta() (err error) {
	// Store Meta to file
	metaFile := path.Join(mp.config.DataPath, dumpFileMetaTmp)
	fp, err := os.OpenFile(metaFile, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE,
		0655)
	if err != nil {
		return
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(metaFile)
		}
	}()
	data, err := json.Marshal(mp)
	if err != nil {
		return
	}
	if _, err = fp.Write(data); err != nil {
		return
	}
	err = os.Rename(metaFile, path.Join(mp.config.DataPath, dumpFileMeta))
	return
}

func (mp *metaPartition) storeApplyID() (err error) {
	filename := path.Join(mp.config.DataPath, dumpFileApplyIdTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
	}()
	if err = binary.Write(fp, binary.BigEndian, mp.applyID); err != nil {
		return
	}
	err = os.Rename(filename, path.Join(mp.config.DataPath, dumpFileApplyId))
	return
}

func (mp *metaPartition) storeInode() (err error) {
	filename := path.Join(mp.config.DataPath, dumpFileInodeTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
	}()
	inoTree := mp.getInodeTree()
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
	err = os.Rename(filename, path.Join(mp.config.DataPath, dumpFileInodeTmp))
	return
}

func (mp *metaPartition) storeDentry() (err error) {
	filename := path.Join(mp.config.DataPath, dumpFileDentryTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
	}()
	denTree := mp.dentryTree
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
	err = os.Rename(filename, path.Join(mp.config.DataPath, dumpFileDentry))
	return
}
