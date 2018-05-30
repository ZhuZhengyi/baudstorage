package metanode

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/google/btree"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
)

const (
	inodeFile      = "inode"
	inodeFileTmp   = ".inode"
	dentryFile     = "dentry"
	dentryFileTmp  = ".dentry"
	metaFile       = "meta"
	metaFileTmp    = ".meta"
	applyIDFile    = "apply"
	applyIDFileTmp = ".apply"
)

// Load struct from meta
func (mp *metaPartition) loadMeta() (err error) {
	metaFile := path.Join(mp.config.RootDir, metaFile)
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
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
	// TODO: Valid PartitionConfig
	if mConf.checkMeta() != nil {
		return
	}
	mp.config.PartitionId = mConf.PartitionId
	mp.config.Start = mConf.Start
	mp.config.End = mConf.End
	mp.config.Peers = mConf.Peers
	return
}

// Load inode info from inode snapshot file
func (mp *metaPartition) loadInode() (err error) {
	filename := path.Join(mp.config.RootDir, inodeFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
	for {
		var (
			line []byte
			ino  = NewInode(0, 0)
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
	filename := path.Join(mp.config.RootDir, dentryFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
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
	filename := path.Join(mp.config.RootDir, applyIDFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
		}
		return
	}
	if len(data) == 0 {
		err = errors.New("read applyid empty error")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &mp.applyID); err != nil {
		return
	}
	return
}

// Store Meta to file
func (mp *metaPartition) storeMeta() (err error) {
	if err = mp.config.checkMeta(); err != nil {
		err = errors.Errorf("[storeMeta]->%s", err.Error())
		return
	}
	os.MkdirAll(mp.config.RootDir, 0755)
	filename := path.Join(mp.config.RootDir, metaFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()
	data, err := json.Marshal(mp.config)
	if err != nil {
		return
	}
	if _, err = fp.Write(data); err != nil {
		return
	}
	err = os.Rename(filename, path.Join(mp.config.RootDir, metaFile))
	return
}

func (mp *metaPartition) storeApplyID(appID uint64) (err error) {
	filename := path.Join(mp.config.RootDir, applyIDFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", appID)); err != nil {
		return
	}
	err = os.Rename(filename, path.Join(mp.config.RootDir, applyIDFile))
	return
}

func (mp *metaPartition) storeInode() (err error) {
	filename := path.Join(mp.config.RootDir, inodeFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		if err != nil {
			os.RemoveAll(filename)
		}
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
	err = os.Rename(filename, path.Join(mp.config.RootDir, inodeFile))
	return
}

func (mp *metaPartition) storeDentry() (err error) {
	filename := path.Join(mp.config.RootDir, dentryFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		os.Remove(filename)
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
	err = os.Rename(filename, path.Join(mp.config.RootDir, dentryFile))
	return
}
