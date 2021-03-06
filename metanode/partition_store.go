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
		err = errors.Errorf("[loadMeta]: OpenFile %s", err.Error())
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		err = errors.Errorf("[loadMeta]: ReadFile %s, data: %s", err.Error(),
			string(data))
		return
	}
	mConf := &MetaPartitionConfig{}
	if err = json.Unmarshal(data, mConf); err != nil {
		err = errors.Errorf("[loadMeta]: Unmarshal MetaPartitionConfig %s",
			err.Error())
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
		err = errors.Errorf("[loadInode] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
	var line []byte
	for {
		var (
			buf      []byte
			isPrefix bool
		)
		buf, isPrefix, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.Errorf("[loadInode] ReadLine: %s", err.Error())
			return
		}
		line = append(line, buf...)
		if isPrefix {
			continue
		}

		ino := NewInode(0, 0)
		if err = json.Unmarshal(line, ino); err != nil {
			err = errors.Errorf("[loadInode] Unmarshal: %s, data: %s",
				err.Error(), string(line))
			return
		}
		if mp.createInode(ino) != proto.OpOk {
			err = errors.Errorf("[loadInode]->%s", err.Error())
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		line = nil
	}
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
			return
		}
		err = errors.Errorf("[loadDentry] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
	var line []byte
	for {
		var (
			buf      []byte
			isPrefix bool
		)
		buf, isPrefix, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.Errorf("[loadDentry] ReadLine: %s", err.Error())
			return
		}
		line = append(line, buf...)
		if isPrefix {
			continue
		}
		dentry := &Dentry{}
		if err = json.Unmarshal(line, dentry); err != nil {
			err = errors.Errorf("[loadDentry] Unmarshal: %s", err.Error())
			return
		}
		if mp.createDentry(dentry) != proto.OpOk {
			err = errors.Errorf("[loadDentry]->%s", err.Error())
			return
		}
		line = nil
	}
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
			return
		}
		err = errors.Errorf("[loadApplyID] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.Errorf("[loadApplyID]: ApplyID is empty")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &mp.applyID); err != nil {
		err = errors.Errorf("[loadApplyID] ReadApplyID: %s", err.Error())
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
