package storage

import (
	"errors"
	"math/rand"
	"os"
	"time"
)

var SimErrors [8]error = [...]error{
	errors.New("invalid argument"),
	errors.New("permission denied"),
	errors.New("file already exists"),
	errors.New("file does not exist"),
	errors.New("file already closed"),
	errors.New("use of closed network connection"),
	errors.New("use of closed file"),
	errors.New("i/o timeout"),
}

type FileSimulator struct {
	fp       *os.File
	fullPath string
}

func (fs *FileSimulator) selectErr() error {
	rand.Seed(time.Now().Unix())
	idx := rand.Intn(len(SimErrors))
	err := SimErrors[idx]
	return err
}

func (fs *FileSimulator) Close() error {
	errRat := rand.Intn(100)
	if errRat <= DiskeErrRatio {
		return fs.selectErr()
	}
	return fs.fp.Close()
}

func (fs *FileSimulator) OpenFile(fname string, flag int, perm os.FileMode) error {
	fs.fullPath = fname
	errRat := rand.Intn(100)
	if errRat <= DiskeErrRatio {
		return fs.selectErr()
	}
	var err error
	fs.fp, err = os.OpenFile(fname, flag, perm)
	return err
}

func (fs *FileSimulator) Open(fname string) error {
	errRat := rand.Intn(100)
	if errRat <= DiskeErrRatio {
		return fs.selectErr()
	}
	var err error
	fs.fp, err = os.Open(fname)
	return err
}

func (fs *FileSimulator) Stat() (os.FileInfo, error) {
	var finfo os.FileInfo
	errRat := rand.Intn(100)
	if errRat <= DiskeErrRatio {
		return finfo, fs.selectErr()
	}
	return fs.fp.Stat()
}

func (fs *FileSimulator) WriteAt(b []byte, off int64) (int, error) {
	errRat := rand.Intn(100)
	if errRat <= DiskeErrRatio {
		return -1, fs.selectErr()
	}
	return fs.fp.WriteAt(b, off)
}

func (fs *FileSimulator) ReadAt(b []byte, off int64) (int, error) {
	errRat := rand.Intn(100)
	if errRat <= DiskeErrRatio {
		return -1, fs.selectErr()
	}
	return fs.fp.ReadAt(b, off)
}

func (fs *FileSimulator) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}

func (fs *FileSimulator) Name() string {
	return fs.fp.Name()
}

func (fs *FileSimulator) Sync() error {
	errRat := rand.Intn(100)
	if errRat <= DiskeErrRatio {
		return fs.selectErr()
	}
	return fs.fp.Sync()
}

func (fs *FileSimulator) Seek(offset int64, whence int) (ret int64, err error) {
	errRat := rand.Intn(100)
	if errRat <= DiskeErrRatio {
		return -1, fs.selectErr()
	}
	return fs.fp.Seek(offset, whence)
}

func (fs *FileSimulator) Write(b []byte) (n int, err error) {
	errRat := rand.Intn(100)
	if errRat <= DiskeErrRatio {
		return 0, fs.selectErr()
	}
	return fs.fp.Write(b)
}
