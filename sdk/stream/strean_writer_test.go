package stream

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"
)

var allKeys map[uint64]*StreamKey

func saveKey(inode uint64, k ExtentKey) (err error) {
	sk := allKeys[inode]
	sk.Put(k)
	sk.Inode = inode
	return
}

func updateKey(inode uint64) (sk StreamKey, err error) {
	sk = *(allKeys[inode])
	return
}

func openFileForWrite(inode uint64, action string) (f *os.File, err error) {
	return
}

func initClient(t *testing.T) (client *ExtentClient) {
	var err error
	client, err = NewExtentClient("log", "127.0.0.1:7778", saveKey, updateKey)
	if err != nil {
		t.Logf(err.Error())
		t.FailNow()
	}
	if client == nil {
		t.Logf("init failed")
		t.FailNow()
	}
	return
}

func initInode(inode uint64) (sk *StreamKey) {
	sk = new(StreamKey)
	sk.Inode = inode
	allKeys[inode] = sk
	return
}

func prepare(inode uint64, t *testing.T, data []byte) {
	for j := 0; j < CFSBLOCKSIZE*2; j++ {
		rand.Seed(time.Now().UnixNano())
		data[j] = byte(rand.Int() % 255)
	}
	return
}

func TestExtentClient_Write(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	allKeys = make(map[uint64]*StreamKey)
	client := initClient(t)
	var (
		inode uint64
	)
	inode = 3
	sk := initInode(inode)
	writebytes := 0
	data := make([]byte, CFSBLOCKSIZE*2)
	localFpWrite, err := os.Create(fmt.Sprintf("inode_%v_%v.txt", inode, "write"))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	prepare(inode, t, data)
	for seqNo := 0; seqNo < CFSBLOCKSIZE; seqNo++ {
		rand.Seed(time.Now().UnixNano())
		ndata := data[:rand.Int31n(CFSBLOCKSIZE)]
		write, err := client.Write(inode, ndata)
		if err != nil {
			fmt.Printf("write inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err)
			t.FailNow()
		}
		writebytes += write
		_, err = localFpWrite.Write(ndata)
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
	}
	client.Close(inode)
	fmt.Println("sum write bytes:", writebytes)
	localFpWrite.Close()
	for {
		time.Sleep(time.Second)
		if sk.Size() == uint64(writebytes) {
			break
		}
	}

}
