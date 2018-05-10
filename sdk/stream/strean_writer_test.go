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
	f, err = os.OpenFile(fmt.Sprintf("inode_%v_action.txt", inode), os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0755)
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

func prepare(inode uint64, t *testing.T) (localFp *os.File, data []byte) {
	var err error
	localFp, err = openFileForWrite(inode, "write")
	if err != nil {
		fmt.Printf("write localFile inode[%v] err[%v]\n", inode, err)
		t.FailNow()
	}
	data = make([]byte, CFSBLOCKSIZE*2)
	for j := 0; j < CFSBLOCKSIZE*2; j++ {
		rand.Seed(time.Now().UnixNano())
		data[j] = byte(rand.Int() % 255)
	}
	return
}

func TestExtentClient_Write(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client := initClient(t)
	var inode uint64
	inode = 2
	sk := initInode(inode)
	writebytes := 0
	localFpWrite, data := prepare(inode, t)
	for seqNo := 0; seqNo < CFSBLOCKSIZE; seqNo++ {
		rand.Seed(time.Now().UnixNano())
		ndata := data[:rand.Int31n(CFSBLOCKSIZE)]
		writebytes += len(ndata)
		write, err := client.Write(inode, ndata)
		if err != nil {
			fmt.Printf("write inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err)
			t.FailNow()
		}
		localFpWrite.Write(data)
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
