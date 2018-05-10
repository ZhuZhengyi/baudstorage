package stream

import (
	"fmt"
	"math/rand"
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

func TestExtentClient_Write(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client, err := NewExtentClient("log", "127.0.0.1:7778", saveKey, updateKey)
	if err != nil {
		t.Logf(err.Error())
		t.FailNow()
	}
	if client == nil {
		t.Logf("init failed")
		t.FailNow()
	}
	var inode uint64
	inode = 1

	data := make([]byte, CFSBLOCKSIZE*2)
	for j := 0; j < CFSBLOCKSIZE*2; j++ {
		rand.Seed(time.Now().UnixNano())
		data[j] = byte(rand.Int() % 255)
	}
	writebytes := 0
	for seqNo := 0; seqNo < CFSBLOCKSIZE; seqNo++ {
		rand.Seed(time.Now().UnixNano())
		ndata := data[:rand.Int31n(CFSBLOCKSIZE)]
		writebytes += len(ndata)
		write, err := client.Write(inode, ndata)
		if err != nil {
			fmt.Printf("write inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err)
			t.FailNow()
		}
	}
	client.writers[inode].flushCurrExtentWriter()
	fmt.Println("sum write bytes:", writebytes)
	for {
		time.Sleep(time.Second)
		if sk.Size() == uint64(writebytes) {
			break
		}
	}

}
