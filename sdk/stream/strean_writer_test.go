package stream

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func TestExtentClient_Write(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client, err := NewExtentClient("log", "127.0.0.1:7778")
	if err != nil {
		t.Logf(err.Error())
		t.FailNow()
	}
	keysChan := make(chan ExtentKey, 100)
	if client == nil {
		t.Logf("init failed")
		t.FailNow()
	}
	var inode uint64
	inode = 1
	client.InitWrite(inode, &keysChan)
	var sk *StreamKey
	sk = new(StreamKey)
	sk.Inode = inode

	go func() {
		for {
			select {
			case k := <-keysChan:
				sk.Put(k)
				fmt.Println(fmt.Sprintf("k %v return keysize:%v", k.Marshal(), sk.Size()))
			}
		}
	}()
	data := make([]byte, CFSBLOCKSIZE*2)
	for j := 0; j < CFSBLOCKSIZE*2; j++ {
		rand.Seed(time.Now().UnixNano())
		data[j] = byte(rand.Int() % 255)
	}
	writebytes := 0
	for seqNo := 0; seqNo < CFSBLOCKSIZE; seqNo++ {
		rand.Seed(time.Now().UnixNano())
		ndata := data[:rand.Int31n(4)]
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
