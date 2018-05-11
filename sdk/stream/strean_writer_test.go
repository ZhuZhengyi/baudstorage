package stream

import (
	"bytes"
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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func updateKey(inode uint64) (sk *StreamKey, err error) {
	sk = (allKeys[inode])
	return
}

func openFileForWrite(inode uint64, action string) (f *os.File, err error) {
	return os.Create(fmt.Sprintf("inode_%v_%v.txt", inode, action))
}

func initClient(t *testing.T) (client *ExtentClient) {
	var err error
	client, err = NewExtentClient("log", "127.0.0.1:7778", saveKey, updateKey)
	if err != nil {
		OccoursErr(fmt.Errorf("init client err[%v]", err.Error()), t)
	}
	if client == nil {
		OccoursErr(fmt.Errorf("init client err[%v]", err.Error()), t)
	}
	return
}

func initInode(inode uint64) (sk *StreamKey) {
	sk = new(StreamKey)
	sk.Inode = inode
	allKeys[inode] = sk
	return
}

func prepare(inode uint64, t *testing.T, data []byte) (localWriteFp *os.File, localReadFp *os.File) {
	var err error
	localWriteFp, err = openFileForWrite(inode, "write")
	if err != nil {
		OccoursErr(fmt.Errorf("write localFile inode[%v] err[%v]\n", inode, err), t)
	}
	localReadFp, err = openFileForWrite(inode, "read")
	if err != nil {
		OccoursErr(fmt.Errorf("read localFile inode[%v] err[%v]\n", inode, err), t)
	}
	return
}

func OccoursErr(err error, t *testing.T) {
	fmt.Println(err.Error())
	t.FailNow()
}

func TestExtentClient_Write(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	allKeys = make(map[uint64]*StreamKey)
	client := initClient(t)
	var (
		inode uint64
		read  int
	)
	inode = 2
	sk := initInode(inode)
	writebytes := 0
	data := make([]byte, CFSBLOCKSIZE*2)
	localWriteFp, _ := prepare(inode, t, data)
	for seqNo := 0; seqNo < CFSBLOCKSIZE; seqNo++ {
		writeStr := randSeq(1024 * 1)
		ndata := ([]byte)(writeStr)
		write, err := client.Write(inode, ndata)
		if err != nil {
			OccoursErr(fmt.Errorf("write inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err), t)
		}
		client.Flush(inode)
		rdata := make([]byte, len(ndata))
		read, err = client.Read(inode, rdata, writebytes, len(ndata))
		if err != nil || read != len(ndata) {
			OccoursErr(fmt.Errorf("read inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, read, err), t)
		}
		if !bytes.Equal(rdata, ndata) {
			fmt.Printf("acatual read bytes[%v]\n", string(rdata))
			fmt.Printf("expectr read bytes[%v]\n", writeStr)
			OccoursErr(fmt.Errorf("acatual read is differ to writestr"), t)
		}
		_, err = localWriteFp.Write(ndata)
		if err != nil {
			OccoursErr(fmt.Errorf("write localFile inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err), t)
		}
		writebytes += len(ndata)
		fmt.Printf("hahah ,write ok [%v]\n", seqNo)
	}
	client.Close(inode)
	fmt.Println("sum write bytes:", writebytes)
	localWriteFp.Close()
	for {
		time.Sleep(time.Second)
		if sk.Size() == uint64(writebytes) {
			break
		}
	}

}
