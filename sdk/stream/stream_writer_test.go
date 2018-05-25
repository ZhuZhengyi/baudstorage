package stream

import (
	"bytes"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

var aalock sync.Mutex
var allKeys map[uint64]*proto.StreamKey

func saveExtentKey(inode uint64, k proto.ExtentKey) (err error) {
	aalock.Lock()
	defer aalock.Unlock()
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

func updateKey(inode uint64) (extents []proto.ExtentKey, err error) {
	aalock.Lock()
	defer aalock.Unlock()
	extents = allKeys[inode].Extents
	return
}

func openFileForWrite(inode uint64, action string) (f *os.File, err error) {
	return os.Create(fmt.Sprintf("inode_%v_%v.txt", inode, action))
}

func initClient(t *testing.T) (client *ExtentClient) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	var err error
	client, err = NewExtentClient("log", "stream_write_test", "127.0.0.1:7778", saveExtentKey, updateKey)
	if err != nil {
		OccoursErr(fmt.Errorf("init client err[%v]", err.Error()), t)
	}
	if client == nil {
		OccoursErr(fmt.Errorf("init client err[%v]", err.Error()), t)
	}
	return
}

func initInode(inode uint64) (sk *proto.StreamKey) {
	sk = new(proto.StreamKey)
	sk.Inode = inode
	allKeys[inode] = sk
	return
}

func prepare(inode uint64, t *testing.T) (localWriteFp *os.File, localReadFp *os.File) {
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
	allKeys = make(map[uint64]*proto.StreamKey)
	client := initClient(t)
	var (
		inode uint64
		read  int
	)
	inode = 2
	sk := initInode(inode)
	writebytes := 0
	writeStr := randSeq(CFSBLOCKSIZE*5 + 1)
	data := ([]byte)(writeStr)
	localWriteFp, localReadFp := prepare(inode, t)

	client.Open(inode)
	client.Open(inode)
	client.Open(inode)
	for seqNo := 0; seqNo < CFSBLOCKSIZE; seqNo++ {
		rand.Seed(time.Now().UnixNano())
		ndata := data[:rand.Intn(CFSBLOCKSIZE*5)]

		//write
		write, err := client.Write(inode, ndata)
		if err != nil || write != len(ndata) {
			OccoursErr(fmt.Errorf("write inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err), t)
		}
		fmt.Printf("hahah ,write ok [%v]\n", seqNo)

		//flush
		err = client.Flush(inode)
		if err != nil {
			OccoursErr(fmt.Errorf("flush inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err), t)
		}
		fmt.Printf("hahah ,flush ok [%v]\n", seqNo)

		//read
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
			OccoursErr(fmt.Errorf("write localFile write inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err), t)
		}
		_, err = localReadFp.Write(rdata)
		if err != nil {
			OccoursErr(fmt.Errorf("write localFile read inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err), t)
		}
		writebytes += write
	}

	//read size more than write size
	rdata := make([]byte, CFSBLOCKSIZE)
	read, err := client.Read(inode, rdata, (writebytes-CFSBLOCKSIZE+1024), CFSBLOCKSIZE)
	if err != nil || read != (CFSBLOCKSIZE-1024) {
		OccoursErr(fmt.Errorf("read inode [%v] bytes[%v] err[%v]\n", inode, read, err), t)
	}

	//finish
	client.Close(inode)
	client.Close(inode)
	client.Close(inode)

	localWriteFp.Close()
	localReadFp.Close()

	for {
		time.Sleep(time.Second)
		if sk.Size() == uint64(writebytes) {
			break
		}
	}

}
