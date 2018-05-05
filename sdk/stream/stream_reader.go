package stream

import "github.com/tiglabs/baudstorage/sdk"

type StreamReader struct {
	inode  uint64
	key    ExtentKey
	start  uint64
	end    uint64
	wraper *sdk.VolGroupWraper
	vol    *sdk.VolGroup
	data   []byte
}

func NewStreamReader(inode, start, end uint64, key ExtentKey) (reader *StreamReader) {
	reader = new(StreamReader)
	reader.inode = inode
	reader.key = key
	reader.start = start

	return
}
