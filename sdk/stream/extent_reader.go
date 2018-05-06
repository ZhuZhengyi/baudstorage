package stream

type ExtentReader struct {
	inInodeOffset int
	data          []byte
	size          int
	key           ExtentKey
}

func NewExtentReader(inInodeOffset int, key ExtentKey) (reader *ExtentReader) {
	reader = new(ExtentReader)
	reader.data = make([]byte, 0)
	reader.key = key
	reader.inInodeOffset = inInodeOffset
	reader.size = int(key.Size)
	return reader
}
