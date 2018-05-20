export GOPATH=/home/guowl/baudstorage
/usr/local/go/bin/go test -c -i -o /tmp/__reader_test github.com/tiglabs/baudstorage/sdk/stream #gosetup
/tmp/__reader_test  -test.v -test.run ^TestStreamReader_GetReader$ #gosetup
