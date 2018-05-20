export GOROOT=/usr/local/go #gosetup
export GOPATH=/root/work/baudstorage #gosetup
go  test -c -i -o /tmp/___stream_writer_test_go github.com/tiglabs/baudstorage/sdk/stream #gosetup
/tmp/___stream_writer_test_go -test.v -test.run ^TestExtentClient_Write$ #gosetup

