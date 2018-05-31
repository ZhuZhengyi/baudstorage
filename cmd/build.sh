#!/usr/bin/env bash
cd /root/work/baud/src/github.com/tiglabs/baudstorage/cmd
export GOPATH=/root/work/baud
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
CGO_CFLAGS="-I/usr/local/include" CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy " go build -v cmd.go