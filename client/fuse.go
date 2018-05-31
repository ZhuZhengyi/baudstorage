package main

//
// Usage: ./client -c fuse.json &
//
// Default mountpoint is specified in fuse.json, which is "/mnt".
// Therefore operations to "/mnt" are routed to the baudstorage.
//

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"path"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	bdfs "github.com/tiglabs/baudstorage/client/fs"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
)

const (
	MaxReadAhead = 128 * 1024
)

const (
	LoggerDir    = "client"
	LoggerPrefix = "client"
)

var (
	configFile = flag.String("c", "", "FUSE client config file")
)

func main() {
	flag.Parse()
	cfg := config.LoadConfigFile(*configFile)

	if err := Mount(cfg); err != nil {
		fmt.Println("Mount failed: ", err)
	}
	fmt.Println("Done!")
}

func Mount(cfg *config.Config) error {
	mnt := cfg.GetString("mountpoint")
	namespace := cfg.GetString("namespace")
	master := cfg.GetString("master")
	logpath := cfg.GetString("logpath")
	profport := cfg.GetString("profport")
	c, err := fuse.Mount(
		mnt,
		fuse.AllowOther(),
		fuse.MaxReadahead(MaxReadAhead),
		fuse.AsyncRead(),
		fuse.FSName("bdfs-"+namespace),
		fuse.LocalVolume(),
		fuse.VolumeName("bdfs-"+namespace))

	if err != nil {
		return err
	}
	defer c.Close()

	_, err = log.NewLog(path.Join(logpath, LoggerDir), LoggerPrefix, log.DebugLevel)
	if err != nil {
		return err
	}

	super, err := bdfs.NewSuper(namespace, master)
	if err != nil {
		return err
	}

	go func() {
		fmt.Println(http.ListenAndServe(":"+profport, nil))
	}()

	if err = fs.Serve(c, super); err != nil {
		return err
	}

	<-c.Ready
	return c.MountError
}
