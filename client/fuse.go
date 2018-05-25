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
	"log"
	"os"
	"path"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	bdfs "github.com/tiglabs/baudstorage/client/fs"
	"github.com/tiglabs/baudstorage/util/config"
)

const (
	MaxReadAhead = 128 * 1024
)

const (
	LoggerName     = "fuse.log"
	LoggerPrefix   = "FUSE:"
	LoggerFileFlag = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	LoggerFlag     = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile
)

var (
	configFile = flag.String("c", "", "FUSE client config file")
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func main() {
	flag.Parse()
	cfg := config.LoadConfigFile(*configFile)

	if err := Mount(cfg); err != nil {
		fmt.Println("Mount failed: ", err)
	}
	fmt.Println("Done!")
}

func Mount(cfg *config.Config) error {
	mnt := cfg.GetString("Mountpoint")
	namespace := cfg.GetString("Namespace")
	master := cfg.GetString("Master")
	logpath := cfg.GetString("Logpath")
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

	logfile, err := os.OpenFile(path.Join(logpath, LoggerName), LoggerFileFlag, 0644)
	if err != nil {
		return err
	}
	defer logfile.Close()

	logger := log.New(logfile, LoggerPrefix, LoggerFlag)
	super, err := bdfs.NewSuper(namespace, master, logpath, logger)
	if err != nil {
		return err
	}

	if err = fs.Serve(c, super); err != nil {
		return err
	}

	<-c.Ready
	return c.MountError
}
