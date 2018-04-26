package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"flag"
	"fmt"
	"github.com/tiglabs/baudstorage/client/cfs"
	"github.com/tiglabs/baudstorage/util/config"
)

const (
	MAX_READ_AHEAD = 128 * 1024
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
	mnt := cfg.GetString("Mountpoint")
	namespace := cfg.GetString("Namespace")
	master := cfg.GetString("Master")
	c, err := fuse.Mount(
		mnt,
		fuse.AllowOther(),
		fuse.MaxReadahead(MAX_READ_AHEAD),
		fuse.AsyncRead(),
		fuse.FSName("ContainerFS-"+namespace),
		fuse.LocalVolume(),
		fuse.VolumeName("ContainerFS-"+namespace))

	if err != nil {
		return err
	}
	defer c.Close()

	super := fs.NewSuper(namespace, master)

	//TODO:init super

	if err = fs.Serve(c, super); err != nil {
		return err
	}

	<-c.Ready
	return c.MountError
}
