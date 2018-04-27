package cmd

import (
	datanode "github.com/tiglabs/baudstorage/datanode"
	master "github.com/tiglabs/baudstorage/master"
	metanode "github.com/tiglabs/baudstorage/metanode"

	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/tiglabs/baudstorage/util/config"
)

const (
	Version = "0.1"
)

var (
	configFile = flag.String("c", "", "config file path")
	logLevel   = flag.Int("log", 0, "log level, as DebugLevel = 0")
)

type Server interface {
	Start(cfg *config.Config) error
	Shutdown()
	// Sync will block invoker goroutine until this MetaNode shutdown.
	Sync()
}

func interceptSignal(s Server) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		s.Shutdown()
	}()
}

func main() {
	log.Println("Hello, Containerfs")
	flag.Parse()
	cfg := config.LoadConfigFile(*configFile)
	role := cfg.GetString("role")
	profPort := cfg.GetString("prof")

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	//init profile server
	go func() {
		log.Println(http.ListenAndServe(":"+profPort, nil))
	}()

	var server Server

	switch role {
	case "metanode":
		server = metanode.NewServer()
	case "master":
		server = master.NewServer()
	case "datanode":
		server = datanode.NewServer()
	default:
		log.Println("Fatal: unmath role: ", role)
		os.Exit(1)
		return
	}
	interceptSignal(server)
	err := server.Start(cfg)
	if err != nil {
		log.Fatal("Fatal: failed to start the jfs daemon - ", err)
		os.Exit(1)
		return
	}
	// Block main goroutine until server shutdown.
	server.Sync()
	os.Exit(0)
}
