package main

import (
	"github.com/tiglabs/baudstorage/datanode"
	"github.com/tiglabs/baudstorage/master"
	"github.com/tiglabs/baudstorage/metanode"
	"github.com/tiglabs/baudstorage/util/log"
	"strings"

	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/tiglabs/baudstorage/util/config"
	"net/http"
	"fmt"
)

const (
	Version = "0.1"
)

const (
	ConfigKeyRole     = "role"
	ConfigKeyLogDir   = "logDir"
	ConfigKeyLogLevel = "logLevel"
	ConfigKeyProfPort = "prof"
)

const (
	RoleMaster = "master"
	RoleMeta   = "metanode"
	RoleData   = "datanode"
)

const (
	ModuleMaster = "master"
	ModuleMeta   = "metaNode"
	ModuleData   = "dataNode"
)

var (
	configFile = flag.String("c", "", "config file path")
)

type Server interface {
	Start(cfg *config.Config) error
	Shutdown()
	// Sync will block invoker goroutine until this MetaNode shutdown.
	Sync()
}

func interceptSignal(s Server) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	log.Println("action[interceptSignal] register system signal.")
	go func() {
		sig := <-sigC
		log.Printf("action[interceptSignal] received signal: %s.", sig.String())
		s.Shutdown()
	}()
}

func main() {
	log.Println("Hello, Baud Storage")
	flag.Parse()
	cfg := config.LoadConfigFile(*configFile)
	role := cfg.GetString(ConfigKeyRole)
	logDir := cfg.GetString(ConfigKeyLogDir)
	logLevel := cfg.GetString(ConfigKeyLogLevel)
	profPort := int(cfg.GetFloat(ConfigKeyProfPort))

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Init server instance with specified role configuration.
	var (
		server Server
		module string
	)
	switch role {
	case RoleMeta:
		server = metanode.NewServer()
		module = ModuleMeta
	case RoleMaster:
		server = master.NewServer()
		module = ModuleMaster
	case RoleData:
		server = datanode.NewServer()
		module = ModuleData
	default:
		log.Println("Fatal: role mismatch: ", role)
		os.Exit(1)
		return
	}

	// Init logging
	var (
		level log.Level
	)
	switch strings.ToLower(logLevel) {
	case "debug":
		level = log.DebugLevel
	case "info":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	default:
		level = log.ErrorLevel
	}

	if profPort > 0 {
		go func() {
			log.Println(http.ListenAndServe(fmt.Sprintf(":%d",profPort), nil))
		}()
	}

	if _, err := log.NewLog(logDir, module, level); err != nil {
		os.Exit(1)
		return
	}

	interceptSignal(server)
	err := server.Start(cfg)
	if err != nil {
		log.Fatal("Fatal: failed to start the baud storage daemon - ", err)
		os.Exit(1)
		return
	}
	// Block main goroutine until server shutdown.
	server.Sync()
	os.Exit(0)
}
