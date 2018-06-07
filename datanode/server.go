package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/master"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/pool"
	"github.com/tiglabs/baudstorage/util/ump"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrStoreTypeMismatch   = errors.New("store type error")
	ErrVolNotExist         = errors.New("dataPartition not exists")
	ErrChunkOffsetMismatch = errors.New("chunk offset not mismatch")
	ErrNoDiskForCreateVol  = errors.New("no disk for create dataPartition")
	ErrBadConfFile         = errors.New("bad config file")

	CurrMaster      string
	LocalIP         string
	MasterAddrs     []string
	MasterAddrIndex uint32
)

const (
	GetIpFromMaster = "/admin/getIp"
	DefaultRackName = "huitian_rack1"
)

const (
	UmpModuleName = "dataNode"
)

const (
	ConfigKeyPort       = "port"       // int
	ConfigKeyClusterID  = "clusterID"  // string
	ConfigKeyMasterAddr = "masterAddr" // array
	ConfigKeyRack       = "rack"       // string
	ConfigKeyDisks      = "disks"      // array
)

type DataNode struct {
	space          *SpaceManager
	port           string
	rackName       string
	clusterId      string
	localIp        string
	localServeAddr string
	tcpListener    net.Listener
	state          uint32
	wg             sync.WaitGroup
	connPool        *pool.ConnPool
}

func (s *DataNode) Start(cfg *config.Config) (err error) {
	if atomic.CompareAndSwapUint32(&s.state, Standby, Start) {
		defer func() {
			if err != nil {
				atomic.StoreUint32(&s.state, Standby)
			} else {
				atomic.StoreUint32(&s.state, Running)
			}
		}()
		if err = s.onStart(cfg); err != nil {
			return
		}
		s.wg.Add(1)
	}
	return
}

func (s *DataNode) Shutdown() {
	if atomic.CompareAndSwapUint32(&s.state, Running, Shutdown) {
		s.onShutdown()
		s.wg.Done()
		atomic.StoreUint32(&s.state, Stopped)
	}
}

func (s *DataNode) Sync() {
	if atomic.LoadUint32(&s.state) == Running {
		s.wg.Wait()
	}
}

func (s *DataNode) onStart(cfg *config.Config) (err error) {
	s.connPool=pool.NewConnPool()
	if err = s.LoadVol(cfg); err != nil {
		return
	}
	s.registerProfHandler()
	if err = s.startTcpService(); err != nil {
		return
	}
	ump.InitUmp(UmpModuleName)
	return
}

func (s *DataNode) onShutdown() {
	s.stopTcpService()
	return
}

func (s *DataNode) LoadVol(cfg *config.Config) (err error) {
	s.port = cfg.GetString(ConfigKeyPort)
	s.clusterId = cfg.GetString(ConfigKeyClusterID)
	if len(cfg.GetArray(ConfigKeyMasterAddr)) == 0 {
		return ErrBadConfFile
	}
	for _, ip := range cfg.GetArray(ConfigKeyMasterAddr) {
		MasterAddrs = append(MasterAddrs, ip.(string))
	}

	s.rackName = cfg.GetString(ConfigKeyRack)
	if err = s.registerToMaster(); err != nil {
		return
	}
	s.space = NewSpaceManager(s.rackName)

	if err != nil || len(strings.TrimSpace(s.port)) == 0 {
		err = ErrBadConfFile
		return
	}
	if s.rackName == "" {
		s.rackName = DefaultRackName
	}

	log.LogDebugf("action[DataNode.Load] load port[%v].", s.port)
	log.LogDebugf("action[DataNode.Load] load clusterId[%v].", s.clusterId)
	log.LogDebugf("action[DataNode.Load] load rackName[%v].", s.rackName)

	for _, d := range cfg.GetArray(ConfigKeyDisks) {
		log.LogDebugf("action[DataNode.Load] load disk raw config[%v].", d)
		// Format "PATH:RESET_SIZE:MAX_ERR
		arr := strings.Split(d.(string), ":")
		if len(arr) != 3 {
			return ErrBadConfFile
		}
		path := arr[0]
		restSize, err := strconv.ParseUint(arr[1], 10, 64)
		if err != nil {
			return ErrBadConfFile
		}
		maxErr, err := strconv.Atoi(arr[2])
		if err != nil {
			return ErrBadConfFile
		}
		_, err = LoadFromDisk(path, restSize, maxErr, s.space)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *DataNode) registerToMaster() (err error) {
	var data []byte
	// Get IP address and cluster ID from master.
	data, err = PostToMaster(nil, GetIpFromMaster)
	if err != nil {
		err = fmt.Errorf("cannot get ip from master[%v] err[%v]", MasterAddrs, err)
		return
	}
	cInfo := new(proto.ClusterInfo)
	json.Unmarshal(data, cInfo)
	LocalIP = string(cInfo.Ip)
	s.clusterId = cInfo.Cluster
	s.localServeAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)
	if !util.IP(LocalIP) {
		err = fmt.Errorf("unavalid ip from master[%v] err[%v]", MasterAddrs, LocalIP)
		return
	}
	// Register this data node to master.
	data, err = PostToMaster(nil, fmt.Sprintf("%s?addr=%s:%v", master.AddDataNode, LocalIP, s.port))
	if err != nil {
		err = fmt.Errorf("cannot add this data node to master[%v] err[%v]", MasterAddrs, err)
		return
	}
	return
}

func (s *DataNode) registerProfHandler() {
	http.HandleFunc("/disks", s.HandleGetDisk)
	http.HandleFunc("/partitions", s.HandleVol)
	http.HandleFunc("/stats", s.HandleStat)
}

func (s *DataNode) startTcpService() (err error) {
	log.LogInfo("Start: startTcpService")
	addr := fmt.Sprintf(":%v", s.port)
	l, err := net.Listen(NetType, addr)
	log.LogDebugf("action[DataNode.startTcpService] listen %v address[%v].", NetType, addr)
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}
	s.tcpListener = l
	go func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.LogErrorf("action[DataNode.startTcpService] failed to accept, err:%s", err.Error())
				break
			}
			log.LogDebugf("action[DataNode.startTcpService] accept connection from %s.", conn.RemoteAddr().String())
			go s.serveConn(conn)
		}
	}(l)
	return
}

func (s *DataNode) stopTcpService() (err error) {
	if s.tcpListener != nil {
		s.tcpListener.Close()
		log.LogDebugf("action[DataNode.stopTcpService] stop tcp service.")
	}
	return
}

func (s *DataNode) serveConn(conn net.Conn) {
	space := s.space
	space.stats.AddConnection()
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)

	msgH := NewMsgHandler(c)
	go s.handleRequest(msgH)
	go s.writeToCli(msgH)

	for {
		select {
		case <-msgH.exitCh:
			log.LogDebugf("action[DataNode.serveConn] event loop for %v exit.", conn.RemoteAddr())
			goto exitDeal
		default:
			if err := s.readFromCliAndDeal(msgH); err != nil {
				goto exitDeal
			}
		}
	}

exitDeal:
	space.stats.RemoveConnection()
	c.Close()

	return
}

func NewServer() *DataNode {
	return &DataNode{}
}

func (s *DataNode) AddCompactTask(t *CompactTask) (err error) {
	dp := s.space.getDataPartition(t.partitionId)
	if dp == nil {
		return nil
	}
	d, _ := s.space.getDisk(dp.path)
	if d == nil {
		return nil
	}
	err = d.addTask(t)
	if err != nil {
		err = errors.Annotatef(err, "Task[%v] ", t.toString())
	}

	return
}

func (s *DataNode) checkChunkInfo(pkg *Packet) (err error) {
	chunkInfo, _ := pkg.dataPartition.store.(*storage.TinyStore).GetWatermark(pkg.FileID)
	leaderObjId := uint64(pkg.Offset)
	localObjId := chunkInfo.Size
	if (leaderObjId - 1) != chunkInfo.Size {
		err = ErrChunkOffsetMismatch
		msg := fmt.Sprintf("Err[%v] leaderObjId[%v] localObjId[%v]", err, leaderObjId, localObjId)
		log.LogWarn(pkg.ActionMsg(ActionCheckChunkInfo, LocalProcessAddr, pkg.StartT, fmt.Errorf(msg)))
	}

	return
}

func (s *DataNode) handleChunkInfo(pkg *Packet) (err error) {
	if !pkg.IsWriteOperation() {
		return
	}

	if !pkg.isHeadNode() {
		err = s.checkChunkInfo(pkg)
	} else {
		err = s.headNodeSetChunkInfo(pkg)
	}
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] handleChunkInfo Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(ActionCheckChunkInfo, err.Error())
	}

	return
}

func (s *DataNode) headNodeSetChunkInfo(pkg *Packet) (err error) {
	var (
		chunkId int
	)
	store := pkg.dataPartition.store.(*storage.TinyStore)
	chunkId, err = store.GetChunkForWrite()
	if err != nil {
		pkg.dataPartition.partitionStatus = storage.ReadOnlyStore
		return
	}
	pkg.FileID = uint64(chunkId)
	objectId, _ := store.AllocObjectId(uint32(pkg.FileID))
	pkg.Offset = int64(objectId)

	return
}

func (s *DataNode) headNodePutChunk(pkg *Packet) {
	if pkg == nil || pkg.FileID <= 0 || pkg.isReturn {
		return
	}
	if pkg.StoreMode != proto.TinyStoreMode || !pkg.isHeadNode() || !pkg.IsWriteOperation() || !pkg.IsTransitPkg() {
		return
	}
	store := pkg.dataPartition.store.(*storage.TinyStore)
	if pkg.IsErrPack() {
		store.PutUnAvailChunk(int(pkg.FileID))
	} else {
		store.PutAvailChunk(int(pkg.FileID))
	}
	pkg.isReturn = true
}
