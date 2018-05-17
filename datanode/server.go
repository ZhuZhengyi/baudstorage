package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/pool"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrStoreTypeUnmatch   = errors.New("store type error")
	ErrVolNotExist        = errors.New("vol not exsits")
	ErrChunkOffsetUnmatch = errors.New("chunk offset not unmatch")
	ErrNoDiskForCreateVol = errors.New("no disk for create vol")
	ErrBadConfFile        = errors.New("bad config file")

	masterAddr string
)

const (
	GetIpFromMaster = "/getip"
)

type DataNode struct {
	ConnPool        *pool.ConnPool
	space           *SpaceManager
	masterAddrs     []string
	masterAddrIndex uint32
	stats           *Stats
	port            string
	logdir          string
	zone            string
	restport        string
	profport        string
	clusterId       string
	localIp         string
}

func (s *DataNode) checkConfig(cfg *config.Config) (err error) {
	s.port = cfg.GetString("Port")
	s.clusterId = cfg.GetString("ClusterID")
	s.logdir = cfg.GetString("LogDir")
	for _, ip := range cfg.GetArray("MasterAddr") {
		s.masterAddrs = append(s.masterAddrs, ip.(string))
	}
	s.zone = cfg.GetString("Zone")
	_, err = log.NewLog(s.logdir, "datanode", log.DebugLevel)
	if err = s.getIpFromMaster(); err != nil {
		return
	}

	s.restport = cfg.GetString("restport")
	s.profport = cfg.GetString("porfport")

	if err != nil || s.port == "" || s.clusterId == "" || s.logdir == "" ||
		masterAddr == "" || s.zone == "" {
		return ErrBadConfFile
	}

	for _, d := range cfg.GetArray("Disks") {
		arr := strings.Split(d.(string), ":")
		if len(arr) != 3 {
			return ErrBadConfFile
		}
		path := arr[0]
		restSize, err := strconv.ParseUint(arr[1], 10, 2)
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

func (s *DataNode) getIpFromMaster() error {
	data, err := s.PostToMaster(nil, GetIpFromMaster)
	if err != nil {
		panic(fmt.Sprintf("cannot get ip from master[%v] err[%v]", s.masterAddrs, err))
	}
	cInfo := new(proto.ClusterInfo)
	json.Unmarshal(data, cInfo)
	s.localIp = string(cInfo.Ip)
	s.clusterId = cInfo.Cluster
	if !util.IP(s.localIp) {
		panic(fmt.Sprintf("unavalid ip from master[%v] err[%v]", s.masterAddrs, s.localIp))
	}
	return nil
}

func (s *DataNode) Start(cfg *config.Config) error {
	s.space = NewSpaceManager()

	panic("implement me")
}

func (s *DataNode) StartRestService() {
	http.HandleFunc("/disks", QueryDisks)
	http.HandleFunc("/vols", QueryVols)
	http.HandleFunc("/stats", QueryStats)
	http.HandleFunc("/task", HandleTasks)
	http.HandleFunc("/op", HandleOps)
	go s.handleCmds()
	go s.handleCmdResps()
	go func() {
		err := http.ListenAndServe(s.ip+":"+s.restport, nil)
		if err != nil {
			println("Failed to start rest service")
			s.Shutdown()
		}
	}()
}

func (s *DataNode) listenAndServe() (err error) {
	log.LogInfo("Start: listenAndServe")
	l, err := net.Listen(NetType, ":"+s.port)
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.LogError("failed to accept, err:", err)
			break
		}
		go s.serveConn(conn)
	}

	log.LogError(LogShutdown + " return listenAndServe, listen is closing")
	return l.Close()
}

func (s *DataNode) serveConn(conn net.Conn) {
	s.stats.AddConnection()
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)

	msgH := NewMsgHandler(c)
	go s.handleReqs(msgH)
	go s.writeToCli(msgH)

	for {
		select {
		case <-msgH.exitCh:
			goto exitDeal
		default:
			if err := s.readFromCliAndDeal(msgH); err != nil {
				goto exitDeal
			}
		}
	}

exitDeal:
	s.stats.RemoveConnection()
	c.Close()

	return
}

func (s *DataNode) Shutdown() {
	panic("implement me")
}

func (s *DataNode) Sync() {
	panic("implement me")
}

func NewServer() *DataNode {
	return &DataNode{}
}

func (s *DataNode) AddCompactTask(t *CompactTask) (err error) {
	v := s.space.getVol(t.volId)
	if v == nil {
		return nil
	}
	d, _ := s.space.getDisk(v.path)
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
	chunkInfo, _ := pkg.vol.store.(*storage.TinyStore).GetWatermark(pkg.FileID)
	leaderObjId := uint64(pkg.Offset)
	localObjId := chunkInfo.Size
	if (leaderObjId - 1) != chunkInfo.Size {
		err = ErrChunkOffsetUnmatch
		mesg := fmt.Sprintf("Err[%v] leaderObjId[%v] localObjId[%v]", err, leaderObjId, localObjId)
		log.LogWarn(pkg.actionMesg(ActionCheckChunkInfo, LocalProcessAddr, pkg.StartT, fmt.Errorf(mesg)))
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
		pkg.PackErrorBody(ActionCheckChunkInfo, err.Error())
	}

	return
}

func (s *DataNode) headNodeSetChunkInfo(pkg *Packet) (err error) {
	var (
		chunkId int
	)
	store := pkg.vol.store.(*storage.TinyStore)
	chunkId, err = store.GetChunkForWrite()
	if err != nil {
		pkg.vol.status = storage.ReadOnlyStore
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
	if pkg.StoreType != proto.TinyStoreMode || !pkg.isHeadNode() || !pkg.IsWriteOperation() || !pkg.IsTransitPkg() {
		return
	}
	store := pkg.vol.store.(*storage.TinyStore)
	if pkg.IsErrPack() {
		store.PutUnAvailChunk(int(pkg.FileID))
	} else {
		store.PutAvailChunk(int(pkg.FileID))
	}
	pkg.isReturn = true
}
