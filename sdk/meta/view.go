package meta

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/juju/errors"

	"github.com/tiglabs/baudstorage/util/log"
)

var (
	NotLeader = errors.New("NotLeader")
)

type NamespaceView struct {
	Name           string
	MetaPartitions []*MetaPartition
}

type ClusterInfo struct {
	Cluster string
}

// Namespace view managements
//

func (mw *MetaWrapper) PostGetRequest(addr string) ([]byte, error) {
	resp, err := http.Get(addr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusForbidden {
		msg := fmt.Sprintf("Post to (%v) failed, StatusCode(%v)", addr, resp.StatusCode)
		return nil, errors.New(msg)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		msg := fmt.Sprintf("Post to (%v) read body failed, err(%v)", addr, err.Error())
		return nil, errors.New(msg)
	}

	if resp.StatusCode == http.StatusForbidden {
		// Update MetaWrapper's leader addr
		mw.leader = strings.TrimSuffix(string(data), "\n")
		return data, NotLeader
	}
	return data, nil
}

func (mw *MetaWrapper) PullNamespaceView() (*NamespaceView, error) {
	body, err := mw.PostGetRequest("http://" + mw.leader + MetaPartitionViewURL + mw.namespace)
	if err != nil {
		if err == NotLeader {
			// MetaWrapper's leader addr is already updated
			body, err = mw.PostGetRequest("http://" + mw.leader + MetaPartitionViewURL + mw.namespace)
		} else {
			for _, addr := range mw.master {
				body, err = mw.PostGetRequest("http://" + addr + MetaPartitionViewURL + mw.namespace)
				if err == nil {
					mw.leader = addr
					break
				}
			}
		}
	}

	if err != nil {
		return nil, err
	}

	view := new(NamespaceView)
	if err = json.Unmarshal(body, view); err != nil {
		return nil, err
	}
	return view, nil
}

func (mw *MetaWrapper) UpdateClusterInfo() error {
	body, err := mw.PostGetRequest("http://" + mw.leader + GetClusterInfoURL)
	if err != nil {
		log.LogErrorf("ClusterInfo error: %v", err)
		if err == NotLeader {
			// MetaWrapper's leader addr is already updated
			body, err = mw.PostGetRequest("http://" + mw.leader + GetClusterInfoURL)
		} else {
			for _, addr := range mw.master {
				body, err = mw.PostGetRequest("http://" + addr + GetClusterInfoURL)
				if err == nil {
					mw.leader = addr
					break
				}
			}
		}
	}

	if err != nil {
		log.LogErrorf("ClusterInfo error: %v", err)
		return err
	}

	info := new(ClusterInfo)
	if err = json.Unmarshal(body, info); err != nil {
		log.LogErrorf("ClusterInfo error: %v", err)
		return err
	}
	log.LogInfof("ClusterInfo: %v", *info)
	mw.cluster = info.Cluster
	return nil
}

func (mw *MetaWrapper) UpdateMetaPartitions() error {
	nv, err := mw.PullNamespaceView()
	if err != nil {
		return err
	}

	for _, mp := range nv.MetaPartitions {
		mw.replaceOrInsertPartition(mp)
	}
	return nil
}

func (mw *MetaWrapper) refresh() {
	t := time.NewTicker(RefreshMetaPartitionsInterval)
	for {
		select {
		case <-t.C:
			if err := mw.UpdateMetaPartitions(); err != nil {
				//TODO: log error
			}
		}
	}
}
