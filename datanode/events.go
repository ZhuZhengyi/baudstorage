package datanode

import "github.com/tiglabs/baudstorage/storage"

func (s *DataNode) modifyVolsStatus() {
	for _, d := range s.space.disks {
		volsID := d.getVols()
		diskStatus := d.Status

		for _, vID := range volsID {
			v:=s.space.getVol(vID)
			if v==nil {
				continue
			}

			switch v.volMode {
			case ExtentVol:
				store:=v.store.(*storage.ExtentStore)
				v.status=store.GetStoreStatus()
			case TinyVol:
				store:=v.store.(*storage.TinyStore)
				v.status=store.GetStoreStatus()
				if v.isLeader{
					store.MoveChunkToUnavailChan()
				}
			}
			if v.isLeader && v.status==storage.ReadOnlyStore{
				v.status=storage.ReadOnlyStore
			}

			if diskStatus==storage.DiskErrStore{
				v.status=storage.DiskErrStore
			}
		}
	}
}