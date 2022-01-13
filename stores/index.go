package stores

import (
	"context"
	logging "github.com/ipfs/go-log/v2"
	"sync"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

var log = logging.Logger("index")

// ID identifies sector storage by UUID. One sector storage should map to one
//  filesystem, local or networked / shared by multiple machines
type ID string

type StorageInfo struct {
	ID         ID
	Weight     uint64
	MaxStorage uint64

	CanSeal  bool
	CanStore bool
}

type SectorStorageInfo struct {
	ID     ID
	Weight uint64

	CanSeal  bool
	CanStore bool

	Primary bool
}

type Decl struct {
	abi.SectorID
	storiface.SectorFileType
}

type declMeta struct {
	storage ID
	primary bool
}

type storageEntry struct {
	info *StorageInfo
	fsi  fsutil.FsStat
}

type Index struct {
	*indexLocks
	lk sync.RWMutex

	sectors map[Decl][]*declMeta
	stores  map[ID]*storageEntry
}

func NewIndex() *Index {
	return &Index{
		indexLocks: &indexLocks{
			locks: map[abi.SectorID]*sectorLock{},
		},
		sectors: map[Decl][]*declMeta{},
		stores:  map[ID]*storageEntry{},
	}
}

func (i *Index) StorageList(_ context.Context) (map[ID][]Decl, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	byID := map[ID]map[abi.SectorID]storiface.SectorFileType{}

	for id := range i.stores {
		byID[id] = map[abi.SectorID]storiface.SectorFileType{}
	}
	for decl, ids := range i.sectors {
		for _, id := range ids {
			byID[id.storage][decl.SectorID] |= decl.SectorFileType
		}
	}

	out := map[ID][]Decl{}
	for id, m := range byID {
		out[id] = []Decl{}
		for sectorID, fileType := range m {
			out[id] = append(out[id], Decl{
				SectorID:       sectorID,
				SectorFileType: fileType,
			})
		}
	}

	return out, nil
}

func (i *Index) StorageAttach(_ context.Context, si StorageInfo, st fsutil.FsStat) error {
	i.lk.Lock()
	defer i.lk.Unlock()

	log.Infof("New sector storage: %s", si.ID)

	if _, ok := i.stores[si.ID]; ok {
		i.stores[si.ID].info.Weight = si.Weight
		i.stores[si.ID].info.MaxStorage = si.MaxStorage
		i.stores[si.ID].info.CanSeal = si.CanSeal
		i.stores[si.ID].info.CanStore = si.CanStore

		return nil
	}

	i.stores[si.ID] = &storageEntry{
		info: &si,
		fsi:  st,
	}
	return nil
}

func (i *Index) StorageDeclareSector(_ context.Context, storageID ID, s abi.SectorID, ft storiface.SectorFileType, primary bool) error {
	i.lk.Lock()
	defer i.lk.Unlock()

loop:
	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := Decl{s, fileType}

		for _, sid := range i.sectors[d] {
			if sid.storage == storageID {
				if !sid.primary && primary {
					sid.primary = true
				} else {
					log.Warnf("sector %v redeclared in %s", s, storageID)
				}
				continue loop
			}
		}

		i.sectors[d] = append(i.sectors[d], &declMeta{
			storage: storageID,
			primary: primary,
		})
	}

	return nil
}

func (i *Index) StorageDropSector(_ context.Context, storageID ID, s abi.SectorID, ft storiface.SectorFileType) error {
	i.lk.Lock()
	defer i.lk.Unlock()

	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := Decl{s, fileType}

		if len(i.sectors[d]) == 0 {
			continue
		}

		rewritten := make([]*declMeta, 0, len(i.sectors[d])-1)
		for _, sid := range i.sectors[d] {
			if sid.storage == storageID {
				continue
			}

			rewritten = append(rewritten, sid)
		}
		if len(rewritten) == 0 {
			delete(i.sectors, d)
			continue
		}

		i.sectors[d] = rewritten
	}

	return nil
}

func (i *Index) StorageFindSector(_ context.Context, s abi.SectorID, ft storiface.SectorFileType) []SectorStorageInfo {
	i.lk.RLock()
	defer i.lk.RUnlock()

	storageIDs := map[ID]uint64{}
	isprimary := map[ID]bool{}

	for _, pathType := range storiface.PathTypes {
		if ft&pathType == 0 {
			continue
		}

		for _, id := range i.sectors[Decl{s, pathType}] {
			storageIDs[id.storage]++
			isprimary[id.storage] = isprimary[id.storage] || id.primary
		}
	}

	out := make([]SectorStorageInfo, 0, len(storageIDs))

	for id, n := range storageIDs {
		st, ok := i.stores[id]
		if !ok {
			log.Warnf("storage %s is not present in sector index (referenced by sector %v)", id, s)
			continue
		}

		out = append(out, SectorStorageInfo{
			ID:     id,
			Weight: st.info.Weight * n, // storage with more sector types is better

			CanSeal:  st.info.CanSeal,
			CanStore: st.info.CanStore,

			Primary: isprimary[id],
		})
	}

	return out
}

func (i *Index) StorageInfo(_ context.Context, id ID) (StorageInfo, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	si, found := i.stores[id]
	if !found {
		return StorageInfo{}, xerrors.Errorf("sector store not found")
	}

	return *si.info, nil
}

var _ SectorIndex = &Index{}
