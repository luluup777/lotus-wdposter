package stores

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

const MetaFile = "sectorstore.json"

type StoragePath struct {
	ID     ID
	Weight uint64

	LocalPath string

	CanSeal  bool
	CanStore bool
}

// LocalStorageMeta [path]/sectorstore.json
type LocalStorageMeta struct {
	ID ID

	// A high weight means data is more likely to be stored in this path
	Weight uint64 // 0 = readonly

	// Intermediate data for the sealing process will be stored here
	CanSeal bool

	// Finalized sectors that will be proved over time will be stored here
	CanStore bool

	// MaxStorage specifies the maximum number of bytes to use for sector storage
	// (0 = unlimited)
	MaxStorage uint64
}

type Local struct {
	index SectorIndex

	paths map[ID]*path

	localLk sync.RWMutex
}

type path struct {
	local string // absolute local path
}

func (p *path) sectorPath(sid abi.SectorID, fileType storiface.SectorFileType) string {
	return filepath.Join(p.local, fileType.String(), storiface.SectorName(sid))
}

func NewLocal(ctx context.Context, index SectorIndex, storagePaths []string) (*Local, error) {
	l := &Local{
		index: index,
		paths: map[ID]*path{},
	}

	return l, l.open(ctx, storagePaths)
}

func (st *Local) OpenPath(ctx context.Context, p string) error {
	st.localLk.Lock()
	defer st.localLk.Unlock()

	mb, err := ioutil.ReadFile(filepath.Join(p, MetaFile))
	if err != nil {
		return xerrors.Errorf("reading storage metadata for %s: %w", p, err)
	}

	var meta LocalStorageMeta
	if err := json.Unmarshal(mb, &meta); err != nil {
		return xerrors.Errorf("unmarshalling storage metadata for %s: %w", p, err)
	}

	out := &path{
		local: p,
	}

	fst, err := fsutil.Statfs(p)
	if err != nil {
		return err
	}

	err = st.index.StorageAttach(ctx, StorageInfo{
		ID:         meta.ID,
		Weight:     meta.Weight,
		MaxStorage: meta.MaxStorage,
		CanSeal:    meta.CanSeal,
		CanStore:   meta.CanStore,
	}, fst)
	if err != nil {
		return xerrors.Errorf("declaring storage in index: %w", err)
	}

	st.paths[meta.ID] = out

	return nil
}

func (st *Local) open(ctx context.Context, storagePaths []string) error {
	for _, p := range storagePaths {
		err := st.OpenPath(ctx, p)
		if err != nil {
			return xerrors.Errorf("opening path %s: %w", p, err)
		}
	}

	return nil
}

func (st *Local) findSectorFromStorages(ctx context.Context, sid storage.SectorRef, fileType storiface.SectorFileType) []SectorStorageInfo {
	si := st.index.StorageFindSector(ctx, sid.ID, fileType)
	if len(si) != 0 {
		return si
	}

	for id, p := range st.paths {
		storageInfo, err := st.index.StorageInfo(ctx, id)
		if err != nil {
			continue
		}

		sectorPath := p.sectorPath(sid.ID, fileType)
		if storiface.FTCache == fileType {
			sectorPath = filepath.Join(sectorPath)
		}

		_, err = os.Stat(sectorPath)
		if err != nil {
			continue
		}
		if err = st.index.StorageDeclareSector(ctx, id, sid.ID, fileType, storageInfo.CanStore); err != nil {
			log.Warnf("StorageDeclareSector err: %s", err.Error())
		}
	}

	return st.index.StorageFindSector(ctx, sid.ID, fileType)
}

func (st *Local) AcquireSector(ctx context.Context, sid storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType) (storiface.SectorPaths, storiface.SectorPaths, error) {
	if existing|allocate != existing^allocate {
		return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.New("can't both find and allocate a sector")
	}

	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out storiface.SectorPaths
	var storageIDs storiface.SectorPaths

	for _, fileType := range storiface.PathTypes {
		if fileType&existing == 0 {
			continue
		}

		si := st.findSectorFromStorages(ctx, sid, fileType)
		for _, info := range si {
			p, ok := st.paths[info.ID]
			if !ok {
				continue
			}

			if p.local == "" { // TODO: can that even be the case?
				continue
			}

			spath := p.sectorPath(sid.ID, fileType)
			storiface.SetPathByType(&out, fileType, spath)
			storiface.SetPathByType(&storageIDs, fileType, string(info.ID))

			existing ^= fileType
			break
		}
	}

	return out, storageIDs, nil
}

func (st *Local) Local(ctx context.Context) ([]StoragePath, error) {
	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out []StoragePath
	for id, p := range st.paths {
		if p.local == "" {
			continue
		}

		si, err := st.index.StorageInfo(ctx, id)
		if err != nil {
			return nil, xerrors.Errorf("get storage info for %s: %w", id, err)
		}

		out = append(out, StoragePath{
			ID:        id,
			Weight:    si.Weight,
			LocalPath: p.local,
			CanSeal:   si.CanSeal,
			CanStore:  si.CanStore,
		})
	}

	return out, nil
}

var _ Store = &Local{}
