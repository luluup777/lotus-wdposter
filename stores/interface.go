package stores

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type SectorIndex interface {
	StorageAttach(context.Context, StorageInfo, fsutil.FsStat) error
	StorageInfo(context.Context, ID) (StorageInfo, error)

	StorageDeclareSector(ctx context.Context, storageID ID, s abi.SectorID, ft storiface.SectorFileType, primary bool) error
	StorageDropSector(ctx context.Context, storageID ID, s abi.SectorID, ft storiface.SectorFileType) error
	StorageFindSector(ctx context.Context, sector abi.SectorID, ft storiface.SectorFileType) []SectorStorageInfo

	StorageLock(ctx context.Context, sector abi.SectorID, read storiface.SectorFileType, write storiface.SectorFileType) error
	StorageTryLock(ctx context.Context, sector abi.SectorID, read storiface.SectorFileType, write storiface.SectorFileType) (bool, error)

	StorageList(ctx context.Context) (map[ID][]Decl, error)
}

type Store interface {
	AcquireSector(ctx context.Context, s storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType) (paths storiface.SectorPaths, stores storiface.SectorPaths, err error)
	Local(ctx context.Context) ([]StoragePath, error)
}
