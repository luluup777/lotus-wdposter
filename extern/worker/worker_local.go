package worker

import (
	"context"
	"fmt"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/v5/actors/runtime/proof"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/luluup777/wdkeep/stores/storiface"
	"golang.org/x/xerrors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	log = logging.Logger("worker")
)

type LocalWorker struct {
	workerName string
	session    uuid.UUID

	ret storiface.WorkerReturn

	lk           sync.Mutex
	runLimit     int
	runningTasks map[storiface.CallID]struct{}

	closing chan struct{}
}

func NewLocalWorker(workerName string, ret storiface.WorkerReturn, runLimit int) *LocalWorker {
	return &LocalWorker{
		workerName:   workerName,
		session:      uuid.New(),
		ret:          ret,
		runLimit:     runLimit,
		runningTasks: map[storiface.CallID]struct{}{},
		closing:      make(chan struct{}),
	}
}

func (l *LocalWorker) Info(context.Context) (storiface.WorkerInfo, error) {
	info := storiface.WorkerInfo{
		WorkerName: l.workerName,
		WorkerID:   l.session,
		RunLimit:   l.runLimit,
		RunNum:     0,
	}

	l.lk.Lock()
	info.RunNum = len(l.runningTasks)
	l.lk.Unlock()

	return info, nil
}

func (l *LocalWorker) Session(ctx context.Context) (uuid.UUID, error) {
	return l.session, nil
}

func (l *LocalWorker) Close() error {
	close(l.closing)
	return nil
}

func (l *LocalWorker) GenerateWindowPoSt(_ context.Context, minerID abi.ActorID, deadline, partition uint64, open int64, priSectorInfos []ffi.PrivateSectorInfo, randomness abi.PoStRandomness) (storiface.CallID, error) {
	ci := storiface.CallID{
		Miner:     minerID,
		Deadline:  deadline,
		Partition: partition,
		Open:      open,
	}

	if !l.isTaskRunning(ci) {
		return storiface.CallID{}, xerrors.Errorf("task miner:%s deadline: %d partition: %d is running", ci.Miner.String(), ci.Deadline, ci.Partition)
	}

	l.taskStart(ci)
	nowRunNum := l.getTaskRunningNum()
	if nowRunNum > l.runLimit {
		l.taskEnd(ci)
		return storiface.CallID{}, xerrors.Errorf("reached run limit, nowRunNum: %d limit: %d", nowRunNum, l.runLimit)
	}

	// start run task
	go func() {
		defer l.taskEnd(ci)

		proofs, faultySectors, err := generateWindowPoSt(minerID, priSectorInfos, randomness)
		if err != nil {
			err = xerrors.Errorf("%s [WorkerName: %s]", err.Error(), l.workerName)
		}

		ctx := &wctx{
			vals:    context.Background(),
			closing: l.closing,
		}

		if doReturn(ctx, ci, l.ret, proofs, faultySectors, toCallError(err)) {
			log.Infow("return proof", "minerId", ci.Miner.String(), "deadline", ci.Deadline, "", ci.Partition)
		}
	}()

	return ci, nil
}

// doReturn tries to send the result to keeper, returns true if successful
func doReturn(ctx context.Context, ci storiface.CallID, ret storiface.WorkerReturn, proofs []proof.PoStProof, faulty []abi.SectorNumber, err *storiface.CallError) bool {
	for _, proof := range proofs {
		for {
			err := ret.ReturnWindowPoSt(ctx, ci, proof.ProofBytes, faulty, err)
			if err == nil {
				break
			}

			log.Errorf("return error, will retry in 5s: %+v", err)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				log.Errorf("failed to return results: %s", ctx.Err())
				return false
			}
		}
	}

	return true
}

func toCallError(err error) *storiface.CallError {
	var serr *storiface.CallError
	if err != nil && !xerrors.As(err, &serr) {
		serr = storiface.Err(storiface.ErrUnknown, err)
	}

	return serr
}

func generateWindowPoSt(minerID abi.ActorID, priSectorInfos []ffi.PrivateSectorInfo, randomness abi.PoStRandomness) ([]proof.PoStProof, []abi.SectorNumber, error) {
	priSectors, skipped, err := pubSectorToPri(priSectorInfos)
	if err != nil {
		return nil, nil, xerrors.Errorf("pubSectorToPri err: %s", err.Error())
	}
	if len(priSectors.Values()) == 0 {
		return nil, nil, xerrors.Errorf("pubSectorToPri skip all sector: %d", len(skipped))
	}

	randomness[31] &= 0x3f

	proofs, faultySectors, err := ffi.GenerateWindowPoSt(minerID, priSectors, randomness)
	if err != nil {
		return nil, nil, xerrors.Errorf("ffi.GenerateWindowPoSt err: %s", err.Error())
	}

	return proofs, faultySectors, nil
}

func pubSectorToPri(priSectorInfos []ffi.PrivateSectorInfo) (ffi.SortedPrivateSectorInfo, []abi.SectorNumber, error) {
	var out []ffi.PrivateSectorInfo
	var skipped []abi.SectorNumber

	if len(priSectorInfos) == 0 {
		return ffi.SortedPrivateSectorInfo{}, nil, xerrors.New("priSectorInfos is empty")
	}

	wdPostProof, err := priSectorInfos[0].SealProof.RegisteredWindowPoStProof()
	if err != nil {
		return ffi.SortedPrivateSectorInfo{}, nil, xerrors.Errorf("RegisteredWindowPoStProof fail, sealProof: %d", priSectorInfos[0].SealProof)
	}

	ssize, err := wdPostProof.SectorSize()
	if err != nil {
		return ffi.SortedPrivateSectorInfo{}, nil, xerrors.Errorf("get SectorSize err, wdPostProof: %d", wdPostProof)
	}

	for _, priSector := range priSectorInfos {
		if priSector.SealedSectorPath == "" || priSector.CacheDirPath == "" {
			log.Warnw("pubSectorToPri Sector FAULT: cache and/or sealed paths not found", "sector", priSector.SectorNumber, "sealed", priSector.SealedSectorPath, "cache", priSector.CacheDirPath)
			skipped = append(skipped, priSector.SectorNumber)
			continue
		}

		toCheck := map[string]int64{
			priSector.SealedSectorPath:                     1,
			filepath.Join(priSector.CacheDirPath, "p_aux"): 0,
		}

		addCachePathsForSectorSize(toCheck, priSector.CacheDirPath, ssize)

		for p, sz := range toCheck {
			st, err := os.Stat(p)
			if err != nil {
				log.Warnw("pubSectorToPri Sector FAULT: sector file stat error", "sector", priSector.SectorNumber, "sealed", priSector.SealedSectorPath, "cache", priSector.CacheDirPath, "file", p, "err", err)
				skipped = append(skipped, priSector.SectorNumber)
				break
			}

			if sz != 0 {
				if st.Size() != int64(ssize)*sz {
					log.Warnw("pubSectorToPri Sector FAULT: sector file is wrong size", "sector", priSector.SectorNumber, "sealed", priSector.SealedSectorPath, "cache", priSector.CacheDirPath, "file", p, "size", st.Size(), "expectSize", int64(ssize)*sz)
					skipped = append(skipped, priSector.SectorNumber)
					break
				}
			}
		}

		out = append(out, priSector)
	}

	return ffi.NewSortedPrivateSectorInfo(out...), skipped, nil
}

func addCachePathsForSectorSize(chk map[string]int64, cacheDir string, ssize abi.SectorSize) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		chk[filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat")] = 0
	case 32 << 30:
		for i := 0; i < 8; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
}

func (l *LocalWorker) getTaskRunningNum() int {
	l.lk.Lock()
	defer l.lk.Unlock()

	return len(l.runningTasks)
}

func (l *LocalWorker) isTaskRunning(ci storiface.CallID) bool {
	l.lk.Lock()
	defer l.lk.Unlock()

	if _, ok := l.runningTasks[ci]; ok {
		return false
	}

	return true
}

func (l *LocalWorker) taskStart(ci storiface.CallID) {
	l.lk.Lock()
	defer l.lk.Unlock()

	l.runningTasks[ci] = struct{}{}
}

func (l *LocalWorker) taskEnd(ci storiface.CallID) {
	l.lk.Lock()
	defer l.lk.Unlock()

	delete(l.runningTasks, ci)
}

type wctx struct {
	vals    context.Context
	closing chan struct{}
}

func (w *wctx) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (w *wctx) Done() <-chan struct{} {
	return w.closing
}

func (w *wctx) Err() error {
	select {
	case <-w.closing:
		return context.Canceled
	default:
		return nil
	}
}

func (w *wctx) Value(key interface{}) interface{} {
	return w.vals.Value(key)
}

var _ context.Context = &wctx{}
