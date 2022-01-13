package keeper

import (
	"context"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statestore"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/luluup777/wdkeep/build"
	"github.com/luluup777/wdkeep/extern/worker"
	"github.com/luluup777/wdkeep/stores/storiface"
	"golang.org/x/xerrors"
	"sync"
	"time"
)

var log = logging.Logger("keeper")

type Keeper struct {
	workLk  sync.Mutex
	session uuid.UUID

	sched *scheduler

	work *statestore.StateStore

	callToWork map[storiface.CallID]WorkID
	callRes    map[storiface.CallID]chan result
	results    map[WorkID]result
	waitRes    map[WorkID]chan struct{}
}

func NewKeeper(kss *statestore.StateStore) *Keeper {
	return &Keeper{
		session:    uuid.New(),
		sched:      newScheduler(),
		work:       kss,
		callToWork: map[storiface.CallID]WorkID{},
		callRes:    map[storiface.CallID]chan result{},
		results:    map[WorkID]result{},
		waitRes:    map[WorkID]chan struct{}{},
	}
}

type result struct {
	r   interface{}
	err error
}

func (k *Keeper) WindowPoSt(ctx context.Context, minerID abi.ActorID, deadline, partition uint64, open int64, priSectorInfos []ffi.PrivateSectorInfo, randomness abi.PoStRandomness) (proof []byte, faulty []abi.SectorNumber, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wk, wait, cancel, err := k.getWork(ctx, minerID, deadline, partition, open)
	if err != nil {
		return nil, nil, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	var out wdPostResult
	waitRes := func() {
		p, werr := k.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(wdPostResult)
		}
	}

	if wait {
		waitRes()
		return out.postProof, out.faulty, waitErr
	}

	err = k.sched.Schedule(ctx, minerID, deadline, partition, open, func(ctx context.Context, w Worker) error {
		err := k.startWork(ctx, w, wk)(w.GenerateWindowPoSt(ctx, minerID, deadline, partition, open, priSectorInfos, randomness))
		if err != nil {
			return err
		}

		waitRes()
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return out.postProof, out.faulty, nil
}

type wdPostResult struct {
	postProof []byte
	faulty    []abi.SectorNumber
}

func (k *Keeper) ReturnWindowPoSt(ctx context.Context, callID storiface.CallID, postProof []byte, faulty []abi.SectorNumber, err *storiface.CallError) error {
	wdr := wdPostResult{
		postProof: postProof,
		faulty:    faulty,
	}

	return k.returnResult(ctx, callID, wdr, err)
}

func (k *Keeper) WorkerConnect(ctx context.Context, url string) error {
	w, err := worker.ConnectRemoteWorker(ctx, url)
	if err != nil {
		return xerrors.Errorf("connecting remote storage failed: %w", err)
	}

	log.Infof("Connected to a remote worker at %s", url)

	return k.addWorker(ctx, w)
}

func (k *Keeper) addWorker(ctx context.Context, w Worker) error {
	return k.sched.runWorker(ctx, w)
}

func (k *Keeper) WorkerInfos(context.Context) (map[uuid.UUID]storiface.WorkerInfo, error) {
	k.sched.workersLk.RLock()
	defer k.sched.workersLk.RUnlock()

	out := map[uuid.UUID]storiface.WorkerInfo{}

	for id, handle := range k.sched.workers {
		handle.lk.Lock()
		out[uuid.UUID(id)] = storiface.WorkerInfo{
			WorkerName: handle.info.WorkerName,
			WorkerID:   handle.info.WorkerID,
			RunNum:     handle.info.RunNum,
			RunLimit:   handle.info.RunLimit,
		}
		handle.lk.Unlock()
	}

	return out, nil
}

func (k *Keeper) WorkerJobs(context.Context) (map[uuid.UUID][]storiface.WorkerJob, error) {
	out := map[uuid.UUID][]storiface.WorkerJob{}

	k.workLk.Lock()
	defer k.workLk.Unlock()

	for id, work := range k.callToWork {
		var ws WorkState
		if err := k.work.Get(work).Get(&ws); err != nil {
			log.Errorf("WorkerJobs: get work %s: %+v", work, err)
		}

		wait := storiface.RWRetWait
		if _, ok := k.results[work]; ok {
			wait = storiface.RWReturned
		}
		if ws.Status == wsDone {
			wait = storiface.RWRetDone
		}

		out[uuid.UUID{}] = append(out[uuid.UUID{}], storiface.WorkerJob{
			ID:        id,
			RunStatus: wait,
			Start:     time.Unix(ws.StartTime, 0),
			Hostname:  ws.WorkerName,
		})
	}

	return out, nil
}

func (k *Keeper) Version(context.Context) (string, error) {
	return build.WdKeepManagerAPIVersion(), nil
}

func (k *Keeper) Session(context.Context) (uuid.UUID, error) {
	return k.session, nil
}
