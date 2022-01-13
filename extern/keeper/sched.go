package keeper

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/luluup777/wdkeep/stores/storiface"
	"golang.org/x/xerrors"
	"sync"
	"sync/atomic"
	"time"
)

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	schedule chan *workerRequest

	taskQueue *requestQueue

	closCh chan struct{}
}

func newScheduler() *scheduler {
	return &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule: make(chan *workerRequest),

		taskQueue: &requestQueue{},

		closCh: make(chan struct{}),
	}
}

func (sh *scheduler) Schedule(ctx context.Context, miner abi.ActorID, deadline, partition uint64, open int64, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		miner:     miner,
		deadline:  deadline,
		partition: partition,
		open:      open,

		work: work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closCh:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closCh:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sh *scheduler) runSched() {

	for {
		var doSched bool

		select {
		case req := <-sh.schedule:
			sh.taskQueue.Push(req)
			doSched = true
		case <-sh.closCh:
			return
		}

		if doSched {
			sh.trySched()
		}
	}
}

func (sh *scheduler) trySched() {
	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	startLen := sh.taskQueue.Len()

	sqi := 0
	for wid, w := range sh.workers {
		if sh.taskQueue.Len() == 0 {
			break
		}

		task := (*sh.taskQueue)[sqi]

		if !w.enabled {
			continue
		}

		if atomic.LoadInt64(&w.running) >= int64(w.info.RunLimit) {
			continue
		}

		sh.taskQueue.Remove(sqi)

		log.Infow("sched success", "taskID", storiface.CallID{
			Miner:     task.miner,
			Deadline:  task.deadline,
			Partition: task.partition,
			Open:      task.open,
		}.String(), "wid", wid, "workerName", w.info.WorkerName)

		if err := sh.execWork(w, task); err != nil {
			go task.respond(xerrors.Errorf("execWork error: %w", err))
		}
	}

	log.Infof("trySched %d task", startLen-sh.taskQueue.Len())
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

func (sh *scheduler) execWork(w *workerHandle, req *workerRequest) error {
	w.startWork()
	go func() {
		defer w.endWork()

		err := req.work(req.ctx, w.workerRpc)
		select {
		case req.ret <- workerResponse{err: err}:
		case <-req.ctx.Done():
			log.Warnf("request got cancelled before we could respond")
		case <-sh.closCh:
			log.Warnf("scheduler closed while sending response")
		}

		if err != nil {
			log.Errorf("error executing worker: %+v", err)
		}
	}()

	return nil
}

func (w *workerHandle) startWork() {
	atomic.AddInt64(&w.running, 1)
}

func (w *workerHandle) endWork() {
	atomic.AddInt64(&w.running, -1)
}
