package keeper

import (
	"context"
	"golang.org/x/xerrors"
	"time"
)

type schedWorker struct {
	sched  *scheduler
	worker *workerHandle

	wid WorkerID

	heartbeatTimer *time.Ticker
}

func (sh *scheduler) runWorker(ctx context.Context, w Worker) error {
	info, err := w.Info(ctx)
	if err != nil {
		return xerrors.Errorf("getting worker info: %w", err)
	}

	sessID, err := w.Session(ctx)
	if err != nil {
		return xerrors.Errorf("getting worker session: %w", err)
	}

	worker := &workerHandle{
		workerRpc: w,
		info:      info,
		running:   0,
		enabled:   true,
		closeCh:   make(chan struct{}),
	}

	wid := WorkerID(sessID)

	sh.workersLk.Lock()
	oldWorker, exist := sh.workers[wid]
	if exist {
		log.Warnw("duplicated worker added", "id", wid)
		oldWorker.info = worker.info
		sh.workersLk.Unlock()
		return nil
	}

	sh.workers[wid] = worker
	sh.workersLk.Unlock()

	sw := &schedWorker{
		sched:  sh,
		worker: worker,

		wid: wid,

		heartbeatTimer: time.NewTicker(heartbeatInterval),
	}

	go sw.handleWorker()

	return nil
}

func (sw *schedWorker) handleWorker() {
	worker, sched := sw.worker, sw.sched
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	defer close(sw.worker.closeCh)

	defer func() {
		log.Warnw("Worker closing", "workerName", sw.wid)

		if err := sw.disable(); err != nil {
			log.Warnw("failed to disable worker", "worker", sw.wid, "error", err)
		}

		sched.workersLk.Lock()
		delete(sched.workers, sw.wid)
		sched.workersLk.Unlock()
	}()

	defer sw.heartbeatTimer.Stop()

	for {
		select {
		case <-sw.heartbeatTimer.C:

			if !sw.checkSession(ctx) {
				return
			}

			// session looks good
			{
				sched.workersLk.Lock()
				worker.enabled = true
				sched.workersLk.Unlock()
			}
		}
	}
}

func (sw *schedWorker) disable() error {
	sw.sched.workersLk.Lock()
	defer sw.sched.workersLk.Unlock()

	sw.worker.enabled = false
	return nil
}

func (sw *schedWorker) checkSession(ctx context.Context) bool {
	for {
		sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
		curSes, err := sw.worker.workerRpc.Session(sctx)
		scancel()
		if err != nil {
			// Likely temporary error

			log.Warnw("failed to check worker session", "error", err)

			if err := sw.disable(); err != nil {
				log.Warnw("failed to disable worker with session error", "worker", sw.wid, "error", err)
			}

			select {
			case <-sw.heartbeatTimer.C:
				continue
			case <-sw.sched.closCh:
				return false
			case <-sw.worker.closeCh:
				return false
			}
			continue
		}

		if WorkerID(curSes) != sw.wid {
			// worker restarted
			log.Warnw("worker session changed (worker restarted?)", "initial", sw.wid, "current", curSes)

			return false
		}

		return true
	}
}
