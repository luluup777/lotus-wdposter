package keeper

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/luluup777/wdkeep/stores/storiface"
	"golang.org/x/xerrors"
	"os"
	"time"
)

type WorkStatus string
type WorkID string

const (
	wsStarted WorkStatus = "started"
	wsRunning WorkStatus = "running"
	wsDone    WorkStatus = "done"
)

type WorkState struct {
	ID WorkID

	Status WorkStatus

	WorkerCall storiface.CallID // Set when entering wsRunning
	WorkError  string           // Status = wsDone, set when failed to start work

	WorkerName string

	StartTime int64 // unix seconds
}

func newWorkID(minerId abi.ActorID, deadline, partition uint64, open int64) WorkID {
	callID := storiface.CallID{
		Miner:     minerId,
		Deadline:  deadline,
		Partition: partition,
		Open:      open,
	}

	return WorkID(callID.String())
}

func (k *Keeper) setupWorkTracker() {
	k.workLk.Lock()
	defer k.workLk.Unlock()

	var ids []WorkState
	if err := k.work.List(&ids); err != nil {
		log.Error("getting work IDs") // quite bad
		return
	}

	for _, st := range ids {
		wid := st.ID

		if os.Getenv("ABORT_UNFINISHED_WORK") == "1" {
			st.Status = wsDone
		}

		switch st.Status {
		case wsStarted:
			log.Warnf("dropping non-running work %s", wid)

			if err := k.work.Get(wid).End(); err != nil {
				log.Errorf("cleannig up work state for %s", wid)
			}
		case wsDone:
			// can happen after restart, abandoning work, and another restart
			log.Warnf("dropping done work, no result, wid %s", wid)

			if err := k.work.Get(wid).End(); err != nil {
				log.Errorf("cleannig up work state for %s", wid)
			}
		case wsRunning:
			log.Infow("task running", "wid", wid, "workerName", st.WorkerName)
			k.callToWork[st.WorkerCall] = wid
		}
	}
}

// returns wait=true when the task is already tracked/running
func (k *Keeper) getWork(ctx context.Context, minerId abi.ActorID, deadline, partition uint64, open int64) (wid WorkID, wait bool, cancel func(), err error) {
	wid = newWorkID(minerId, deadline, partition, open)
	if err != nil {
		return "", false, nil, xerrors.Errorf("creating WorkID: %w", err)
	}

	k.workLk.Lock()
	defer k.workLk.Unlock()

	have, err := k.work.Has(wid)
	if err != nil {
		return "", false, nil, xerrors.Errorf("failed to check if the task is already tracked: %w", err)
	}

	if !have {
		err := k.work.Begin(wid, &WorkState{
			ID:     wid,
			Status: wsStarted,
		})
		if err != nil {
			return "", false, nil, xerrors.Errorf("failed to track task start: %w", err)
		}

		return wid, false, func() {
			k.workLk.Lock()
			defer k.workLk.Unlock()

			have, err := k.work.Has(wid)
			if err != nil {
				log.Errorf("cancel: work has error: %+v", err)
				return
			}

			if !have {
				return // expected / happy path
			}

			var ws WorkState
			if err := k.work.Get(wid).Get(&ws); err != nil {
				log.Errorf("cancel: get work %s: %+v", wid, err)
				return
			}

			switch ws.Status {
			case wsStarted:
				log.Warnf("canceling started (not running) work %s", wid)

				if err := k.work.Get(wid).End(); err != nil {
					log.Errorf("cancel: failed to cancel started work %s: %+v", wid, err)
					return
				}
			case wsDone:
				// TODO: still remove?
				log.Warnf("cancel called on work %s in 'done' state", wid)
			case wsRunning:
				log.Warnf("cancel called on work %s in 'running' state (manager shutting down?)", wid)
			}

		}, nil
	}

	// already started

	return wid, true, func() {}, nil
}

func (k *Keeper) startWork(ctx context.Context, w Worker, wk WorkID) func(callID storiface.CallID, err error) error {
	return func(callID storiface.CallID, err error) error {
		var workerName string
		info, ierr := w.Info(ctx)
		if ierr != nil {
			workerName = "[err]"
		} else {
			workerName = info.WorkerName
		}

		k.workLk.Lock()
		defer k.workLk.Unlock()

		if err != nil {
			merr := k.work.Get(wk).Mutate(func(ws *WorkState) error {
				ws.Status = wsDone
				ws.WorkError = err.Error()
				return nil
			})

			if merr != nil {
				return xerrors.Errorf("failed to start work and to track the error; merr: %+v, err: %w", merr, err)
			}
			return err
		}

		err = k.work.Get(wk).Mutate(func(ws *WorkState) error {
			_, ok := k.results[wk]
			if ok {
				log.Warn("work returned before we started tracking it")
				ws.Status = wsDone
			} else {
				ws.Status = wsRunning
			}
			ws.WorkerCall = callID
			ws.WorkerName = workerName
			ws.StartTime = time.Now().Unix()
			return nil
		})
		if err != nil {
			return xerrors.Errorf("registering running work: %w", err)
		}

		k.callToWork[callID] = wk

		return nil
	}
}

func (k *Keeper) waitWork(ctx context.Context, wid WorkID) (interface{}, error) {
	k.workLk.Lock()

	var ws WorkState
	if err := k.work.Get(wid).Get(&ws); err != nil {
		k.workLk.Unlock()
		return nil, xerrors.Errorf("getting work status: %w", err)
	}

	if ws.Status == wsStarted {
		k.workLk.Unlock()
		return nil, xerrors.Errorf("waitWork called for work in 'started' state")
	}

	// sanity check
	wk := k.callToWork[ws.WorkerCall]
	if wk != wid {
		k.workLk.Unlock()
		return nil, xerrors.Errorf("wrong callToWork mapping for call %s; expected %s, got %s", ws.WorkerCall, wid, wk)
	}

	// make sure we don't have the result ready
	cr, ok := k.callRes[ws.WorkerCall]
	if ok {
		delete(k.callToWork, ws.WorkerCall)

		if len(cr) == 1 {
			err := k.work.Get(wk).End()
			if err != nil {
				k.workLk.Unlock()
				// Not great, but not worth discarding potentially multi-hour computation over this
				log.Errorf("marking work as done: %+v", err)
			}

			res := <-cr
			delete(k.callRes, ws.WorkerCall)

			k.workLk.Unlock()
			return res.r, res.err
		}

		k.workLk.Unlock()
		return nil, xerrors.Errorf("something else in waiting on callRes")
	}

	done := func() {
		delete(k.results, wid)

		_, ok := k.callToWork[ws.WorkerCall]
		if ok {
			delete(k.callToWork, ws.WorkerCall)
		}

		err := k.work.Get(wk).End()
		if err != nil {
			// Not great, but not worth discarding potentially multi-hour computation over this
			log.Errorf("marking work as done: %+v", err)
		}
	}

	// the result can already be there if the work was running, manager restarted,
	// and the worker has delivered the result before we entered waitWork
	res, ok := k.results[wid]
	if ok {
		done()
		k.workLk.Unlock()
		return res.r, res.err
	}

	ch, ok := k.waitRes[wid]
	if !ok {
		ch = make(chan struct{})
		k.waitRes[wid] = ch
	}

	k.workLk.Unlock()

	select {
	case <-ch:
		k.workLk.Lock()
		defer k.workLk.Unlock()

		res := k.results[wid]
		done()

		return res.r, res.err
	case <-ctx.Done():
		return nil, xerrors.Errorf("waiting for work result: %w", ctx.Err())
	}
}

func (k *Keeper) returnResult(ctx context.Context, callID storiface.CallID, r interface{}, cerr *storiface.CallError) error {
	res := result{
		r: r,
	}
	if cerr != nil {
		res.err = cerr
	}

	k.workLk.Lock()
	defer k.workLk.Unlock()

	wid, ok := k.callToWork[callID]
	if !ok {
		rch, ok := k.callRes[callID]
		if !ok {
			rch = make(chan result, 1)
			k.callRes[callID] = rch
		}

		if len(rch) > 0 {
			return xerrors.Errorf("callRes channel already has a response")
		}
		if cap(rch) == 0 {
			return xerrors.Errorf("expected rch to be buffered")
		}

		rch <- res
		return nil
	}

	_, ok = k.results[wid]
	if ok {
		return xerrors.Errorf("result for call %v already reported", wid)
	}

	k.results[wid] = res

	err := k.work.Get(wid).Mutate(func(ws *WorkState) error {
		ws.Status = wsDone
		return nil
	})
	if err != nil {
		log.Errorf("marking work as done: %+v", err)
	}

	_, found := k.waitRes[wid]
	if found {
		close(k.waitRes[wid])
		delete(k.waitRes, wid)
	}

	return nil
}

func (k *Keeper) Abort(ctx context.Context, call storiface.CallID) error {
	return k.returnResult(ctx, call, nil, storiface.Err(storiface.ErrUnknown, xerrors.New("task aborted")))
}
