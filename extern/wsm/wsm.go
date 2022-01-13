package wsm

import (
	"context"
	"github.com/filecoin-project/go-statemachine"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	logging "github.com/ipfs/go-log/v2"
	"github.com/luluup777/wdkeep/extern"
	"golang.org/x/xerrors"
	"sync"
)

var log = logging.Logger("wsm")

type WdPostStateMachine struct {
	api      extern.FullNodeFilteredAPI
	verifier ffiwrapper.Verifier

	startupWait sync.WaitGroup
	wdPostTasks *statemachine.StateGroup
}

func (w *WdPostStateMachine) Run(ctx context.Context) error {
	if err := w.restartTasks(ctx); err != nil {
		log.Errorf("%+v", err)
		return xerrors.Errorf("failed load sector states: %w", err)
	}

	return nil
}

func (w *WdPostStateMachine) restartTasks(ctx context.Context) error {
	defer w.startupWait.Done()
	// todo
	return nil
}