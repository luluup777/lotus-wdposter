package wdpost

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/luluup777/wdkeep/extern"
	"go.opencensus.io/trace"
	"golang.org/x/xerrors"
	"time"
)

type WindowPoStScheduler struct {
	api       extern.FullNodeFilteredAPI
	proofType abi.RegisteredPoStProof
	ch        *changeHandler

	actor address.Address
}

// NewWindowedPoStScheduler creates a new WindowPoStScheduler scheduler.
func NewWindowedPoStScheduler(api extern.FullNodeFilteredAPI, actor address.Address) (*WindowPoStScheduler, error) {
	mi, err := api.StateMinerInfo(context.TODO(), actor, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("getting sector size: %w", err)
	}

	return &WindowPoStScheduler{
		api:       api,
		proofType: mi.WindowPoStProofType,

		actor: actor,
	}, nil
}

func (s *WindowPoStScheduler) Run(ctx context.Context) {
	// Initialize change handler.

	// callbacks is a union of the fullNodeFilteredAPI and ourselves.
	callbacks := struct {
		extern.FullNodeFilteredAPI
		*WindowPoStScheduler
	}{s.api, s}

	s.ch = newChangeHandler(callbacks, s.actor)
	defer s.ch.shutdown()
	s.ch.start()

	var (
		notifs <-chan []*api.HeadChange
		err    error
		gotCur bool
	)

	// not fine to panic after this point
	for {
		if notifs == nil {
			notifs, err = s.api.ChainNotify(ctx)
			if err != nil {
				log.Errorf("ChainNotify error: %+v", err)

				build.Clock.Sleep(10 * time.Second)
				continue
			}

			gotCur = false
		}

		select {
		case changes, ok := <-notifs:
			if !ok {
				log.Warn("window post scheduler notifs channel closed")
				notifs = nil
				continue
			}

			if !gotCur {
				if len(changes) != 1 {
					log.Errorf("expected first notif to have len = 1")
					continue
				}
				chg := changes[0]
				if chg.Type != store.HCCurrent {
					log.Errorf("expected first notif to tell current ts")
					continue
				}

				ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.headChange")

				s.update(ctx, nil, chg.Val)

				span.End()
				gotCur = true
				continue
			}

			ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.headChange")

			var lowest, highest *types.TipSet = nil, nil

			for _, change := range changes {
				if change.Val == nil {
					log.Errorf("change.Val was nil")
				}
				switch change.Type {
				case store.HCRevert:
					lowest = change.Val
				case store.HCApply:
					highest = change.Val
				}
			}

			s.update(ctx, lowest, highest)

			span.End()
		case <-ctx.Done():
			return
		}
	}
}

func (s *WindowPoStScheduler) update(ctx context.Context, revert, apply *types.TipSet) {
	if apply == nil {
		log.Error("no new tipset in window post WindowPoStScheduler.update")
		return
	}
	err := s.ch.update(ctx, revert, apply)
	if err != nil {
		log.Errorf("handling head updates in window post sched: %+v", err)
	}
}
