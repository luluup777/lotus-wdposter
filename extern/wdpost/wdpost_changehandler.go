package wdpost

import (
	"bytes"
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	logging "github.com/ipfs/go-log/v2"
	"github.com/luluup777/wdkeep/extern"
	"golang.org/x/xerrors"
)

var log = logging.Logger("wdpost")

const (
	ChallengeConfidence = 10
)

// wdPoStCommands is the subset of the WindowPoStScheduler + full node APIs used
// by the changeHandler to execute actions and query state.
type wdPoStCommands interface {
	StateMinerProvingDeadline(context.Context, address.Address, types.TipSetKey) (*dline.Info, error)
	ChainHead(context.Context) (*types.TipSet, error)
	StateGetRandomnessFromBeacon(ctx context.Context, personalization crypto.DomainSeparationTag, randEpoch abi.ChainEpoch, entropy []byte, tsk types.TipSetKey) (abi.Randomness, error)
	StateMinerPartitions(context.Context, address.Address, uint64, types.TipSetKey) ([]api.Partition, error)
}

type changeHandler struct {
	api       wdPoStCommands
	actor     address.Address
	proveHdlr *proveHandler
}

func newChangeHandler(api wdPoStCommands, actor address.Address) *changeHandler {
	return &changeHandler{api: api, actor: actor, proveHdlr: newProver(api)}
}

func (ch *changeHandler) start() {
	go ch.proveHdlr.run()
}

func (ch *changeHandler) update(ctx context.Context, revert *types.TipSet, advance *types.TipSet) error {
	// Get the current deadline period
	di, err := ch.api.StateMinerProvingDeadline(ctx, ch.actor, advance.Key())
	if err != nil {
		return err
	}

	if !di.PeriodStarted() {
		return nil // not proving anything yet
	}

	hc := &headChange{
		ctx:     ctx,
		revert:  revert,
		advance: advance,
		di:      di,
	}

	select {
	case ch.proveHdlr.hcs <- hc:
	case <-ch.proveHdlr.shutdownCtx.Done():
	case <-ctx.Done():
	}

	return nil
}

func (ch *changeHandler) shutdown() {
	ch.proveHdlr.shutdown()
}

type headChange struct {
	ctx     context.Context
	revert  *types.TipSet
	advance *types.TipSet
	di      *dline.Info
}

type currentPost struct {
	di    *dline.Info
	abort context.CancelFunc
}

// proveHandler generates proofs
type proveHandler struct {
	actor address.Address

	api     wdPoStCommands
	trigger extern.PostTrigger

	hcs chan *headChange

	current *currentPost

	currentTS *types.TipSet

	shutdownCtx context.Context
	shutdown    context.CancelFunc
}

func newProver(api wdPoStCommands) *proveHandler {
	ctx, cancel := context.WithCancel(context.Background())
	return &proveHandler{
		api:         api,
		hcs:         make(chan *headChange),
		shutdownCtx: ctx,
		shutdown:    cancel,
	}
}

func (p *proveHandler) run() {
	// Abort proving on shutdown
	defer func() {
		if p.current != nil {
			p.current.abort()
		}
	}()

	for p.shutdownCtx.Err() == nil {
		select {
		case <-p.shutdownCtx.Done():
			return

		case hc := <-p.hcs:
			// Head changed
			p.processHeadChange(hc.advance, hc.di)
		}
	}
}

func (p *proveHandler) processHeadChange(newTS *types.TipSet, di *dline.Info) {
	// If the post window has expired, abort the current proof
	if p.current != nil && newTS.Height() >= p.current.di.Close {
		p.current = nil
	}

	// Only generate one proof at a time
	if p.current != nil {
		return
	}

	// Check if the chain is above the Challenge height for the post window
	if newTS.Height() < di.Challenge+ChallengeConfidence {
		return
	}

	p.current = &currentPost{di: di}

	p.runPoStCycle(context.TODO(), *di, newTS)
}

func (p *proveHandler) runPoStCycle(ctx context.Context, di dline.Info, ts *types.TipSet) error {
	actorID, err := address.IDFromAddress(p.actor)
	if err != nil {
		return err
	}

	// trigger declare
	err = p.trigger.TriggerDeclare(abi.ActorID(actorID), di.Index)
	if err != nil {
		log.Errorf("runPoStCycle TriggerDeclare: %s", err.Error())
	}

	// trigger window post
	buf := new(bytes.Buffer)
	if err := p.actor.MarshalCBOR(buf); err != nil {
		return xerrors.Errorf("failed to marshal address to cbor: %w", err)
	}

	headTs, err := p.api.ChainHead(ctx)
	if err != nil {
		return xerrors.Errorf("getting current head: %w", err)
	}

	rand, err := p.api.StateGetRandomnessFromBeacon(ctx, crypto.DomainSeparationTag_WindowedPoStChallengeSeed, di.Challenge, buf.Bytes(), headTs.Key())
	if err != nil {
		return xerrors.Errorf("failed to get chain randomness from beacon for window post (ts=%d; deadline=%d): %w", ts.Height(), di, err)
	}

	partitions, err := p.api.StateMinerPartitions(ctx, p.actor, di.Index, ts.Key())
	if err != nil {
		return xerrors.Errorf("getting partitions: %w", err)
	}

	if len(partitions) == 0 {
		return nil
	}

	for index, _ := range partitions {
		err = p.trigger.TriggerWindowPoSt(abi.ActorID(actorID), di.Index, index, di.Open, di.Close, append(abi.PoStRandomness{}, rand...))
		if err != nil {
			log.Errorf("runPoStCycle TriggerWindowPoSt: %s", err.Error())
		} else {
			log.Infow("runPoStCycle TriggerWindowPoSt success", "minerID", p.actor.String(), "deadlineID", di.Index, "partitionID", index)
		}
	}

	return nil
}
