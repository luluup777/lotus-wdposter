package wdpost

import (
	"context"
	"errors"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/storage/ctladdr"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/filecoin-project/lotus/storage/sealer"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/ipfs/go-cid"
	"github.com/spf13/viper"
	"os"
	"sync"
	"time"
)

type MinerInfo struct {
	Actor    string `yaml:"actor"`
	PostAddr string `yaml:"postaddr"`
	Stop     bool   `yaml:"stop"`
}

type WindowPoStClusterScheduler struct {
	api                                     NodeAPI
	feeCfg                                  config.MinerFeeConfig
	addrSel                                 sealing.AddressSelector
	prover                                  storiface.ProverPoSt
	verifier                                storiface.Verifier
	faultTracker                            sealer.FaultTracker
	disablePreChecks                        bool
	maxPartitionsPerPostMessage             int
	maxPartitionsPerRecoveryMessage         int
	singleRecoveringPartitionPerPostMessage bool
	journal                                 journal.Journal

	lk           sync.Mutex
	minerTracker map[string]context.CancelFunc
	minerWdPost  map[string]*WindowPoStScheduler
}

// NewWindowedPoStClusterScheduler creates a new WindowPoStScheduler cluster scheduler.
func NewWindowedPoStClusterScheduler(api NodeAPI,
	cfg config.MinerFeeConfig,
	pcfg config.ProvingConfig,
	as sealing.AddressSelector,
	sp storiface.ProverPoSt,
	verif storiface.Verifier,
	ft sealer.FaultTracker,
	j journal.Journal) (*WindowPoStClusterScheduler, error) {
	return &WindowPoStClusterScheduler{
		api:                                     api,
		feeCfg:                                  cfg,
		addrSel:                                 as,
		prover:                                  sp,
		verifier:                                verif,
		faultTracker:                            ft,
		disablePreChecks:                        pcfg.DisableWDPoStPreChecks,
		maxPartitionsPerPostMessage:             pcfg.MaxPartitionsPerPoStMessage,
		maxPartitionsPerRecoveryMessage:         pcfg.MaxPartitionsPerRecoveryMessage,
		singleRecoveringPartitionPerPostMessage: pcfg.SingleRecoveringPartitionPerPostMessage,
		journal:                                 j,
		minerTracker:                            map[string]context.CancelFunc{},
		minerWdPost:                             map[string]*WindowPoStScheduler{},
	}, nil
}

func (s *WindowPoStClusterScheduler) Run(ctx context.Context) {
	miners, err := loadMinerInfo()
	if err != nil {
		panic(err)
	}
	for _, mi := range miners {
		if mi.Stop {
			continue
		}

		err = s.startupWindowPost(mi)
		if err != nil {
			log.Warnw("startupWindowPost", "err", err)
		}
	}

	var ticker = time.NewTicker(2 * time.Minute)
	for {
		select {
		case <-ticker.C:
			miners, err = loadMinerInfo()
			if err != nil {
				log.Errorw("loadMinerInfo", "err", err)
				continue
			}

			for _, mi := range miners {
				if cancel, ok := s.minerTracker[mi.Actor]; ok {
					if mi.Stop {
						log.Warnw("miner post stop", "miner", mi.Actor)
						cancel()
						delete(s.minerTracker, mi.Actor)

						s.lk.Lock()
						delete(s.minerWdPost, mi.PostAddr)
						s.lk.Unlock()
					}
					continue
				}

				err = s.startupWindowPost(mi)
				if err != nil {
					log.Warnw("startupWindowPost", "err", err)
				}
			}
		case <-ctx.Done():
			for _, cancel := range s.minerTracker {
				cancel()
			}
			return
		}
	}
}

func loadMinerInfo() ([]MinerInfo, error) {
	minerPath, ok := os.LookupEnv("LOTUS_MINER_PATH")
	if !ok {
		panic("no set LOTUS_MINER_PATH")
	}

	conf := struct {
		Posts []MinerInfo `yaml:"posts"`
	}{}

	viper.SetConfigName("minerinfo")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(minerPath)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	err = viper.Unmarshal(&conf)
	if err != nil {
		return nil, err
	}

	return conf.Posts, nil
}

func (s *WindowPoStClusterScheduler) startupWindowPost(post MinerInfo) error {
	log.Infow("startupWindowPost", "miner", post.Actor, "postAddr", post.PostAddr)
	actor, err := address.NewFromString(post.Actor)
	if err != nil {
		return err
	}

	if _, ok := s.minerTracker[post.Actor]; ok {
		return nil
	}

	postAddr, err := address.NewFromString(post.PostAddr)
	if err != nil {
		return err
	}

	mi, err := s.api.StateMinerInfo(context.TODO(), actor, types.EmptyTSK)
	if err != nil {
		return err
	}

	var isOk = false
	for _, addr := range append(mi.ControlAddresses, mi.Worker) {
		if addr == postAddr {
			isOk = true
			break
		}
	}
	if !isOk {
		return errors.New("invalid post address")
	}

	fps, err := NewWindowedPoStScheduler(s.api, s.feeCfg,
		config.ProvingConfig{
			DisableWDPoStPreChecks:                  s.disablePreChecks,
			MaxPartitionsPerPoStMessage:             s.maxPartitionsPerPostMessage,
			MaxPartitionsPerRecoveryMessage:         s.maxPartitionsPerRecoveryMessage,
			SingleRecoveringPartitionPerPostMessage: s.singleRecoveringPartitionPerPostMessage,
		},
		&PostAddress{postAddr: postAddr}, s.prover, s.verifier, s.faultTracker, s.journal, actor)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.minerTracker[post.Actor] = cancel

	s.lk.Lock()
	wdPost := *fps
	s.minerWdPost[post.Actor] = &wdPost
	s.lk.Unlock()

	go fps.Run(ctx)

	return nil
}

type PostAddress struct {
	postAddr address.Address
}

func (post *PostAddress) AddressFor(ctx context.Context, a ctladdr.NodeApi, mi api.MinerInfo, use api.AddrUse, goodFunds, minFunds abi.TokenAmount) (address.Address, abi.TokenAmount, error) {
	b, err := a.WalletBalance(ctx, post.postAddr)
	if err != nil {
		log.Errorw("checking control address balance", "addr", post.postAddr, "error", err)
		return post.postAddr, abi.NewTokenAmount(0), err
	}
	return post.postAddr, b, nil
}

func (s *WindowPoStClusterScheduler) ComputePoSt(ctx context.Context, actor string, dlIdx uint64, ts *types.TipSet) ([]miner.SubmitWindowedPoStParams, error) {
	var wdPostScheduler WindowPoStScheduler
	s.lk.Lock()
	if wdPost, ok := s.minerWdPost[actor]; ok {
		wdPostScheduler = *wdPost
	} else {
		s.lk.Unlock()
		return nil, errors.New("miner does not exist")
	}
	s.lk.Unlock()

	return wdPostScheduler.ComputePoSt(ctx, dlIdx, ts)
}

func (s *WindowPoStClusterScheduler) ManualFaultRecovery(ctx context.Context, actor string, sectors []abi.SectorNumber) ([]cid.Cid, error) {
	var wdPostScheduler WindowPoStScheduler
	s.lk.Lock()
	if wdPost, ok := s.minerWdPost[actor]; ok {
		wdPostScheduler = *wdPost
	} else {
		s.lk.Unlock()
		return nil, errors.New("miner does not exist")
	}
	s.lk.Unlock()

	minerId, err := address.NewFromString(actor)
	if err != nil {
		return nil, err
	}

	return wdPostScheduler.ManualFaultRecovery(ctx, minerId, sectors)
}
