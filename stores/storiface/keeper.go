package storiface

import (
	"context"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
)

type Prover interface {
	WindowPoSt(ctx context.Context, minerID abi.ActorID, deadline, partition uint64, open int64, priSectorInfos []ffi.PrivateSectorInfo, randomness abi.PoStRandomness) (proof []byte, faulty []abi.SectorNumber, err error)
}