package api

import (
	"context"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/luluup777/wdkeep/stores/storiface"
)

type WdKeepWorker interface {
	storiface.WorkerCalls

	Info(context.Context) (storiface.WorkerInfo, error)
	Version(context.Context) (string, error)
	Session(context.Context) (uuid.UUID, error)
}

type WdKeepWorkerStruct struct {
	Internal struct {
		GenerateWindowPoSt func(ctx context.Context, minerID abi.ActorID, deadline, partition uint64, open int64, privateSectorInfos []ffi.PrivateSectorInfo, randomness abi.PoStRandomness) (storiface.CallID, error)

		Info    func(context.Context) (storiface.WorkerInfo, error)
		Version func(context.Context) (string, error)
		Session func(context.Context) (uuid.UUID, error)
	}
}

func (w *WdKeepWorkerStruct) GenerateWindowPoSt(ctx context.Context, minerID abi.ActorID, deadline, partition uint64, open int64, privateSectorInfos []ffi.PrivateSectorInfo, randomness abi.PoStRandomness) (storiface.CallID, error) {
	if w.Internal.GenerateWindowPoSt == nil {
		return storiface.CallID{}, ErrNotSupported
	}

	return w.Internal.GenerateWindowPoSt(ctx, minerID, deadline, partition, open, privateSectorInfos, randomness)
}

func (w *WdKeepWorkerStruct) Info(ctx context.Context) (storiface.WorkerInfo, error) {
	if w.Internal.Info == nil {
		return storiface.WorkerInfo{}, ErrNotSupported
	}

	return w.Internal.Info(ctx)
}

func (w *WdKeepWorkerStruct) Version(ctx context.Context) (string, error) {
	if w.Internal.Version == nil {
		return "", ErrNotSupported
	}

	return w.Internal.Version(ctx)
}

func (w *WdKeepWorkerStruct) Session(ctx context.Context) (uuid.UUID, error) {
	if w.Internal.Session == nil {
		return uuid.UUID{}, ErrNotSupported
	}

	return w.Internal.Session(ctx)
}
