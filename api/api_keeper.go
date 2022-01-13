package api

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/luluup777/wdkeep/stores/storiface"
)

type WdKeepManager interface {
	storiface.WorkerReturn

	WorkerConnect(context.Context, string) error
	WorkerInfos(context.Context) (map[uuid.UUID]storiface.WorkerInfo, error)
	WorkerJobs(context.Context) (map[uuid.UUID][]storiface.WorkerJob, error)

	Version(context.Context) (string, error)
	Session(context.Context) (uuid.UUID, error)
}

type WdKeepManagerStruct struct {
	Internal struct {
		ReturnWindowPoSt func(ctx context.Context, callID storiface.CallID, postProof []byte, faulty []abi.SectorNumber, err *storiface.CallError) error

		WorkerConnect func(context.Context, string) error
		WorkerInfos   func(context.Context) (map[uuid.UUID]storiface.WorkerInfo, error)
		WorkerJobs    func(context.Context) (map[uuid.UUID][]storiface.WorkerJob, error)
		Version       func(context.Context) (string, error)
		Session       func(context.Context) (uuid.UUID, error)
	}
}

func (w *WdKeepManagerStruct) ReturnWindowPoSt(ctx context.Context, callID storiface.CallID, postProof []byte, faulty []abi.SectorNumber, err *storiface.CallError) error {
	if w.Internal.ReturnWindowPoSt == nil {
		return ErrNotSupported
	}

	return w.Internal.ReturnWindowPoSt(ctx, callID, postProof, faulty, err)
}

func (w *WdKeepManagerStruct) WorkerConnect(ctx context.Context, url string) error {
	if w.Internal.WorkerConnect == nil {
		return ErrNotSupported
	}

	return w.Internal.WorkerConnect(ctx, url)
}

func (w *WdKeepManagerStruct) WorkerInfos(ctx context.Context) (map[uuid.UUID]storiface.WorkerInfo, error) {
	if w.Internal.WorkerInfos == nil {
		return nil, ErrNotSupported
	}

	return w.Internal.WorkerInfos(ctx)
}

func (w *WdKeepManagerStruct) WorkerJobs(ctx context.Context) (map[uuid.UUID][]storiface.WorkerJob, error) {
	if w.Internal.WorkerJobs == nil {
		return nil, ErrNotSupported
	}

	return w.Internal.WorkerJobs(ctx)
}

func (w *WdKeepManagerStruct) Version(ctx context.Context) (string, error) {
	if w.Internal.WorkerJobs == nil {
		return "", ErrNotSupported
	}

	return w.Internal.Version(ctx)
}

func (w *WdKeepManagerStruct) Session(ctx context.Context) (uuid.UUID, error) {
	if w.Internal.Session == nil {
		return uuid.UUID{}, ErrNotSupported
	}

	return w.Internal.Session(ctx)
}
