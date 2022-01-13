package keeper

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/luluup777/wdkeep/stores/storiface"
	"sync"
	"time"
)

var heartbeatInterval = 30 * time.Second

type WorkerID uuid.UUID
type WorkerAction func(ctx context.Context, w Worker) error

type Worker interface {
	storiface.WorkerCalls

	Info(context.Context) (storiface.WorkerInfo, error)
	Version(context.Context) (string, error)
	Session(context.Context) (uuid.UUID, error)
}

type workerHandle struct {
	info storiface.WorkerInfo

	enabled bool

	workerRpc Worker

	lk sync.Mutex

	running int64

	closeCh chan struct{}
}

type workerRequest struct {
	miner     abi.ActorID
	deadline  uint64
	partition uint64
	open      int64

	work WorkerAction

	start time.Time

	index     int
	indexHeap int

	ret chan<- workerResponse
	ctx context.Context
}

type workerResponse struct {
	err error
}
