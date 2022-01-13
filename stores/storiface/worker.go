package storiface

import (
	"context"
	"errors"
	"fmt"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"time"
)

type WorkerInfo struct {
	WorkerName string
	WorkerID   uuid.UUID

	RunLimit int
	RunNum   int
}

const (
	RWRetWait  = -1
	RWReturned = -2
	RWRetDone  = -3
)

type WorkerJob struct {
	ID CallID

	// 1+ - assigned
	// 0  - running
	// -1 - ret-wait
	// -2 - returned
	// -3 - ret-done
	RunStatus int
	Start     time.Time

	Hostname string `json:",omitempty"` // optional, set for ret-wait jobs
}

type CallID struct {
	Miner     abi.ActorID
	Deadline  uint64
	Partition uint64
	Open      int64
}

func (c CallID) String() string {
	return fmt.Sprintf("%d-%d-%d-%d", c.Miner, c.Deadline, c.Partition, c.Open)
}

var _ fmt.Stringer = &CallID{}

type WorkerCalls interface {
	GenerateWindowPoSt(ctx context.Context, minerID abi.ActorID, deadline, partition uint64, open int64, priSectorInfos []ffi.PrivateSectorInfo, randomness abi.PoStRandomness) (CallID, error)
}

type ErrorCode int

const (
	ErrUnknown ErrorCode = iota
)

const (
	ErrTempUnknown ErrorCode = iota + 100
	ErrTempWorkerRestart
)

type CallError struct {
	Code    ErrorCode
	Message string
}

func (c *CallError) Error() string {
	return fmt.Sprintf("wdpost call error %d: %s", c.Code, c.Message)
}

func (c *CallError) Unwrap() error {
	return errors.New(c.Message)
}

func Err(code ErrorCode, sub error) *CallError {
	return &CallError{
		Code:    code,
		Message: sub.Error(),
	}
}

type WorkerReturn interface {
	ReturnWindowPoSt(ctx context.Context, callID CallID, postProof []byte, faulty []abi.SectorNumber, err *CallError) error
}
