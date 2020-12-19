package storiface

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
)

type WorkerInfo struct {
	Hostname string

	Resources       WorkerResources
	TaskResourcesLk sync.Mutex
	TaskResources   map[sealtasks.TaskType]*TaskConfig
}

type WorkerResources struct {
	MemPhysical uint64
	MemSwap     uint64

	MemReserved uint64 // Used by system / other processes

	CPUs uint64 // Logical cores
	GPUs []string
}

type WorkerStats struct {
	Info    WorkerInfo
	Enabled bool

	MemUsedMin uint64
	MemUsedMax uint64
	GpuUsed    bool   // nolint
	CpuUse     uint64 // nolint
}

const (
	RWRetWait  = -1
	RWReturned = -2
	RWRetDone  = -3
)

type WorkerJob struct {
	ID     CallID
	Sector abi.SectorID
	Task   sealtasks.TaskType

	// 1+ - assigned
	// 0  - running
	// -1 - ret-wait
	// -2 - returned
	// -3 - ret-done
	RunWait int
	Start   time.Time

	Hostname string `json:",omitempty"` // optional, set for ret-wait jobs
}

type CallID struct {
	Sector abi.SectorID
	ID     uuid.UUID
}

func (c CallID) String() string {
	return fmt.Sprintf("%d-%d-%s", c.Sector.Miner, c.Sector.Number, c.ID)
}

var _ fmt.Stringer = &CallID{}

var UndefCall CallID

type WorkerCalls interface {
	AddPiece(ctx context.Context, sector storage.SectorRef, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (CallID, error)
	SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (CallID, error)
	SealPreCommit2(ctx context.Context, sector storage.SectorRef, pc1o storage.PreCommit1Out) (CallID, error)
	SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (CallID, error)
	SealCommit2(ctx context.Context, sector storage.SectorRef, c1o storage.Commit1Out) (CallID, error)
	FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (CallID, error)
	ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) (CallID, error)
	MoveStorage(ctx context.Context, sector storage.SectorRef, types SectorFileType) (CallID, error)
	UnsealPiece(context.Context, storage.SectorRef, UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) (CallID, error)
	ReadPiece(context.Context, io.Writer, storage.SectorRef, UnpaddedByteIndex, abi.UnpaddedPieceSize) (CallID, error)
	Fetch(context.Context, storage.SectorRef, SectorFileType, PathType, AcquireMode) (CallID, error)
}

type ErrorCode int

const (
	ErrUnknown ErrorCode = iota
)

const (
	// Temp Errors
	ErrTempUnknown ErrorCode = iota + 100
	ErrTempWorkerRestart
	ErrTempAllocateSpace
)

type CallError struct {
	Code    ErrorCode
	Message string
	sub     error
}

func (c *CallError) Error() string {
	return fmt.Sprintf("storage call error %d: %s", c.Code, c.Message)
}

func (c *CallError) Unwrap() error {
	if c.sub != nil {
		return c.sub
	}

	return errors.New(c.Message)
}

func Err(code ErrorCode, sub error) *CallError {
	return &CallError{
		Code:    code,
		Message: sub.Error(),

		sub: sub,
	}
}

type WorkerReturn interface {
	ReturnAddPiece(ctx context.Context, callID CallID, pi abi.PieceInfo, err *CallError) error
	ReturnSealPreCommit1(ctx context.Context, callID CallID, p1o storage.PreCommit1Out, err *CallError) error
	ReturnSealPreCommit2(ctx context.Context, callID CallID, sealed storage.SectorCids, err *CallError) error
	ReturnSealCommit1(ctx context.Context, callID CallID, out storage.Commit1Out, err *CallError) error
	ReturnSealCommit2(ctx context.Context, callID CallID, proof storage.Proof, err *CallError) error
	ReturnFinalizeSector(ctx context.Context, callID CallID, err *CallError) error
	ReturnReleaseUnsealed(ctx context.Context, callID CallID, err *CallError) error
	ReturnMoveStorage(ctx context.Context, callID CallID, err *CallError) error
	ReturnUnsealPiece(ctx context.Context, callID CallID, err *CallError) error
	ReturnReadPiece(ctx context.Context, callID CallID, ok bool, err *CallError) error
	ReturnFetch(ctx context.Context, callID CallID, err *CallError) error
}

type TaskConfig struct {
	LimitCount int
	RunCount   int
}

type taskLimitConfig struct {
	AddPiece     int
	PreCommit1   int
	PreCommit2   int
	Commit1      int
	Commit2      int
	Fetch        int
	Finalize     int
	Unseal       int
	ReadUnsealed int
}

func NewTaskLimitConfig() map[sealtasks.TaskType]*TaskConfig {
	config := &taskLimitConfig{
		AddPiece:     1,
		PreCommit1:   1,
		PreCommit2:   1,
		Commit1:      1,
		Commit2:      1,
		Fetch:        1,
		Finalize:     1,
		Unseal:       1,
		ReadUnsealed: 1,
	}

	ability := ""
	if env, ok := os.LookupEnv("ABILITY"); ok {
		ability = strings.Replace(string(env), " ", "", -1) // 去除空格
		ability = strings.Replace(ability, "\n", "", -1)    // 去除换行符
	}
	splitArr := strings.Split(ability, ",")
	for _, part := range splitArr {
		if strings.Contains(part, "AP:") {
			splitPart := strings.Split(part, ":")
			if intCount, err := strconv.Atoi(splitPart[1]); err == nil {
				config.AddPiece = intCount
			}
		} else if strings.Contains(part, "PC1:") {
			splitPart := strings.Split(part, ":")
			if intCount, err := strconv.Atoi(splitPart[1]); err == nil {
				config.PreCommit1 = intCount
			}
		} else if strings.Contains(part, "PC2:") {
			splitPart := strings.Split(part, ":")
			if intCount, err := strconv.Atoi(splitPart[1]); err == nil {
				config.PreCommit2 = intCount
			}
		} else if strings.Contains(part, "C1:") {
			splitPart := strings.Split(part, ":")
			if intCount, err := strconv.Atoi(splitPart[1]); err == nil {
				config.Commit1 = intCount
			}
		} else if strings.Contains(part, "C2:") {
			splitPart := strings.Split(part, ":")
			if intCount, err := strconv.Atoi(splitPart[1]); err == nil {
				config.Commit2 = intCount
			}
		} else if strings.Contains(part, "GET:") {
			splitPart := strings.Split(part, ":")
			if intCount, err := strconv.Atoi(splitPart[1]); err == nil {
				config.Fetch = intCount
			}
		} else if strings.Contains(part, "FIN:") {
			splitPart := strings.Split(part, ":")
			if intCount, err := strconv.Atoi(splitPart[1]); err == nil {
				config.Finalize = intCount
			}
		} else if strings.Contains(part, "UNS:") {
			splitPart := strings.Split(part, ":")
			if intCount, err := strconv.Atoi(splitPart[1]); err == nil {
				config.Unseal = intCount
			}
		} else if strings.Contains(part, "RD:") {
			splitPart := strings.Split(part, ":")
			if intCount, err := strconv.Atoi(splitPart[1]); err == nil {
				config.ReadUnsealed = intCount
			}
		}
	}

	cfgResources := make(map[sealtasks.TaskType]*TaskConfig)

	if _, ok := cfgResources[sealtasks.TTAddPiece]; !ok {
		cfgResources[sealtasks.TTAddPiece] = &TaskConfig{
			LimitCount: config.AddPiece,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTPreCommit1]; !ok {
		cfgResources[sealtasks.TTPreCommit1] = &TaskConfig{
			LimitCount: config.PreCommit1,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTPreCommit2]; !ok {
		cfgResources[sealtasks.TTPreCommit2] = &TaskConfig{
			LimitCount: config.PreCommit2,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTCommit1]; !ok {
		cfgResources[sealtasks.TTCommit1] = &TaskConfig{
			LimitCount: config.Commit1,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTCommit2]; !ok {
		cfgResources[sealtasks.TTCommit2] = &TaskConfig{
			LimitCount: config.Commit2,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTFetch]; !ok {
		cfgResources[sealtasks.TTFetch] = &TaskConfig{
			LimitCount: config.Fetch,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTFinalize]; !ok {
		cfgResources[sealtasks.TTFinalize] = &TaskConfig{
			LimitCount: config.Finalize,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTUnseal]; !ok {
		cfgResources[sealtasks.TTUnseal] = &TaskConfig{
			LimitCount: config.Unseal,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTReadUnsealed]; !ok {
		cfgResources[sealtasks.TTReadUnsealed] = &TaskConfig{
			LimitCount: config.ReadUnsealed,
			RunCount:   0,
		}
	}

	return cfgResources
}
