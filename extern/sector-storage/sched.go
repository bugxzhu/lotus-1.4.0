package sectorstorage

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
	FindDataWoker(ctx context.Context, task sealtasks.TaskType, sid abi.SectorID, spt abi.RegisteredSealProof, a *workerHandle) bool
}

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
	workerOnFree   chan struct{}
	todo           []*workerRequest
}

type schedWindowRequest struct {
	worker WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	todo          []*workerRequest
	activeWindows []*schedWindow
	wid           WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

type workerRequest struct {
	sector   storage.SectorRef
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler() *scheduler {
	return &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (sh *scheduler) runSched() {
	defer close(sh.closed)
	go func() {
		for {
			time.Sleep(time.Second * 180) // 3分钟执行一次
			sh.workersLk.Lock()
			if sh.schedQueue.Len() > 0 {
				sh.windowRequests <- &schedWindowRequest{}
			}
			sh.workersLk.Unlock()
		}
	}()

	for {
		select {
		case <-sh.workerChange:
			sh.trySched()
		case toDisable := <-sh.workerDisable:
			sh.workersLk.Lock()
			sh.workers[toDisable.wid].enabled = false
			sh.workersLk.Unlock()
			toDisable.done()
		case req := <-sh.schedule:
			sh.schedQueue.Push(req)
			sh.trySched()

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case <-sh.windowRequests:
			sh.trySched()
		case ireq := <-sh.info:
			ireq(sh.diag())
		case <-sh.closing:
			sh.schedClose()
			return
		}
	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

func (sh *scheduler) trySched() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	log.Debugf("trySched %d queued", sh.schedQueue.Len())
	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ { // 遍历任务列表
		task := (*sh.schedQueue)[sqi]

		tried := 0
		var acceptable []WorkerID
		var freetable []int
		best := 0
		localWorker := false
		for wid, worker := range sh.workers {
			if !worker.enabled {
				continue
			}

			if task.taskType == sealtasks.TTPreCommit1 || task.taskType == sealtasks.TTPreCommit2 || task.taskType == sealtasks.TTCommit1 {
				if isExist := task.sel.FindDataWoker(task.ctx, task.taskType, task.sector.ID, task.sector.ProofType, worker); !isExist {
					continue
				}
			}

			ok, err := task.sel.Ok(task.ctx, task.taskType, task.sector.ProofType, worker)
			if err != nil || !ok {
				continue
			}

			freecount := sh.getTaskFreeCount(wid, task.taskType)
			if freecount <= 0 {
				continue
			}
			tried++
			freetable = append(freetable, freecount)
			acceptable = append(acceptable, wid)

			if isExist := task.sel.FindDataWoker(task.ctx, task.taskType, task.sector.ID, task.sector.ProofType, worker); isExist {
				localWorker = true
				break
			}
		}

		if len(acceptable) > 0 {
			if localWorker {
				best = len(acceptable) - 1
			} else {
				max := 0
				for i, v := range freetable {
					if v > max {
						max = v
						best = i
					}
				}
			}

			wid := acceptable[best]
			whl := sh.workers[wid]
			log.Infof("worker %s will be do the %+v jobTask!", whl.info.Hostname, task.taskType)
			sh.schedQueue.Remove(sqi)
			sqi--
			if err := sh.assignWorker(wid, whl, task); err != nil {
				log.Error("assignWorker error: %+v", err)
				go task.respond(xerrors.Errorf("assignWorker error: %w", err))
			}
		}

		if tried == 0 {
			log.Infof("no worker do the %+v jobTask!", task.taskType)
		}
	}
}

func (sh *scheduler) assignWorker(wid WorkerID, w *workerHandle, req *workerRequest) error {
	sh.taskAddOne(wid, req.taskType)
	needRes := ResourceTable[req.taskType][req.sector.ProofType]

	w.lk.Lock()
	w.preparing.add(w.info.Resources, needRes)
	w.lk.Unlock()

	go func() {
		err := req.prepare(req.ctx, sh.workTracker.worker(wid, w.workerRpc)) // fetch扇区
		sh.workersLk.Lock()

		if err != nil {
			sh.taskReduceOne(wid, req.taskType)
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()

			select {
			case w.workerOnFree <- struct{}{}:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		err = w.active.withResources(wid, w.info.Resources, needRes, &sh.workersLk, func() error {
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()
			defer sh.workersLk.Lock() // we MUST return locked from this function

			select {
			case w.workerOnFree <- struct{}{}:
			case <-sh.closing:
			}

			err = req.work(req.ctx, sh.workTracker.worker(wid, w.workerRpc))
			sh.taskReduceOne(wid, req.taskType)

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			return nil
		})

		sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (sh *scheduler) taskAddOne(wid WorkerID, phaseTaskType sealtasks.TaskType) {
	if whl, ok := sh.workers[wid]; ok {
		whl.info.TaskResourcesLk.Lock()
		defer whl.info.TaskResourcesLk.Unlock()
		if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
			counts.RunCount++
		}
	}
}

func (sh *scheduler) taskReduceOne(wid WorkerID, phaseTaskType sealtasks.TaskType) {
	if whl, ok := sh.workers[wid]; ok {
		whl.info.TaskResourcesLk.Lock()
		defer whl.info.TaskResourcesLk.Unlock()
		if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
			counts.RunCount--
		}
	}
}

func (sh *scheduler) getTaskCount(wid WorkerID, phaseTaskType sealtasks.TaskType, typeCount string) int {
	if whl, ok := sh.workers[wid]; ok {
		if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
			whl.info.TaskResourcesLk.Lock()
			defer whl.info.TaskResourcesLk.Unlock()
			if typeCount == "limit" {
				return counts.LimitCount
			}
			if typeCount == "run" {
				return counts.RunCount
			}
		}
	}
	return 0
}

func (sh *scheduler) getTaskFreeCount(wid WorkerID, phaseTaskType sealtasks.TaskType) int {
	limitCount := sh.getTaskCount(wid, phaseTaskType, "limit") // json文件限制的任务数量
	runCount := sh.getTaskCount(wid, phaseTaskType, "run")     // 运行中的任务数量
	freeCount := limitCount - runCount

	if limitCount == 0 { // 0:禁止
		return 0
	}

	whl := sh.workers[wid]
	log.Infof("worker %s %s: %d free count", whl.info.Hostname, phaseTaskType, freeCount)

	if phaseTaskType == sealtasks.TTAddPiece || phaseTaskType == sealtasks.TTPreCommit1 {
		if freeCount >= 0 { // 空闲数量不小于0，小于0也要校准为0
			return freeCount
		}
		return 0
	}

	if phaseTaskType == sealtasks.TTPreCommit2 || phaseTaskType == sealtasks.TTCommit1 {
		c2runCount := sh.getTaskCount(wid, sealtasks.TTCommit2, "run")
		if freeCount >= 0 && c2runCount <= 0 { // 需做的任务空闲数量不小于0，且没有c2任务在运行
			return freeCount
		}
		log.Infof("worker already doing C2 taskjob")
		return 0
	}

	if phaseTaskType == sealtasks.TTCommit2 {
		p2runCount := sh.getTaskCount(wid, sealtasks.TTPreCommit2, "run")
		c1runCount := sh.getTaskCount(wid, sealtasks.TTCommit1, "run")
		if freeCount >= 0 && p2runCount <= 0 && c1runCount <= 0 { // 需做的任务空闲数量不小于0，且没有p2\c1任务在运行
			return freeCount
		}
		log.Infof("worker already doing P2C1 taskjob")
		return 0
	}

	if phaseTaskType == sealtasks.TTFetch || phaseTaskType == sealtasks.TTFinalize ||
		phaseTaskType == sealtasks.TTUnseal || phaseTaskType == sealtasks.TTReadUnsealed { // 不限制
		return 1
	}

	return 0
}
