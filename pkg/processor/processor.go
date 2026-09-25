// Package processor 实现多分组并发、有界缓冲、检查点与可恢复的窗口聚合引擎。
//
// 并发模型：每个分组（key）一个串行 worker，配合容量为 MailboxSize 的有界邮箱。
//   - 同一分组内：去重 → 水位线观察 → 并入窗口 → 触发输出，全部串行，输出顺序确定；
//   - 不同分组之间并行执行；
//   - 邮箱满时 Submit 阻塞（背压可观测），不会丢弃任何已入队（已确认）事件；
//   - 水位线为全局单调值（取所有事件时间最大值-lag），推进后唤醒各 worker 触发窗口；
//   - 检查点走屏障：屏障排在邮箱中，保证屏障之前的事件全部完成 sink 后才采集状态。
package processor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/checkpoint"
	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/model"
	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/watermark"
	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/window"
)

// Config 控制窗口、水位线与有界缓冲行为。
type Config struct {
	GroupID         string
	WindowSize      time.Duration
	AllowedLateness time.Duration
	WatermarkLag    time.Duration
	MailboxSize     int
	// Logger 用于打印水位线、窗口触发与恢复依据；nil 时使用标准 logger。
	Logger *log.Logger
}

// Metrics 暴露可观测的背压与资源上限指标。
type Metrics struct {
	Submitted          int64
	Merged             int64
	DroppedLate        int64
	Duplicates         int64
	FiredWindows       int64
	SinkErrors         int64
	InFlight           int64
	MailboxPeak        int64
	BackpressureBlocks int64
	Checkpoints        int64
}

// Sink 下游输出回调；同一 key 的输出严格按窗口起始时间升序调用。
type Sink func(ctx context.Context, out model.WindowOutput) error

type job struct {
	offset int64
	ev     model.Event
	ack    chan AckResult
}

type barrier struct {
	reply chan barrierReply
}

type barrierReply struct {
	state window.KeyedState
	err   error
}

// tag 用于 worker 邮箱中区分普通事件与屏障。
type tag struct {
	ev *job
	br *barrier
}

type worker struct {
	key     string
	agg     *window.Aggregator
	mailbox chan tag
	wake    chan struct{} // 容量1，全局水位线推进通知（非阻塞投递）
}

// Engine 是聚合引擎。
type Engine struct {
	cfg    Config
	store  checkpoint.Store
	sink   Sink
	logger *log.Logger

	// mu 保护水位线/生命周期/恢复元信息；不得在"向邮箱发送"的阻塞期间被持有。
	mu sync.Mutex
	// enqMu 专门保护 workers 的增删与邮箱关闭，入队（可能阻塞）只持有它的读锁。
	// 两把锁分开，避免"入队阻塞 + worker 需要写锁"的自死锁。
	enqMu   sync.RWMutex
	workers map[string]*worker
	started bool
	closed  bool
	fatal   error

	wm *watermark.Watermark

	committedOffset int64
	lastSeq         int64

	wg sync.WaitGroup

	// 指标全部以原子方式更新。
	submitted          atomic.Int64
	merged             atomic.Int64
	droppedLate        atomic.Int64
	duplicates         atomic.Int64
	firedWindows       atomic.Int64
	sinkErrors         atomic.Int64
	inFlight           atomic.Int64
	mailboxPeak        atomic.Int64
	backpressureBlocks atomic.Int64
	checkpoints        atomic.Int64
}

// New 创建引擎（尚未启动）。
func New(cfg Config, store checkpoint.Store, sink Sink) *Engine {
	if cfg.MailboxSize <= 0 {
		cfg.MailboxSize = 16
	}
	lg := cfg.Logger
	if lg == nil {
		lg = log.New(os.Stderr, "[engine] ", log.LstdFlags|log.Lmicroseconds)
	}
	return &Engine{
		cfg:             cfg,
		store:           store,
		sink:            sink,
		logger:          lg,
		workers:         make(map[string]*worker),
		wm:              watermark.New(cfg.WatermarkLag),
		committedOffset: -1,
	}
}

// Start 加载并校验检查点、恢复状态。
// 检查点不完整/损坏/版本不兼容/位点回退时返回 *checkpoint.Error，拒绝启动。
func (e *Engine) Start(ctx context.Context) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return false, errors.New("engine already started")
	}

	st, err := e.store.Load(ctx)
	if errors.Is(err, checkpoint.ErrNotFound) {
		// 全新启动。临时文件残留只作诊断，不影响全新状态。
		e.started = true
		e.logger.Printf("全新启动：无可用检查点 group=%q window=%s allowedLateness=%s wmLag=%s",
			e.cfg.GroupID, e.cfg.WindowSize, e.cfg.AllowedLateness, e.cfg.WatermarkLag)
		return false, nil
	}
	if err != nil {
		// 结构性错误（不完整/CRC）直接拒绝，绝不用部分状态继续运行。
		return false, err
	}
	if verr := checkpoint.Validate(st, e.cfg.GroupID, -1, -1); verr != nil {
		return false, verr
	}

	e.committedOffset = st.Offset
	e.lastSeq = st.Seq
	e.wm.Restore(st.Watermark)
	e.enqMu.Lock()
	for i := range st.Keys {
		ks := st.Keys[i]
		w := &worker{
			key:     ks.Key,
			agg:     window.Restore(ks.Key, e.cfg.WindowSize, e.cfg.AllowedLateness, ks),
			mailbox: make(chan tag, e.cfg.MailboxSize),
			wake:    make(chan struct{}, 1),
		}
		e.workers[ks.Key] = w
		e.spawn(w)
	}
	e.enqMu.Unlock()
	e.started = true
	e.logger.Printf("恢复依据：checkpoint seq=%d group=%q offset=%d watermark=%s keys=%d（格式版本 v%d）",
		st.Seq, st.GroupID, st.Offset, wmText(st.Watermark), len(st.Keys), st.Version)
	return true, nil
}

// CommittedOffset 返回已提交位点；无检查点时为 -1。
func (e *Engine) CommittedOffset() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.committedOffset
}

func (e *Engine) Submit(ctx context.Context, offset int64, ev model.Event) (model.AcceptResult, error) {
	ack, err := e.SubmitAsync(ctx, offset, ev)
	if err != nil {
		return model.AcceptResult{}, err
	}
	select {
	case r := <-ack:
		return r.Res, r.Err
	case <-ctx.Done():
		// 事件已入队即视为已确认：不能丢弃，等待 worker 处理完毕。
		r := <-ack
		if r.Err == nil {
			r.Res.Reason += "（调用方已取消，但事件已确认，仍被处理）"
		}
		return r.Res, r.Err
	}
}

// AckResult 是一条事件处理完成的回执。
type AckResult struct {
	Res model.AcceptResult
	Err error
}

// Ack 是一条事件处理完成的回执通道。
type Ack <-chan AckResult

// SubmitAsync 仅在"事件成功入有界邮箱"时返回；入队前的背压等待可被 ctx 取消，
// 此时事件未被确认。入队成功后调用方必须消费返回的 Ack（缓冲为 1，不会阻塞引擎）。
func (e *Engine) SubmitAsync(ctx context.Context, offset int64, ev model.Event) (Ack, error) {
	if err := e.checkAlive(); err != nil {
		return nil, err
	}

	w := e.getOrCreateWorker(ev.Key)

	// 读锁仅保护"向邮箱发送"不与 Drain 关闭邮箱并发；与 worker 端使用的 mu 不交叉。
	e.enqMu.RLock()
	if e.closed {
		e.enqMu.RUnlock()
		return nil, errors.New("engine stopped before enqueue")
	}
	if len(w.mailbox) == cap(w.mailbox) {
		e.backpressureBlocks.Add(1) // 入队前已满：本次投递将经历背压等待
	}

	ack := make(chan AckResult, 1)
	j := job{offset: offset, ev: ev, ack: ack}
	select {
	case w.mailbox <- tag{ev: &j}:
		e.enqMu.RUnlock()
		e.submitted.Add(1)
		e.notePeak(w)
	case <-ctx.Done():
		e.enqMu.RUnlock()
		return nil, fmt.Errorf("submit cancelled before enqueue (key=%s): %w", ev.Key, ctx.Err())
	}
	return ack, nil
}

// Checkpoint 在一致屏障处刷写完整检查点。
// 屏障按邮箱顺序排在各分组所有在途事件之后，确保位点连续、无重复无丢失。
func (e *Engine) Checkpoint(ctx context.Context) (checkpoint.State, error) {
	if err := e.checkAlive(); err != nil {
		return checkpoint.State{}, err
	}

	e.mu.Lock()
	globalWM := e.wm.Current()
	e.mu.Unlock()

	e.enqMu.RLock()
	ws := make([]*worker, 0, len(e.workers))
	for _, w := range e.workers {
		ws = append(ws, w)
	}
	brs := make([]*barrier, len(ws))
	for i, w := range ws {
		br := &barrier{reply: make(chan barrierReply, 1)}
		brs[i] = br
		if e.closed {
			e.enqMu.RUnlock()
			return checkpoint.State{}, errors.New("engine drained during checkpoint")
		}
		select {
		case w.mailbox <- tag{br: br}:
		case <-ctx.Done():
			e.enqMu.RUnlock()
			return checkpoint.State{}, fmt.Errorf("checkpoint barrier enqueue cancelled: %w", ctx.Err())
		}
	}
	e.enqMu.RUnlock()

	states := make([]window.KeyedState, 0, len(ws))
	minOffset := int64(-1)
	for i, br := range brs {
		select {
		case rep := <-br.reply:
			if rep.err != nil {
				return checkpoint.State{}, fmt.Errorf("sink failure before checkpoint (key=%s): %w", ws[i].key, rep.err)
			}
			states = append(states, rep.state)
			if minOffset < 0 || rep.state.Offset < minOffset {
				minOffset = rep.state.Offset
			}
		case <-ctx.Done():
			return checkpoint.State{}, fmt.Errorf("checkpoint barrier cancelled: %w", ctx.Err())
		}
	}

	st := checkpoint.State{
		Version:   checkpoint.Version,
		GroupID:   e.cfg.GroupID,
		Seq:       e.lastSeq + 1,
		Offset:    minOffset, // 最慢分组的位点：保证此前所有输入均已处理
		Watermark: globalWM,
		Keys:      states,
	}
	if err := e.store.Save(ctx, st); err != nil {
		return checkpoint.State{}, err
	}
	e.checkpoints.Add(1)

	e.mu.Lock()
	e.committedOffset = minOffset
	e.lastSeq = st.Seq
	e.mu.Unlock()

	e.logger.Printf("检查点已提交：seq=%d offset=%d watermark=%s keys=%d",
		st.Seq, minOffset, wmText(globalWM), len(states))
	return st, nil
}

// Drain 等待所有已确认事件处理完毕，做最终检查点后停止工作协程。
func (e *Engine) Drain(ctx context.Context) error {
	if err := e.checkAlive(); err != nil {
		return err
	}
	if _, err := e.Checkpoint(ctx); err != nil {
		return err
	}
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.enqMu.Lock()
	workers := make([]*worker, 0, len(e.workers))
	for _, w := range e.workers {
		close(w.mailbox)
		workers = append(workers, w)
	}
	e.enqMu.Unlock()
	e.wg.Wait()
	e.logger.Printf("优雅停止：所有已确认事件已处理，共 %d 个分组协程退出", len(workers))
	return nil
}

// Metrics 返回指标快照。
func (e *Engine) Metrics() Metrics {
	return Metrics{
		Submitted:          e.submitted.Load(),
		Merged:             e.merged.Load(),
		DroppedLate:        e.droppedLate.Load(),
		Duplicates:         e.duplicates.Load(),
		FiredWindows:       e.firedWindows.Load(),
		SinkErrors:         e.sinkErrors.Load(),
		InFlight:           e.inFlight.Load(),
		MailboxPeak:        e.mailboxPeak.Load(),
		BackpressureBlocks: e.backpressureBlocks.Load(),
		Checkpoints:        e.checkpoints.Load(),
	}
}

func (e *Engine) checkAlive() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.started {
		return errors.New("engine not started")
	}
	if e.fatal != nil {
		return fmt.Errorf("engine in fatal state: %w", e.fatal)
	}
	if e.closed {
		return errors.New("engine already drained")
	}
	return nil
}

func (e *Engine) getOrCreateWorker(key string) *worker {
	e.enqMu.Lock()
	defer e.enqMu.Unlock()
	w := e.workers[key]
	if w != nil {
		return w
	}
	w = &worker{
		key:     key,
		agg:     window.New(key, e.cfg.WindowSize, e.cfg.AllowedLateness),
		mailbox: make(chan tag, e.cfg.MailboxSize),
		wake:    make(chan struct{}, 1),
	}
	if !e.closed {
		e.workers[key] = w
		e.spawn(w)
	}
	return w
}

func (e *Engine) spawn(w *worker) {
	e.wg.Add(1)
	go e.run(w)
}

func (e *Engine) run(w *worker) {
	for {
		select {
		case t, ok := <-w.mailbox:
			if !ok {
				e.wg.Done()
				return
			}
			e.handle(w, t)
		case <-w.wake:
			e.flushOutputs(w, e.snapshotWM())
		}
	}
}

func (e *Engine) handle(w *worker, t tag) {
	if t.br != nil {
		// 屏障前先按最新水位线触发，再导出一致快照。
		e.flushOutputs(w, e.snapshotWM())
		t.br.reply <- barrierReply{state: w.agg.Snapshot(), err: e.fatalSnapshot()}
		return
	}
	e.inFlight.Add(1)
	defer e.inFlight.Add(-1)

	j := t.ev
	// 分组级去重：恢复后重放位点 <= 该分组已提交位点即判为重复。
	if j.offset <= w.agg.Offset() {
		e.duplicates.Add(1)
		j.ack <- AckResult{Res: model.AcceptResult{
			Kind:        model.KindDuplicate,
			Key:         j.ev.Key,
			WindowStart: mustWindowStart(j.ev.Timestamp, e.cfg.WindowSize),
			Reason: fmt.Sprintf("重复重放：位点 %d <= 分组 %q 已提交位点 %d，跳过且不改变聚合",
				j.offset, w.key, w.agg.Offset()),
		}}
		return
	}

	// 观察事件时间推进全局水位线，再以新水位线处理本事件并触发窗口。
	newWM := e.observeWM(j.ev.Timestamp)
	res, ok := w.agg.Add(j.ev, newWM)
	w.agg.SetOffset(j.offset)

	if ok {
		e.merged.Add(1)
	} else {
		e.droppedLate.Add(1)
		e.logger.Printf("超期拒绝：key=%s offset=%d %s", w.key, j.offset, res.Reason)
	}
	e.flushOutputs(w, newWM)

	// 水位线可能推进：唤醒其它可能有窗口到期的分组（非阻塞，合并通知）。
	e.broadcastWM(w)
	j.ack <- AckResult{Res: res}
}

func (e *Engine) flushOutputs(w *worker, wm time.Time) {
	if e.fatalSnapshot() != nil {
		return
	}
	outs := w.agg.Advance(wm)
	for i := range outs {
		e.logger.Printf("窗口触发：key=%s window=[%s,%s) count=%d sum=%d min=%d max=%d watermark=%s",
			w.key,
			outs[i].State.Start.Format(time.RFC3339),
			outs[i].State.End.Format(time.RFC3339),
			outs[i].State.Count, outs[i].State.Sum,
			outs[i].State.Min, outs[i].State.Max, wmText(wm))
		if err := e.sink(context.Background(), outs[i]); err != nil {
			e.sinkErrors.Add(1)
			e.markFatal(err)
			return
		}
		e.firedWindows.Add(1)
	}
}

func (e *Engine) observeWM(t time.Time) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	old := e.wm.Current()
	nw := e.wm.Observe(t)
	if nw.After(old) {
		e.logger.Printf("水位线推进：%s -> %s（事件时间 %s）", wmText(old), wmText(nw), t.Format(time.RFC3339))
	}
	return nw
}

func (e *Engine) snapshotWM() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wm.Current()
}

func (e *Engine) broadcastWM(except *worker) {
	e.enqMu.RLock()
	defer e.enqMu.RUnlock()
	for _, w := range e.workers {
		if w == except {
			continue
		}
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}
}

func (e *Engine) markFatal(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fatal == nil {
		e.fatal = err
	}
}

func (e *Engine) fatalSnapshot() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.fatal
}

// notePeak 更新观测到的邮箱深度峰值。
func (e *Engine) notePeak(w *worker) {
	depth := int64(len(w.mailbox))
	for {
		peak := e.mailboxPeak.Load()
		if depth <= peak || e.mailboxPeak.CompareAndSwap(peak, depth) {
			return
		}
	}
}

func mustWindowStart(ts time.Time, size time.Duration) time.Time {
	s, _ := window.WindowOf(ts, size)
	return s
}

func wmText(t time.Time) string {
	if t.IsZero() {
		return "-inf"
	}
	return t.Format(time.RFC3339)
}
