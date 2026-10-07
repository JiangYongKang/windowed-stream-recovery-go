package stream

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// Config 配置引擎行为。
type Config struct {
	WindowSize      int64 // 窗口长度（毫秒）
	AllowedLateness int64 // 允许延迟（毫秒）
	InputBuffer     int   // 有界输入缓冲（事件条数）
	OutputBuffer    int   // 有界输出缓冲（窗口结果条数）
	Logger          *log.Logger

	// 运行中存档（不停机检查点）。Store 为 nil 时关闭该能力。
	CheckpointStore    *GenStore     // 代际检查点存储
	CheckpointEvery    int64         // 每确认 N 条事件存档一次（0 = 不按处理量）
	CheckpointInterval time.Duration // 每隔该时长存档一次（0 = 不按时间）
}

func (c Config) withDefaults() Config {
	if c.WindowSize <= 0 {
		c.WindowSize = 1000
	}
	if c.InputBuffer <= 0 {
		c.InputBuffer = 64
	}
	if c.OutputBuffer <= 0 {
		c.OutputBuffer = 64
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
	return c
}

// Engine 是单实例流处理引擎：接收事件、维护水位线与窗口、输出结果。
// 所有可变状态仅由 Run 所在 goroutine 访问，外部通过通道交互，无数据竞争。
type Engine struct {
	cfg     Config
	wm      *WatermarkTracker
	store   *windowStore
	metrics *Metrics

	in      chan Event
	out     chan WindowResult
	rejects chan Rejection

	offsets map[string]int64 // 每个分组已确认处理的最大 Seq（重放按分组去重）

	sinceCheckpoint int64 // 距上次存档已确认的事件数

	watermarkPub atomic.Int64 // 对外可观测的水位线镜像
}

// NewEngine 创建引擎。
func NewEngine(cfg Config) *Engine {
	cfg = cfg.withDefaults()
	return &Engine{
		cfg:     cfg,
		wm:      NewWatermarkTracker(cfg.AllowedLateness),
		store:   newWindowStore(cfg.WindowSize),
		metrics: &Metrics{},
		in:      make(chan Event, cfg.InputBuffer),
		out:     make(chan WindowResult, cfg.OutputBuffer),
		rejects: make(chan Rejection, cfg.InputBuffer),
		offsets: make(map[string]int64),
	}
}

// Metrics 返回指标句柄（原子计数，可在引擎运行期间读取）。
func (e *Engine) Metrics() *Metrics { return e.metrics }

// Watermark 返回当前水位线的可观测镜像。
func (e *Engine) Watermark() int64 { return e.watermarkPub.Load() }

// Submit 提交事件；缓冲满时立即返回 ErrBufferFull，使背压对上游可观测。
func (e *Engine) Submit(ev Event) error {
	select {
	case e.in <- ev:
		return nil
	default:
		return ErrBufferFull
	}
}

// Results 返回窗口结果输出通道。
func (e *Engine) Results() <-chan WindowResult { return e.out }

// Rejections 返回被拒绝事件通道（超期、重复重放等原因可区分）。
func (e *Engine) Rejections() <-chan Rejection { return e.rejects }

// Run 启动处理循环。ctx 取消后排空输入缓冲再退出，不丢弃已提交事件。
// 配置了 CheckpointStore 时，运行中按处理量/时间周期落代际检查点：
// 快照与保存都在本 goroutine 内完成，事件处理不停止、输出顺序不受影响。
func (e *Engine) Run(ctx context.Context) error {
	defer close(e.out)
	defer close(e.rejects)

	var tickC <-chan time.Time
	if e.cfg.CheckpointStore != nil && e.cfg.CheckpointInterval > 0 {
		ticker := time.NewTicker(e.cfg.CheckpointInterval)
		tickC = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case ev := <-e.in:
			e.process(ctx, ev)
			e.maybeCheckpoint()
		case <-tickC:
			e.checkpointNow("interval")
		case <-ctx.Done():
			// 排空输入缓冲：已提交的事件不得因取消而丢失。
			for {
				select {
				case ev := <-e.in:
					e.process(context.Background(), ev)
				default:
					e.checkpointNow("shutdown")
					e.log("engine stopped: %v", ctx.Err())
					return ctx.Err()
				}
			}
		}
	}
}

// maybeCheckpoint 按处理量触发存档（仅统计已确认事件，拒绝/判重不计入）。
func (e *Engine) maybeCheckpoint() {
	if e.cfg.CheckpointStore == nil || e.cfg.CheckpointEvery <= 0 {
		return
	}
	e.sinceCheckpoint++
	if e.sinceCheckpoint >= e.cfg.CheckpointEvery {
		e.checkpointNow("count")
	}
}

// checkpointNow 在当前一致性边界（事件处理之间）生成快照并落为新一代存档。
// 仅在 Run 所在 goroutine 内调用，因此快照天然一致，无需停流或加锁。
func (e *Engine) checkpointNow(reason string) {
	gs := e.cfg.CheckpointStore
	if gs == nil {
		return
	}
	gen, err := gs.Save(e.Snapshot())
	if err != nil {
		e.metrics.CheckpointFailures.Add(1)
		e.log("checkpoint failed (reason=%s): %v", reason, err)
		return
	}
	e.sinceCheckpoint = 0
	e.metrics.Checkpoints.Add(1)
	e.log("checkpoint generation %d saved (reason=%s, watermark=%d)", gen, reason, e.wm.Watermark())
}

// process 处理单条事件：去重 → 延迟判定 → 并入窗口 → 推进水位线 → 触发窗口。
func (e *Engine) process(ctx context.Context, ev Event) {
	if off, seen := e.offsets[ev.Key]; seen && ev.Seq <= off {
		e.metrics.Duplicates.Add(1)
		e.reject(ctx, ev, RejectDuplicate)
		return
	}
	if e.wm.IsLate(ev.EventTime) {
		e.metrics.Late.Add(1)
		e.log("reject late event: key=%s eventTime=%d watermark=%d", ev.Key, ev.EventTime, e.wm.Watermark())
		e.reject(ctx, ev, RejectLateEvent)
		return
	}
	e.store.add(ev.Key, ev)
	e.offsets[ev.Key] = ev.Seq
	e.metrics.Accepted.Add(1)

	wm := e.wm.Observe(ev.EventTime)
	e.watermarkPub.Store(wm)
	for _, res := range e.store.fire(wm) {
		e.metrics.Fired.Add(1)
		e.log("window fired: key=%s window=%s count=%d sum=%d watermark=%d",
			res.Key, res.Window, res.Count, res.Sum, wm)
		e.emit(ctx, res)
	}
}

// emit 向下游发送结果；下游变慢时先计入背压指标再阻塞等待（或直到 ctx 取消）。
func (e *Engine) emit(ctx context.Context, res WindowResult) {
	select {
	case e.out <- res:
		return
	default:
	}
	e.metrics.BackpressureStalls.Add(1)
	e.log("backpressure: output buffer full, blocking (key=%s window=%s)", res.Key, res.Window)
	select {
	case e.out <- res:
	case <-ctx.Done():
		// 取消时结果仍保留在窗口状态之后的检查点中不可行——已触发窗口必须送达，
		// 因此这里继续阻塞发送，保证不丢失已确认结果。
		e.out <- res
	}
}

// reject 记录一条被拒绝的事件。
func (e *Engine) reject(ctx context.Context, ev Event, reason RejectReason) {
	select {
	case e.rejects <- Rejection{Event: ev, Reason: reason}:
	case <-ctx.Done():
		e.rejects <- Rejection{Event: ev, Reason: reason}
	}
}

// Snapshot 生成当前状态的检查点。应在引擎停止后调用。
func (e *Engine) Snapshot() Checkpoint {
	offsets := make(map[string]int64, len(e.offsets))
	for k, v := range e.offsets {
		offsets[k] = v
	}
	return Checkpoint{
		Version:   CheckpointVersion,
		Offsets:   offsets,
		Watermark: e.wm.Watermark(),
		MaxSeen:   e.wm.MaxSeen(),
		States:    e.store.snapshot(),
	}
}

// Restore 从检查点恢复；版本不兼容、位点回退将被明确拒绝。
// 应在引擎启动前调用。
func (e *Engine) Restore(c Checkpoint) error {
	if err := c.validate(0); err != nil {
		return err
	}
	// 逐分组检查位点回退：检查点位点低于当前任一分组位点时拒绝。
	for key, off := range e.offsets {
		if c.Offsets[key] < off {
			return fmt.Errorf("%w: key=%s checkpoint offset %d < current %d",
				ErrOffsetRegression, key, c.Offsets[key], off)
		}
	}
	e.offsets = make(map[string]int64, len(c.Offsets))
	for k, v := range c.Offsets {
		e.offsets[k] = v
	}
	e.wm.Restore(c.Watermark, c.MaxSeen)
	e.watermarkPub.Store(c.Watermark)
	e.store.restore(c.States)
	e.log("restored from checkpoint: offsets=%v watermark=%d keys=%d",
		c.Offsets, c.Watermark, len(c.States))
	return nil
}

// RestoreLatest 从代际存储自动恢复：采用最新可用的一代，损坏/版本不符/位点回退的
// 更新代际被跳过（报告中如实记录），退到更旧的可用代际；绝不带残缺状态继续运行。
// 应在引擎启动前调用。
func (e *Engine) RestoreLatest(gs *GenStore) (LoadReport, error) {
	c, report, err := gs.LoadLatest()
	if err != nil {
		e.log("recovery failed: %v (report: %s)", err, report)
		return report, err
	}
	if err := e.Restore(c); err != nil {
		return report, err
	}
	e.log("recovered from checkpoint: %s", report)
	return report, nil
}

func (e *Engine) log(format string, args ...any) {
	e.cfg.Logger.Printf("[stream] %s", fmt.Sprintf(format, args...))
}
