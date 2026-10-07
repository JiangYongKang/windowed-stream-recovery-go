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

	// 运行中定期存档（不停机检查点）。Checkpoints 为 nil 时禁用。
	// 存档在 Run 的处理循环内生成，与事件处理串行，快照天然一致，
	// 不阻塞事件接收（缓冲未满时），也不改变任何分组的输出顺序。
	Checkpoints        *GenerationStore // 代际存档存储
	CheckpointInterval time.Duration    // 按时间间隔存档，<=0 禁用
	CheckpointEvery    int64            // 每确认 N 条事件存档一次，<=0 禁用
	CheckpointOnStop   bool             // 停止前排空输入后落最终一代存档
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

	sinceCheckpoint int64 // 距上一次存档已确认的事件数

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
// 配置了 Checkpoints 时，运行中按时间间隔/处理量定期落代际存档，
// 停止前（CheckpointOnStop）再落最终一代。
func (e *Engine) Run(ctx context.Context) error {
	defer close(e.out)
	defer close(e.rejects)

	var tick <-chan time.Time
	if e.cfg.Checkpoints != nil && e.cfg.CheckpointInterval > 0 {
		t := time.NewTicker(e.cfg.CheckpointInterval)
		defer t.Stop()
		tick = t.C
	}

	for {
		select {
		case ev := <-e.in:
			e.process(ctx, ev)
			e.maybeCheckpoint()
		case <-tick:
			e.saveCheckpoint("interval")
		case <-ctx.Done():
			// 排空输入缓冲：已提交的事件不得因取消而丢失。
			for {
				select {
				case ev := <-e.in:
					e.process(context.Background(), ev)
					e.maybeCheckpoint()
				default:
					if e.cfg.CheckpointOnStop {
						e.saveCheckpoint("shutdown")
					}
					e.log("engine stopped: %v", ctx.Err())
					return ctx.Err()
				}
			}
		}
	}
}

// maybeCheckpoint 按处理量触发存档（CheckpointEvery > 0 时每确认 N 条事件一次）。
func (e *Engine) maybeCheckpoint() {
	if e.cfg.Checkpoints == nil || e.cfg.CheckpointEvery <= 0 {
		return
	}
	e.sinceCheckpoint++
	if e.sinceCheckpoint >= e.cfg.CheckpointEvery {
		e.saveCheckpoint("count")
	}
}

// saveCheckpoint 在处理循环内生成一致快照并落为新一代存档。
// 写失败只记录指标与日志，不中断处理（旧代际仍可用于恢复）。
func (e *Engine) saveCheckpoint(reason string) {
	if e.cfg.Checkpoints == nil {
		return
	}
	gen, err := e.cfg.Checkpoints.Save(e.Snapshot())
	if err != nil {
		e.metrics.CheckpointErrors.Add(1)
		e.log("checkpoint (%s) failed: %v", reason, err)
		return
	}
	e.sinceCheckpoint = 0
	e.metrics.Checkpoints.Add(1)
	e.log("checkpoint (%s) saved as generation %d: watermark=%d accepted=%d",
		reason, gen, e.wm.Watermark(), e.metrics.Accepted.Load())
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

// RestoreLatest 从代际存档中自动选择最新可用一代恢复：
// 最新一代损坏、版本不兼容或位点回退时逐代回退到更旧的可用存档，
// 不会拿残缺状态凑合继续跑。返回的报告说明最终选用了哪一代、跳过了哪些及各自原因。
func (e *Engine) RestoreLatest(gs *GenerationStore) (RecoveryReport, error) {
	report, err := gs.Recover()
	if err != nil {
		return report, err
	}
	if err := e.Restore(report.Checkpoint); err != nil {
		return report, err
	}
	e.log("restored from generation %d (skipped %d unusable)", report.Generation, len(report.Skipped))
	return report, nil
}

func (e *Engine) log(format string, args ...any) {
	e.cfg.Logger.Printf("[stream] %s", fmt.Sprintf(format, args...))
}
