// Command demo 用本地输入流与检查点目录演示：乱序事件、超期事件、
// 检查点重启重放、损坏检查点拒绝等场景。
//
// 用法：
//
//	go run ./cmd/demo -dir ./ckpt-demo -phase 1   # 第一段运行：处理事件并写检查点
//	go run ./cmd/demo -dir ./ckpt-demo -phase 2   # 第二段运行：从检查点重放后续跑
//	go run ./cmd/demo -dir ./ckpt-demo -phase corrupt   # 破坏检查点后启动，观察被拒绝
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/checkpoint"
	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/model"
	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/processor"
)

func main() {
	dir := flag.String("dir", "./ckpt-demo", "检查点目录")
	phase := flag.String("phase", "1", "运行阶段：1=首段处理并检查点；2=重放+续跑；corrupt=破坏后启动")
	flag.Parse()

	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmicroseconds)
	ctx := context.Background()

	var mu sync.Mutex
	sink := func(_ context.Context, out model.WindowOutput) error {
		mu.Lock()
		defer mu.Unlock()
		fmt.Printf(">>> 下游收到最终结果 key=%s 窗口[%s,%s) count=%d sum=%d min=%d max=%d\n",
			out.Key,
			out.State.Start.Format("15:04:05"), out.State.End.Format("15:04:05"),
			out.State.Count, out.State.Sum, out.State.Min, out.State.Max)
		return nil
	}

	cfg := processor.Config{
		GroupID:         "orders-demo",
		WindowSize:      10 * time.Second,
		AllowedLateness: 2 * time.Second,
		WatermarkLag:    1 * time.Second,
		MailboxSize:     4,
		Logger:          logger,
	}
	store := checkpoint.NewFileStore(*dir, cfg.GroupID)

	switch *phase {
	case "1":
		runPhase1(ctx, cfg, store, sink)
	case "2":
		runPhase2(ctx, cfg, store, sink)
	case "corrupt":
		runCorrupt(ctx, cfg, *dir, store, sink)
	default:
		log.Fatalf("未知 phase=%q", *phase)
	}
}

// 事件流：offset 单调；同一窗口故意乱序，并在末尾推进水位线。
var phase1Events = []model.Event{
	{Key: "user-a", Timestamp: time.Unix(1, 0).UTC(), Value: 10},
	{Key: "user-a", Timestamp: time.Unix(3, 0).UTC(), Value: 5}, // 乱序但仍在所属窗口
	{Key: "user-b", Timestamp: time.Unix(2, 0).UTC(), Value: 7},
	{Key: "user-a", Timestamp: time.Unix(12, 0).UTC(), Value: 2},
	{Key: "user-b", Timestamp: time.Unix(15, 0).UTC(), Value: 3},
	{Key: "user-a", Timestamp: time.Unix(20, 0).UTC(), Value: 1}, // 推进水位线，触发早期窗口
}

var phase2Events = []model.Event{
	{Key: "user-a", Timestamp: time.Unix(21, 0).UTC(), Value: 4},
	{Key: "user-b", Timestamp: time.Unix(22, 0).UTC(), Value: 6},
	{Key: "user-a", Timestamp: time.Unix(2, 0).UTC(), Value: 999}, // 严重超期：所属窗口早已触发
}

func runPhase1(ctx context.Context, cfg processor.Config, store checkpoint.Store, sink processor.Sink) {
	e := processor.New(cfg, store, sink)
	restored, err := e.Start(ctx)
	must(err)
	fmt.Printf("启动：restored=%v\n", restored)

	for i, ev := range phase1Events {
		r, err := e.Submit(ctx, int64(i), ev)
		must(err)
		fmt.Printf("投递 offset=%d key=%s t=%-2ds value=%-3d => 结论=%s | %s\n",
			i, ev.Key, ev.Timestamp.Unix(), ev.Value, kindName(r.Kind), r.Reason)
	}

	st, err := e.Checkpoint(ctx)
	must(err)
	fmt.Printf("=== 已写检查点 seq=%d offset=%d watermark=%s ===\n",
		st.Seq, st.Offset, st.Watermark.Format("15:04:05"))

	// 模拟进程崩溃：直接退出，不做优雅 Drain（检查点已是持久化的恢复依据）。
	fmt.Println("=== 模拟进程崩溃退出（未 Drain）===")
	reportMetrics(e)
}

func runPhase2(ctx context.Context, cfg processor.Config, store checkpoint.Store, sink processor.Sink) {
	e := processor.New(cfg, store, sink)
	restored, err := e.Start(ctx)
	must(err)
	if !restored {
		log.Fatal("phase 2 应从检查点恢复，但未找到可用检查点")
	}
	fmt.Printf("启动：restored=true，已提交位点=%d\n", e.CommittedOffset())

	// 重放 phase1 的全部事件：应当全部判定为重复，不改变任何聚合。
	fmt.Println("--- 重放第一段事件（崩溃前未确认/外部 source 重新投递的部分）---")
	for i, ev := range phase1Events {
		r, err := e.Submit(ctx, int64(i), ev)
		must(err)
		fmt.Printf("重放 offset=%d key=%s => 结论=%s | %s\n", i, ev.Key, kindName(r.Kind), r.Reason)
	}

	fmt.Println("--- 投递新事件（含一条超期事件）---")
	for j, ev := range phase2Events {
		off := int64(len(phase1Events) + j)
		r, err := e.Submit(ctx, off, ev)
		must(err)
		fmt.Printf("投递 offset=%d key=%s t=%-2ds value=%-3d => 结论=%s | %s\n",
			off, ev.Key, ev.Timestamp.Unix(), ev.Value, kindName(r.Kind), r.Reason)
	}

	must(e.Drain(ctx))
	reportMetrics(e)
}

func runCorrupt(ctx context.Context, cfg processor.Config, dir string, store checkpoint.Store, sink processor.Sink) {
	// 先确保存在一份检查点，再对载荷做等长破坏。
	if _, err := os.Stat(filepath.Join(dir, "checkpoint.json")); os.IsNotExist(err) {
		runPhase1(ctx, cfg, store, sink)
	}
	path := filepath.Join(dir, "checkpoint.json")
	raw, err := os.ReadFile(path)
	must(err)
	if len(raw) > 20 {
		raw[len(raw)-4] = flipByte(raw[len(raw)-4]) // 破坏 base64 载荷尾部
		must(os.WriteFile(path, raw, 0o644))
	}
	fmt.Println("--- 已破坏检查点，尝试以同 group 启动（必须被拒绝）---")
	e := processor.New(cfg, store, sink)
	if _, err := e.Start(ctx); err != nil {
		var ce *checkpoint.Error
		fmt.Printf("启动被明确拒绝：%v\n", err)
		if asCE(err, &ce) {
			fmt.Printf("拒绝原因代码=%s（可据此区分处理：不允许用部分状态继续运行）\n", ce.Reason)
		}
		return
	}
	log.Fatal("损坏的检查点居然启动成功，这是不允许的")
}

func asCE(err error, target **checkpoint.Error) bool {
	for err != nil {
		if ce, ok := err.(*checkpoint.Error); ok {
			*target = ce
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func flipByte(b byte) byte {
	if b == 'A' {
		return 'B'
	}
	return 'A'
}

func kindName(k model.AcceptKind) string {
	switch k {
	case model.KindMerged:
		return "并入窗口"
	case model.KindDroppedLate:
		return "超期拒绝(不静默丢弃)"
	case model.KindDuplicate:
		return "重放重复(忽略)"
	default:
		return "未知"
	}
}

func reportMetrics(e *processor.Engine) {
	m := e.Metrics()
	fmt.Printf("指标：submitted=%d merged=%d droppedLate=%d duplicates=%d fired=%d"+
		" mailboxPeak=%d backpressureBlocks=%d checkpoints=%d inFlight=%d\n",
		m.Submitted, m.Merged, m.DroppedLate, m.Duplicates, m.FiredWindows,
		m.MailboxPeak, m.BackpressureBlocks, m.Checkpoints, m.InFlight)
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
