// demo 在本地模拟完整流程：乱序输入 → 运行中周期存档（代际管理）→ 模拟崩溃
// （最新一代损坏）→ 自动回退到可用代际恢复 → 重放判重、继续聚合。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/highcumontoa/windowed-stream-recovery-go/stream"
)

func main() {
	dir, err := os.MkdirTemp("", "stream-demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	logger := log.New(os.Stderr, "", log.Ltime)

	// 代际检查点存储：最多保留 3 代，超出自动轮转清理。
	store := stream.NewGenStore(filepath.Join(dir, "checkpoints"), 3, 0, logger)

	cfg := stream.Config{
		WindowSize:      1000,
		AllowedLateness: 500,
		InputBuffer:     16,
		OutputBuffer:    16,
		Logger:          logger,
		// 运行中存档：每确认 4 条事件或每 50ms 落一代，处理不停止。
		CheckpointStore:    store,
		CheckpointEvery:    4,
		CheckpointInterval: 50 * time.Millisecond,
	}

	// 第一阶段：边处理边存档，随后模拟崩溃。
	eng := stream.NewEngine(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()
	go drain(eng.Results(), "run-1")
	go drainRejects(eng.Rejections(), "run-1")

	events := []stream.Event{
		{Key: "a", Value: 1, EventTime: 100, Seq: 0},
		{Key: "b", Value: 10, EventTime: 300, Seq: 1},
		{Key: "a", Value: 2, EventTime: 900, Seq: 2},
		{Key: "a", Value: 3, EventTime: 400, Seq: 3}, // 乱序，但在允许延迟内
		{Key: "a", Value: 4, EventTime: 1600, Seq: 4},
		{Key: "b", Value: 20, EventTime: 1800, Seq: 5},
	}
	for _, ev := range events {
		mustSubmit(eng, ev)
	}
	time.Sleep(150 * time.Millisecond) // 让按时间触发的存档也发生
	cancel()
	<-done
	m := eng.Metrics().Snapshot()
	fmt.Printf("=== run-1 stopped: checkpoints=%d accepted=%d ===\n", m.Checkpoints, m.Accepted)

	// 模拟最新一代存档损坏（如崩溃时正好在写盘）。
	gens, _ := store.List()
	latest := gens[len(gens)-1]
	latestPath := filepath.Join(dir, "checkpoints", fmt.Sprintf("ckpt-%06d.json", latest))
	raw, _ := os.ReadFile(latestPath)
	raw[len(raw)/2] ^= 0xFF
	os.WriteFile(latestPath, raw, 0o644)
	fmt.Printf("=== simulated crash: latest generation %d corrupted ===\n", latest)

	// 第二阶段：自动恢复——跳过损坏的最新一代，回退到更旧的可用代际。
	eng2 := stream.NewEngine(stream.Config{
		WindowSize: 1000, AllowedLateness: 500, InputBuffer: 16, OutputBuffer: 16, Logger: logger,
	})
	report, err := eng2.RestoreLatest(store)
	if err != nil {
		log.Fatalf("restore: %v", err)
	}
	fmt.Printf("=== recovered: %s ===\n", report)

	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { done <- eng2.Run(ctx2) }()
	go drain(eng2.Results(), "run-2")
	go drainRejects(eng2.Rejections(), "run-2")

	// 重放全部历史事件（已确认的应被判重丢弃），再追加新事件。
	replay := append([]stream.Event{}, events...)
	replay = append(replay,
		stream.Event{Key: "a", Value: 5, EventTime: 1700, Seq: 6},
		stream.Event{Key: "b", Value: 30, EventTime: 2600, Seq: 7},
		stream.Event{Key: "a", Value: 99, EventTime: 200, Seq: 8}, // 超期事件，应被拒绝
	)
	for _, ev := range replay {
		mustSubmit(eng2, ev)
	}
	time.Sleep(100 * time.Millisecond)
	cancel2()
	<-done

	m2 := eng2.Metrics().Snapshot()
	fmt.Printf("run-2 metrics: %+v\n", m2)
	fmt.Println("=== demo finished: 运行中存档 + 代际回退恢复，重放无重复计入，超期事件被明确拒绝 ===")
}

func mustSubmit(eng *stream.Engine, ev stream.Event) {
	for {
		if err := eng.Submit(ev); err == stream.ErrBufferFull {
			time.Sleep(time.Millisecond)
			continue
		}
		return
	}
}

func drain(ch <-chan stream.WindowResult, tag string) {
	for r := range ch {
		fmt.Printf("[%s] result key=%s window=%s count=%d sum=%d\n", tag, r.Key, r.Window, r.Count, r.Sum)
	}
}

func drainRejects(ch <-chan stream.Rejection, tag string) {
	for r := range ch {
		fmt.Printf("[%s] rejected seq=%d reason=%s\n", tag, r.Event.Seq, r.Reason)
	}
}
