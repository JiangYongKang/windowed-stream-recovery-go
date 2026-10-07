// demo 在本地模拟"乱序输入 → 运行中定期存档 → 崩溃重启 → 最新一代损坏回退 → 重放恢复"的完整流程。
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

	// 代际存档：运行中每确认 3 条事件落一代，最多保留 3 代，停止前落最终一代。
	store := stream.NewGenerationStore(filepath.Join(dir, "checkpoints"), 3, 0, logger)
	cfg := stream.Config{
		WindowSize: 1000, AllowedLateness: 500, InputBuffer: 16, OutputBuffer: 16, Logger: logger,
		Checkpoints: store, CheckpointEvery: 3, CheckpointOnStop: true,
	}

	// 第一阶段：引擎一边处理一边定期存档，随后模拟崩溃。
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
		{Key: "b", Value: 20, EventTime: 1200, Seq: 5},
	}
	for _, ev := range events {
		mustSubmit(eng, ev)
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done
	fmt.Printf("=== crash: %d generations saved while running ===\n", eng.Metrics().Checkpoints.Load())

	// 模拟最新一代存档损坏（进程在写入中途被杀）。
	gens, err := store.Generations()
	if err != nil || len(gens) == 0 {
		log.Fatalf("list generations: %v", err)
	}
	latest := filepath.Join(dir, "checkpoints",
		fmt.Sprintf("checkpoint-%06d.json", gens[len(gens)-1]))
	raw, err := os.ReadFile(latest)
	if err != nil {
		log.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(latest, raw, 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("=== latest generation %d corrupted on disk ===\n", gens[len(gens)-1])

	// 第二阶段：新进程自动从最新可用代际恢复（损坏的最新代被跳过），重放全部输入。
	eng2 := stream.NewEngine(cfg)
	report, err := eng2.RestoreLatest(store)
	if err != nil {
		log.Fatalf("restore latest: %v", err)
	}
	for _, sk := range report.Skipped {
		fmt.Printf("skipped generation %d: %v\n", sk.Generation, sk.Err)
	}
	fmt.Printf("=== recovered from generation %d ===\n", report.Generation)

	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { done <- eng2.Run(ctx2) }()
	go drain(eng2.Results(), "run-2")
	go drainRejects(eng2.Rejections(), "run-2")

	// 重放全部历史事件（Seq 0..5 应被判重丢弃），再追加新事件。
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

	m := eng2.Metrics().Snapshot()
	fmt.Printf("run-2 metrics: %+v\n", m)
	fmt.Println("=== demo finished: 运行中存档可恢复，损坏代际被回退跳过，重放无重复计入 ===")
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
