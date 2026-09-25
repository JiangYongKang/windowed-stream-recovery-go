// demo 在本地模拟一次"乱序输入 → 检查点 → 崩溃重启 → 重放恢复"的完整流程。
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
	ckptPath := filepath.Join(dir, "checkpoint.json")
	logger := log.New(os.Stderr, "", log.Ltime)

	cfg := stream.Config{WindowSize: 1000, AllowedLateness: 500, InputBuffer: 16, OutputBuffer: 16, Logger: logger}

	// 第一阶段：处理部分事件后做检查点，模拟崩溃。
	store := stream.NewFileStore(ckptPath, 0)
	eng := stream.NewEngine(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	go drain(eng.Results(), "run-1")
	go drainRejects(eng.Rejections(), "run-1")

	// 乱序输入：key=a 的事件时间乱序到达，但都在允许延迟内。
	events := []stream.Event{
		{Key: "a", Value: 1, EventTime: 100, Seq: 0},
		{Key: "b", Value: 10, EventTime: 300, Seq: 1},
		{Key: "a", Value: 2, EventTime: 900, Seq: 2},
		{Key: "a", Value: 3, EventTime: 400, Seq: 3}, // 乱序，但在允许延迟内
		{Key: "a", Value: 4, EventTime: 1600, Seq: 4},
	}
	for _, ev := range events {
		mustSubmit(eng, ev)
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	if err := store.Save(eng.Snapshot()); err != nil {
		log.Fatalf("save checkpoint: %v", err)
	}
	fmt.Println("=== checkpoint saved, simulating crash ===")

	// 第二阶段：新进程（新引擎）从检查点恢复，重放全部输入。
	eng2 := stream.NewEngine(cfg)
	ckpt, err := store.Load()
	if err != nil {
		log.Fatalf("load checkpoint: %v", err)
	}
	if err := eng2.Restore(ckpt); err != nil {
		log.Fatalf("restore: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { done <- eng2.Run(ctx2) }()
	go drain(eng2.Results(), "run-2")
	go drainRejects(eng2.Rejections(), "run-2")

	// 重放全部历史事件（Seq 0..4 应被判重丢弃），再追加新事件。
	replay := append([]stream.Event{}, events...)
	replay = append(replay,
		stream.Event{Key: "a", Value: 5, EventTime: 1700, Seq: 5},
		stream.Event{Key: "b", Value: 20, EventTime: 2600, Seq: 6},
		stream.Event{Key: "a", Value: 99, EventTime: 200, Seq: 7}, // 超期事件，应被拒绝
	)
	for _, ev := range replay {
		mustSubmit(eng2, ev)
	}
	time.Sleep(100 * time.Millisecond)
	cancel2()
	<-done

	m := eng2.Metrics().Snapshot()
	fmt.Printf("run-2 metrics: %+v\n", m)
	fmt.Println("=== demo finished: 重放无重复计入，超期事件被明确拒绝 ===")
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
