package stream

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testLogger 把引擎日志接入 testing 日志，单测日志中可看到水位线、窗口触发与恢复依据。
func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(testWriter{t: t}, "", 0)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", string(p[:len(p)-1]))
	return len(p), nil
}

func TestAssignWindow(t *testing.T) {
	cases := []struct{ ts, size, start, end int64 }{
		{0, 1000, 0, 1000},
		{999, 1000, 0, 1000},
		{1000, 1000, 1000, 2000},
		{-1, 1000, -1000, 0},
		{-1000, 1000, -1000, 0},
	}
	for _, c := range cases {
		w := AssignWindow(c.ts, c.size)
		if w.Start != c.start || w.End != c.end {
			t.Errorf("AssignWindow(%d,%d)=%v, want [%d,%d)", c.ts, c.size, w, c.start, c.end)
		}
		if !w.Contains(c.ts) {
			t.Errorf("window %v should contain %d", w, c.ts)
		}
	}
}

func TestWatermarkMonotonicAndLate(t *testing.T) {
	wm := NewWatermarkTracker(500)
	if got := wm.Observe(1000); got != 500 {
		t.Fatalf("watermark=%d, want 500", got)
	}
	if got := wm.Observe(300); got != 500 { // 乱序事件不得回退水位线
		t.Fatalf("watermark regressed to %d", got)
	}
	if got := wm.Observe(2000); got != 1500 {
		t.Fatalf("watermark=%d, want 1500", got)
	}
	if !wm.IsLate(400) {
		t.Error("eventTime=400 should be late (watermark=1500)")
	}
	if wm.IsLate(1500) {
		t.Error("eventTime=1500 equals watermark, should not be late")
	}
}

func TestWindowFireDeterministicOrder(t *testing.T) {
	s := newWindowStore(1000)
	s.add("b", Event{Key: "b", Value: 1, EventTime: 100})
	s.add("a", Event{Key: "a", Value: 2, EventTime: 200})
	s.add("a", Event{Key: "a", Value: 3, EventTime: 1200})
	res := s.fire(2000)
	if len(res) != 3 {
		t.Fatalf("fired %d results, want 3", len(res))
	}
	// 按 (End, Key) 排序：两个 [0,1000) 窗口在前，且 a 在 b 前。
	want := []WindowResult{
		{Key: "a", Window: Window{0, 1000}, Count: 1, Sum: 2},
		{Key: "b", Window: Window{0, 1000}, Count: 1, Sum: 1},
		{Key: "a", Window: Window{1000, 2000}, Count: 1, Sum: 3},
	}
	for i, w := range want {
		if res[i] != w {
			t.Errorf("res[%d]=%+v, want %+v", i, res[i], w)
		}
	}
	if got := s.fire(2000); len(got) != 0 {
		t.Errorf("windows should fire exactly once, got %d more", len(got))
	}
}

// runEngine 启动引擎并并发收集输出，返回结果与拒绝事件的收集器。
func runEngine(t *testing.T, e *Engine) (cancel context.CancelFunc, results *[]WindowResult, rejects *[]Rejection, wait func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	results = &[]WindowResult{}
	rejects = &[]Rejection{}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); e.Run(ctx) }() //nolint:errcheck
	go func() {
		defer wg.Done()
		for r := range e.Results() {
			mu.Lock()
			*results = append(*results, r)
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for r := range e.Rejections() {
			mu.Lock()
			*rejects = append(*rejects, r)
			mu.Unlock()
		}
	}()
	wait = func() { wg.Wait() }
	return cancel, results, rejects, wait
}

func submitAll(t *testing.T, e *Engine, events []Event) {
	t.Helper()
	for _, ev := range events {
		for {
			err := e.Submit(ev)
			if errors.Is(err, ErrBufferFull) {
				time.Sleep(time.Millisecond)
				continue
			}
			if err != nil {
				t.Fatalf("submit: %v", err)
			}
			break
		}
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for: %s", msg)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOutOfOrderWithinLateness(t *testing.T) {
	e := NewEngine(Config{WindowSize: 1000, AllowedLateness: 500, Logger: testLogger(t)})
	cancel, results, _, wait := runEngine(t, e)

	submitAll(t, e, []Event{
		{Key: "a", Value: 1, EventTime: 100, Seq: 0},
		{Key: "a", Value: 2, EventTime: 900, Seq: 1},
		{Key: "a", Value: 3, EventTime: 400, Seq: 2}, // 乱序但在允许延迟内，必须并入 [0,1000)
		{Key: "a", Value: 4, EventTime: 1500, Seq: 3},
		{Key: "a", Value: 0, EventTime: 2600, Seq: 4}, // 推进水位线至 2100，触发前两个窗口
	})
	waitFor(t, func() bool { return e.Metrics().Accepted.Load() == 5 }, "5 accepted")
	t.Logf("watermark after input: %d", e.Watermark())
	cancel()
	wait()

	var sum0, sum1 int64
	for _, r := range *results {
		t.Logf("fired: key=%s window=%s count=%d sum=%d", r.Key, r.Window, r.Count, r.Sum)
		switch r.Window.Start {
		case 0:
			sum0 = r.Sum
			if r.Count != 3 {
				t.Errorf("window [0,1000) count=%d, want 3 (out-of-order event must be merged)", r.Count)
			}
		case 1000:
			sum1 = r.Sum
		}
	}
	if sum0 != 6 || sum1 != 4 {
		t.Errorf("sums: [0,1000)=%d want 6, [1000,2000)=%d want 4", sum0, sum1)
	}
}

func TestLateEventRejected(t *testing.T) {
	e := NewEngine(Config{WindowSize: 1000, AllowedLateness: 500, Logger: testLogger(t)})
	cancel, _, rejects, wait := runEngine(t, e)

	submitAll(t, e, []Event{
		{Key: "a", Value: 1, EventTime: 5000, Seq: 0}, // 水位线推进到 4500
		{Key: "a", Value: 2, EventTime: 1000, Seq: 1}, // 超期
	})
	waitFor(t, func() bool { return e.Metrics().Late.Load() == 1 }, "1 late")
	cancel()
	wait()

	if len(*rejects) != 1 || (*rejects)[0].Reason != RejectLateEvent {
		t.Fatalf("rejects=%+v, want exactly one late-event rejection", *rejects)
	}
	if (*rejects)[0].Event.Seq != 1 {
		t.Errorf("rejected seq=%d, want 1", (*rejects)[0].Event.Seq)
	}
}

func TestCheckpointRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	fs := NewFileStore(path, 0)
	orig := Checkpoint{
		Version: CheckpointVersion, Offsets: map[string]int64{"a": 7}, Watermark: 1500, MaxSeen: 2000,
		States: map[string]map[Window]windowState{
			"a": {{Start: 0, End: 1000}: {Count: 2, Sum: 5}},
		},
	}
	if err := fs.Save(orig); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := fs.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Offsets["a"] != 7 || got.Watermark != 1500 || got.MaxSeen != 2000 {
		t.Errorf("loaded %+v, want offset[a]=7 watermark=1500 maxSeen=2000", got)
	}
	st := got.States["a"][Window{0, 1000}]
	if st.Count != 2 || st.Sum != 5 {
		t.Errorf("state=%+v, want count=2 sum=5", st)
	}
}

func TestCheckpointNotFound(t *testing.T) {
	fs := NewFileStore(filepath.Join(t.TempDir(), "missing.json"), 0)
	_, err := fs.Load()
	if !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("err=%v, want ErrCheckpointNotFound", err)
	}
}

func TestCheckpointCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	fs := NewFileStore(path, 0)
	good := Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 1}}
	if err := fs.Save(good); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)

	// 篡改内容 → CRC 不符。
	corrupt := append([]byte{}, raw...)
	corrupt[len(corrupt)/2] ^= 0xFF
	os.WriteFile(path, corrupt, 0o644)
	if _, err := fs.Load(); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Errorf("tampered: err=%v, want ErrCheckpointCorrupt", err)
	}

	// 截断（半截写入）→ 不完整。
	os.WriteFile(path, raw[:len(raw)/2], 0o644)
	if _, err := fs.Load(); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Errorf("truncated: err=%v, want ErrCheckpointCorrupt", err)
	}
}

func TestCheckpointVersionMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	fs := NewFileStore(path, 0)
	bad := Checkpoint{Version: CheckpointVersion + 1, Offsets: map[string]int64{"a": 1}}
	if err := fs.Save(bad); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("save with future version: err=%v, want ErrVersionMismatch", err)
	}
	// 构造一个版本合法但引擎版本不同的场景：直接校验。
	c := Checkpoint{Version: 99, Offsets: map[string]int64{"a": 0}}
	if err := c.validate(0); !errors.Is(err, ErrVersionMismatch) {
		t.Errorf("validate: err=%v, want ErrVersionMismatch", err)
	}
}

func TestCheckpointOffsetRegression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	fs := NewFileStore(path, 10) // 最小允许位点为 10
	if err := fs.Save(Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 5}}); !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("save: err=%v, want ErrOffsetRegression", err)
	}
	c := Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 3}}
	if err := c.validate(10); !errors.Is(err, ErrOffsetRegression) {
		t.Errorf("validate: err=%v, want ErrOffsetRegression", err)
	}
}

// TestRestartReplay 模拟：运行 → 检查点 → 崩溃 → 新引擎恢复 → 全量重放。
// 重放事件不得重复计入，新事件必须基于恢复的状态继续聚合。
func TestRestartReplay(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(filepath.Join(dir, "ckpt.json"), 0)
	cfg := Config{WindowSize: 1000, AllowedLateness: 500, Logger: testLogger(t)}

	batch1 := []Event{
		{Key: "a", Value: 1, EventTime: 100, Seq: 0},
		{Key: "a", Value: 2, EventTime: 900, Seq: 1},
		{Key: "a", Value: 3, EventTime: 400, Seq: 2}, // 乱序，允许延迟内
		{Key: "a", Value: 4, EventTime: 1200, Seq: 3},
	}

	// 第一次运行。
	e1 := NewEngine(cfg)
	cancel1, results1, _, wait1 := runEngine(t, e1)
	submitAll(t, e1, batch1)
	waitFor(t, func() bool { return e1.Metrics().Accepted.Load() == 4 }, "run1 accepted")
	cancel1()
	wait1()
	if err := fs.Save(e1.Snapshot()); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}
	t.Logf("run1 fired %d results", len(*results1))

	// 模拟重启：新引擎从检查点恢复。
	ckpt, err := fs.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	e2 := NewEngine(cfg)
	if err := e2.Restore(ckpt); err != nil {
		t.Fatalf("restore: %v", err)
	}
	cancel2, results2, rejects2, wait2 := runEngine(t, e2)

	// 全量重放 batch1（应全部判重），再追加新事件。
	batch2 := append([]Event{}, batch1...)
	batch2 = append(batch2,
		Event{Key: "a", Value: 5, EventTime: 1300, Seq: 4},
		Event{Key: "a", Value: 6, EventTime: 2500, Seq: 5},
	)
	submitAll(t, e2, batch2)
	waitFor(t, func() bool {
		m := e2.Metrics().Snapshot()
		return m.Accepted == 2 && m.Duplicates == 4
	}, "run2 accepted=2 duplicates=4")
	cancel2()
	wait2()

	m := e2.Metrics().Snapshot()
	t.Logf("run2 metrics: %+v", m)
	if m.Duplicates != 4 {
		t.Errorf("duplicates=%d, want 4 (replayed events must be deduplicated)", m.Duplicates)
	}
	for _, r := range *rejects2 {
		if r.Reason != RejectDuplicate {
			t.Errorf("unexpected rejection: %+v", r)
		}
	}

	// 汇总两次运行的输出，验证最终聚合结论：重放未造成重复或丢失。
	total := map[int64]WindowResult{}
	for _, r := range append(*results1, *results2...) {
		key := r.Window.Start
		if prev, ok := total[key]; ok {
			t.Fatalf("window %s fired twice (duplicate output): %+v and %+v", r.Window, prev, r)
		}
		total[key] = r
	}
	if w := total[0]; w.Count != 3 || w.Sum != 6 {
		t.Errorf("window [0,1000): %+v, want count=3 sum=6", w)
	}
	if w := total[1000]; w.Count != 2 || w.Sum != 9 {
		t.Errorf("window [1000,2000): %+v, want count=2 sum=9 (events 1200+1300)", w)
	}
}

func TestRestoreRejectsRegression(t *testing.T) {
	e := NewEngine(Config{Logger: testLogger(t)})
	good := Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 10}}
	if err := e.Restore(good); err != nil {
		t.Fatalf("first restore: %v", err)
	}
	stale := Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 5}}
	if err := e.Restore(stale); !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("restore stale: err=%v, want ErrOffsetRegression", err)
	}
	badVersion := Checkpoint{Version: 99, Offsets: map[string]int64{"a": 20}}
	if err := e.Restore(badVersion); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("restore bad version: err=%v, want ErrVersionMismatch", err)
	}
}

func TestBackpressureObservable(t *testing.T) {
	// 输出缓冲为 1 且下游缓慢，背压必须体现在指标上。
	e := NewEngine(Config{WindowSize: 100, AllowedLateness: 0, OutputBuffer: 1, Logger: testLogger(t)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.Run(ctx) //nolint:errcheck

	// 缓慢消费输出。
	go func() {
		for range e.Results() {
			time.Sleep(2 * time.Millisecond)
		}
	}()
	go func() {
		for range e.Rejections() {
		}
	}()

	// 每条事件推进水位线并触发一个窗口，制造大量输出。
	for i := 0; i < 50; i++ {
		ev := Event{Key: fmt.Sprintf("k%d", i%4), Value: 1, EventTime: int64(i * 100), Seq: int64(i)}
		for err := e.Submit(ev); errors.Is(err, ErrBufferFull); err = e.Submit(ev) {
			time.Sleep(time.Millisecond)
		}
	}
	waitFor(t, func() bool { return e.Metrics().Accepted.Load() == 50 }, "50 accepted")
	cancel()
	waitFor(t, func() bool { return e.Metrics().Snapshot().Fired >= 40 }, "windows fired")
	if got := e.Metrics().Snapshot().BackpressureStalls; got == 0 {
		t.Error("BackpressureStalls=0, want >0 when downstream is slow")
	} else {
		t.Logf("backpressure stalls observed: %d", got)
	}
}

func TestSubmitBufferFull(t *testing.T) {
	// 不启动 Run，输入缓冲填满后 Submit 必须返回 ErrBufferFull。
	e := NewEngine(Config{InputBuffer: 2, Logger: testLogger(t)})
	for i := 0; i < 2; i++ {
		if err := e.Submit(Event{Seq: int64(i)}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	if err := e.Submit(Event{Seq: 2}); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("err=%v, want ErrBufferFull", err)
	}
}

// TestConcurrentSubmitters 多分组并发提交 + 取消，在 -race 下运行；
// 验证已确认事件不丢失、同一分组输出窗口顺序单调。
func TestConcurrentSubmitters(t *testing.T) {
	e := NewEngine(Config{WindowSize: 100, AllowedLateness: 100000, InputBuffer: 8, OutputBuffer: 8, Logger: testLogger(t)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.Run(ctx); close(done) }() //nolint:errcheck

	var mu sync.Mutex
	perKey := map[string][]int64{} // 每个 key 已输出窗口的 End 序列
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for r := range e.Results() {
			mu.Lock()
			perKey[r.Key] = append(perKey[r.Key], r.Window.End)
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for range e.Rejections() {
		}
	}()

	const submitters = 8
	const perSubmitter = 200
	var sent sync.WaitGroup
	sent.Add(submitters)
	for g := 0; g < submitters; g++ {
		go func(g int) {
			defer sent.Done()
			key := fmt.Sprintf("key-%d", g) // 每个提交者独占分组，分组内位点单调
			for i := 0; i < perSubmitter; i++ {
				ev := Event{Key: key, Value: 1, EventTime: int64(i * 100), Seq: int64(i)}
				for {
					err := e.Submit(ev)
					if errors.Is(err, ErrBufferFull) {
						time.Sleep(time.Millisecond)
						continue
					}
					break
				}
			}
		}(g)
	}
	sent.Wait()
	waitFor(t, func() bool {
		m := e.Metrics().Snapshot()
		return m.Accepted+m.Duplicates+m.Late == submitters*perSubmitter
	}, "all submitted events accounted for")
	cancel()
	<-done
	wg.Wait()

	m := e.Metrics().Snapshot()
	t.Logf("metrics: %+v", m)
	total := int64(submitters * perSubmitter)
	if m.Accepted+m.Late+m.Duplicates != total {
		t.Errorf("accepted+late+duplicates=%d, want %d (no event may be lost silently)",
			m.Accepted+m.Late+m.Duplicates, total)
	}
	if m.Duplicates != 0 {
		t.Errorf("duplicates=%d, want 0 (per-key monotonic seqs must not be misjudged)", m.Duplicates)
	}
	// 同一分组的输出窗口 End 必须单调不减（输出顺序不乱）。
	mu.Lock()
	for key, ends := range perKey {
		for i := 1; i < len(ends); i++ {
			if ends[i] < ends[i-1] {
				t.Errorf("key=%s output order violated: %v", key, ends)
				break
			}
		}
	}
	mu.Unlock()
}
