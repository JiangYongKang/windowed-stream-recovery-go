package stream

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestPeriodicCheckpointWhileRunning 验证运行中持续存档：
// 引擎一边处理事件一边按处理量+时间间隔落代际存档，
// 已接收事件不丢失、同一分组输出顺序不乱、存档代际受保留上限约束。
func TestPeriodicCheckpointWhileRunning(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenerationStore(dir, 2, 0, testLogger(t))
	e := NewEngine(Config{
		WindowSize:         100,
		AllowedLateness:    10000,
		Checkpoints:        gs,
		CheckpointEvery:    10, // 每确认 10 条事件落一代
		CheckpointInterval: 5 * time.Millisecond,
		CheckpointOnStop:   true,
		Logger:             testLogger(t),
	})
	cancel, results, _, wait := runEngine(t, e)

	const keys = 4
	const perKey = 100
	var sent sync.WaitGroup
	for g := 0; g < keys; g++ {
		sent.Add(1)
		go func(g int) {
			defer sent.Done()
			key := fmt.Sprintf("key-%d", g)
			for i := 0; i < perKey; i++ {
				ev := Event{Key: key, Value: 1, EventTime: int64(i * 100), Seq: int64(i)}
				for err := e.Submit(ev); errors.Is(err, ErrBufferFull); err = e.Submit(ev) {
					time.Sleep(time.Millisecond)
				}
			}
		}(g)
	}
	sent.Wait()
	waitFor(t, func() bool { return e.Metrics().Accepted.Load() == keys*perKey }, "all accepted")
	cancel()
	wait()

	m := e.Metrics().Snapshot()
	t.Logf("metrics: %+v", m)
	if m.Accepted != keys*perKey {
		t.Errorf("accepted=%d, want %d (no event may be lost while checkpointing)", m.Accepted, keys*perKey)
	}
	if m.CheckpointErrors != 0 {
		t.Errorf("checkpoint errors=%d, want 0", m.CheckpointErrors)
	}
	// 至少按处理量落了 keys*perKey/10 代，另有间隔与停止时的存档。
	if want := int64(keys * perKey / 10); m.Checkpoints < want {
		t.Errorf("checkpoints=%d, want >= %d", m.Checkpoints, want)
	}
	// 保留上限生效：目录中最多 2 代。
	gens, err := gs.Generations()
	if err != nil {
		t.Fatalf("generations: %v", err)
	}
	t.Logf("surviving generations: %v (total saved=%d)", gens, m.Checkpoints)
	if len(gens) == 0 || len(gens) > 2 {
		t.Errorf("surviving generations=%v, want 1..2 (retain=2)", gens)
	}
	// 同一分组输出窗口 End 单调不减。
	perKeyEnds := map[string][]int64{}
	for _, r := range *results {
		ends := perKeyEnds[r.Key]
		if len(ends) > 0 && r.Window.End < ends[len(ends)-1] {
			t.Fatalf("key=%s output order violated: %v then %v", r.Key, ends, r.Window.End)
		}
		perKeyEnds[r.Key] = append(ends, r.Window.End)
	}
}

// TestGenerationRotation 验证代际轮转清理：超过保留上限的旧存档被自动删除，
// 磁盘上只保留最近 Retain 代，且留下的都是最新的。
func TestGenerationRotation(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenerationStore(dir, 3, 0, testLogger(t))

	for i := 1; i <= 7; i++ {
		c := Checkpoint{
			Version:   CheckpointVersion,
			Offsets:   map[string]int64{"a": int64(i)},
			Watermark: int64(i * 100),
		}
		gen, err := gs.Save(c)
		if err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
		if gen != int64(i) {
			t.Errorf("generation=%d, want %d", gen, i)
		}
	}

	gens, err := gs.Generations()
	if err != nil {
		t.Fatalf("generations: %v", err)
	}
	t.Logf("surviving generations: %v", gens)
	want := []int64{5, 6, 7}
	if fmt.Sprint(gens) != fmt.Sprint(want) {
		t.Errorf("generations=%v, want %v (only the newest Retain=3 survive)", gens, want)
	}
	// 磁盘上不得残留旧代际文件或临时文件。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Logf("on disk: %s", e.Name())
	}
	if len(entries) != 3 {
		t.Errorf("files on disk=%d, want 3 (disk usage bounded by Retain)", len(entries))
	}
	// 最新一代内容可读且正确。
	report, err := gs.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Generation != 7 || report.Checkpoint.Offsets["a"] != 7 {
		t.Errorf("recovered generation=%d offsets=%v, want 7 / a:7",
			report.Generation, report.Checkpoint.Offsets)
	}
}

// writeRawGeneration 绕过校验直接落一代存档（用于构造损坏/不兼容场景）。
func writeRawGeneration(t *testing.T, gs *GenerationStore, id int64, c Checkpoint) {
	t.Helper()
	payload, err := encodeCheckpoint(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(gs.Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(gs.generationPath(id), payload); err != nil {
		t.Fatal(err)
	}
}

// TestRecoverFallback 验证最新一代不可用时的回退恢复：
// 损坏（CRC 不符）、版本不兼容的最新代际被逐代跳过并记录原因，
// 最终选中更旧的可用代际；目录中的 .tmp 残留与无关文件不影响恢复。
func TestRecoverFallback(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenerationStore(dir, 5, 0, testLogger(t))

	// 第 1、2 代：正常存档。
	for i := int64(1); i <= 2; i++ {
		if _, err := gs.Save(Checkpoint{
			Version: CheckpointVersion, Offsets: map[string]int64{"a": i * 10}, Watermark: i * 100,
		}); err != nil {
			t.Fatalf("save gen %d: %v", i, err)
		}
	}
	// 第 3 代：版本不兼容（格式对不上）。
	writeRawGeneration(t, gs, 3, Checkpoint{Version: CheckpointVersion + 1, Offsets: map[string]int64{"a": 30}})
	// 第 4 代：先正常写入再篡改内容（CRC 不符）。
	writeRawGeneration(t, gs, 4, Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 40}})
	raw, err := os.ReadFile(gs.generationPath(4))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(gs.generationPath(4), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	// 干扰项：未完成的临时文件与无关文件必须被忽略。
	if err := os.WriteFile(filepath.Join(dir, "checkpoint-000009.json.tmp"), []byte("{partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignore me"), 0o644); err != nil {
		t.Fatal(err)
	}

	report, err := gs.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Generation != 2 {
		t.Errorf("selected generation=%d, want 2 (newest usable)", report.Generation)
	}
	if report.Checkpoint.Offsets["a"] != 20 {
		t.Errorf("offsets=%v, want a:20", report.Checkpoint.Offsets)
	}
	if len(report.Skipped) != 2 {
		t.Fatalf("skipped=%d, want 2", len(report.Skipped))
	}
	for _, sk := range report.Skipped {
		t.Logf("skipped generation %d: %v", sk.Generation, sk.Err)
	}
	// 跳过顺序从最新到最旧：4（损坏）→ 3（版本不符）。
	if report.Skipped[0].Generation != 4 || !errors.Is(report.Skipped[0].Err, ErrCheckpointCorrupt) {
		t.Errorf("skipped[0]=%+v, want generation 4 with ErrCheckpointCorrupt", report.Skipped[0])
	}
	if report.Skipped[1].Generation != 3 || !errors.Is(report.Skipped[1].Err, ErrVersionMismatch) {
		t.Errorf("skipped[1]=%+v, want generation 3 with ErrVersionMismatch", report.Skipped[1])
	}
}

// TestRecoverFallbackOffsetRegression 验证位点回退的最新一代被跳过；
// 所有代际都不可用时返回 ErrCheckpointNotFound，且报告中保留全部跳过原因。
func TestRecoverFallbackOffsetRegression(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenerationStore(dir, 5, 100, testLogger(t)) // 最小允许位点 100

	// 两代位点都低于下限（模拟位点比当前进度还旧的存档）。
	writeRawGeneration(t, gs, 1, Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 50}})
	writeRawGeneration(t, gs, 2, Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 60}})

	report, err := gs.Recover()
	if !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("err=%v, want ErrCheckpointNotFound", err)
	}
	if report.Generation != -1 {
		t.Errorf("generation=%d, want -1 (nothing usable)", report.Generation)
	}
	if len(report.Skipped) != 2 {
		t.Fatalf("skipped=%d, want 2", len(report.Skipped))
	}
	for _, sk := range report.Skipped {
		t.Logf("skipped generation %d: %v", sk.Generation, sk.Err)
		if !errors.Is(sk.Err, ErrOffsetRegression) {
			t.Errorf("generation %d skipped with %v, want ErrOffsetRegression", sk.Generation, sk.Err)
		}
	}
}

// resultKey 唯一标识一个窗口输出。
type resultKey struct {
	key    string
	window Window
}

func collectByWindow(results []WindowResult) map[resultKey]WindowResult {
	m := make(map[resultKey]WindowResult, len(results))
	for _, r := range results {
		m[resultKey{r.Key, r.Window}] = r
	}
	return m
}

// TestRecoveryExactlyOnce 验证恢复后聚合不重不漏：
// 参考运行（不间断、不存档）与故障运行（运行中存档 → 停止 → 最新一代恢复 → 全量重放）
// 的每个分组每个窗口的聚合结论必须完全一致，且任何窗口不会被输出两次。
func TestRecoveryExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	base := Config{WindowSize: 1000, AllowedLateness: 500, Logger: testLogger(t)}

	// 构造事件流：两个分组，组内位点单调，含乱序事件，末尾用大时间戳事件推满水位线。
	var events []Event
	seqs := map[string]int64{}
	push := func(key string, value, eventTime int64) {
		events = append(events, Event{Key: key, Value: value, EventTime: eventTime, Seq: seqs[key]})
		seqs[key]++
	}
	push("a", 1, 100)
	push("b", 2, 300)
	push("a", 100, 50)  // 乱序：落在 [0,1000)，此时水位线尚低，允许延迟内
	push("b", 200, 250) // 乱序：落在 [0,1000)
	for i := int64(1); i < 12; i++ {
		push("a", i+1, i*1000+100)
		push("b", 2*(i+1), i*1000+300)
	}
	push("a", 0, 100000) // 推满水位线，冲刷所有窗口

	// 参考运行：不间断、不存档。
	ref := NewEngine(base)
	cancelRef, refResults, _, waitRef := runEngine(t, ref)
	submitAll(t, ref, events)
	waitFor(t, func() bool { return ref.Metrics().Accepted.Load() == int64(len(events)) }, "ref accepted")
	cancelRef()
	waitRef()
	want := collectByWindow(*refResults)
	t.Logf("reference run fired %d windows", len(want))

	// 故障运行：运行中按处理量存档，处理一半后停止（停止时落最终一代）。
	gs := NewGenerationStore(dir, 3, 0, testLogger(t))
	cfgCk := base
	cfgCk.Checkpoints = gs
	cfgCk.CheckpointEvery = 5
	cfgCk.CheckpointOnStop = true

	half := len(events) / 2
	e1 := NewEngine(cfgCk)
	cancel1, results1, _, wait1 := runEngine(t, e1)
	submitAll(t, e1, events[:half])
	waitFor(t, func() bool { return e1.Metrics().Accepted.Load() == int64(half) }, "run1 accepted")
	cancel1()
	wait1()
	t.Logf("run1: accepted=%d checkpoints=%d fired=%d",
		half, e1.Metrics().Checkpoints.Load(), len(*results1))

	// 重启：自动从最新一代可用存档恢复。
	e2 := NewEngine(cfgCk)
	report, err := e2.RestoreLatest(gs)
	if err != nil {
		t.Fatalf("restore latest: %v", err)
	}
	t.Logf("recovered from generation %d (skipped %d)", report.Generation, len(report.Skipped))
	if report.Generation <= 0 {
		t.Fatalf("expected a usable generation, got %d", report.Generation)
	}
	cancel2, results2, rejects2, wait2 := runEngine(t, e2)

	// 源从位点 0 全量重放前半段（必须全部判重），再追加后半段新事件。
	submitAll(t, e2, events[:half])
	submitAll(t, e2, events[half:])
	waitFor(t, func() bool {
		m := e2.Metrics().Snapshot()
		return m.Accepted == int64(len(events)-half) && m.Duplicates == int64(half)
	}, "run2 accepted+duplicates")
	cancel2()
	wait2()

	m2 := e2.Metrics().Snapshot()
	t.Logf("run2 metrics: %+v", m2)
	if m2.Duplicates != int64(half) {
		t.Errorf("duplicates=%d, want %d (replayed prefix must be deduplicated)", m2.Duplicates, half)
	}
	for _, r := range *rejects2 {
		if r.Reason != RejectDuplicate {
			t.Errorf("unexpected rejection: %+v", r)
		}
	}

	// 汇总两次运行的输出：任何窗口不得输出两次（不重）。
	got := make(map[resultKey]WindowResult, len(*results1)+len(*results2))
	for _, r := range append(*results1, *results2...) {
		k := resultKey{r.Key, r.Window}
		if prev, ok := got[k]; ok {
			t.Fatalf("window %s/%s fired twice: %+v and %+v", k.key, k.window, prev, r)
		}
		got[k] = r
	}
	// 与参考运行逐窗口对比：不缺、不多、数值一致（不漏、结论相同）。
	if len(got) != len(want) {
		t.Errorf("fired windows=%d, want %d (same as uninterrupted run)", len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Errorf("missing window %s/%s after recovery", k.key, k.window)
			continue
		}
		if g.Count != w.Count || g.Sum != w.Sum {
			t.Errorf("window %s/%s: got count=%d sum=%d, want count=%d sum=%d",
				k.key, k.window, g.Count, g.Sum, w.Count, w.Sum)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected window %s/%s after recovery", k.key, k.window)
		}
	}
}
