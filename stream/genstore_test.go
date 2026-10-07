package stream

import (
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeRawGen 绕过校验直接写一代存档文件（用于构造版本不符等异常场景）。
func writeRawGen(t *testing.T, gs *GenStore, gen int, c Checkpoint) {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(filePayload{Data: data, Checksum: crc32.ChecksumIEEE(data)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(gs.Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gs.genPath(gen), payload, 0o644); err != nil {
		t.Fatal(err)
	}
}

func countGenFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if _, ok := parseGen(e.Name()); ok {
			n++
		}
	}
	return n
}

// TestGenerationRotation 连续存档超过保留上限后，最旧代际被自动清理，
// 磁盘上的存档份数有明确上界。
func TestGenerationRotation(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenStore(dir, 3, 0, testLogger(t))

	for i := 1; i <= 7; i++ {
		c := Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": int64(i)}, Watermark: int64(i * 100)}
		gen, err := gs.Save(c)
		if err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
		if gen != i {
			t.Fatalf("gen=%d, want %d", gen, i)
		}
	}
	gens, err := gs.List()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("remaining generations: %v", gens)
	if len(gens) != 3 || gens[0] != 5 || gens[2] != 7 {
		t.Errorf("gens=%v, want [5 6 7] (retain=3)", gens)
	}
	if n := countGenFiles(t, dir); n != 3 {
		t.Errorf("files on disk=%d, want 3 (disk usage bounded by retain)", n)
	}
	// 被清理的旧代际确实不可加载。
	if _, err := gs.Load(4); !errors.Is(err, ErrCheckpointNotFound) {
		t.Errorf("load rotated-out gen 4: err=%v, want ErrCheckpointNotFound", err)
	}
}

// TestInterruptedWriteLeavesNoValidGeneration 写入中途被打断只残留 .tmp 文件，
// 不会被当成有效存档，且在下次列举时被清理。
func TestInterruptedWriteLeavesNoValidGeneration(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenStore(dir, 5, 0, testLogger(t))
	if _, err := gs.Save(Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 1}}); err != nil {
		t.Fatal(err)
	}
	// 模拟写到一半被打断：只有临时文件，没有原子改名。
	half := []byte(`{"data":"`)
	if err := os.WriteFile(filepath.Join(dir, "ckpt-000002.json.tmp"), half, 0o644); err != nil {
		t.Fatal(err)
	}
	gens, err := gs.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(gens) != 1 || gens[0] != 1 {
		t.Errorf("gens=%v, want [1] (temp file must not count as a generation)", gens)
	}
	if _, err := os.Stat(filepath.Join(dir, "ckpt-000002.json.tmp")); !os.IsNotExist(err) {
		t.Errorf("stale temp file should have been cleaned up, stat err=%v", err)
	}
	c, report, err := gs.LoadLatest()
	if err != nil {
		t.Fatalf("LoadLatest: %v", err)
	}
	if report.Generation != 1 || c.Offsets["a"] != 1 {
		t.Errorf("report=%s offsets=%v, want gen 1", report, c.Offsets)
	}
}

// TestRecoveryFallback 最新一代不可用（损坏/版本不符/位点过旧）时，
// 恢复必须跳过它退到更旧的可用代际，并如实记录每代的取舍与原因。
func TestRecoveryFallback(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenStore(dir, 10, 0, testLogger(t))

	// gen1、gen2 正常；gen3 内容被篡改；gen4 版本不兼容。
	writeRawGen(t, gs, 1, Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 5}, Watermark: 500})
	writeRawGen(t, gs, 2, Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 10}, Watermark: 1000})
	writeRawGen(t, gs, 3, Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 15}, Watermark: 1500})
	writeRawGen(t, gs, 4, Checkpoint{Version: CheckpointVersion + 1, Offsets: map[string]int64{"a": 20}, Watermark: 2000})

	// 篡改 gen3 的一个字节 → CRC 不符。
	p3 := gs.genPath(3)
	raw, err := os.ReadFile(p3)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(p3, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	c, report, err := gs.LoadLatest()
	if err != nil {
		t.Fatalf("LoadLatest: %v", err)
	}
	t.Logf("report: %s", report)
	if report.Generation != 2 {
		t.Errorf("selected gen=%d, want 2 (gen4 版本不符、gen3 损坏，应回退)", report.Generation)
	}
	if c.Offsets["a"] != 10 {
		t.Errorf("offset[a]=%d, want 10", c.Offsets["a"])
	}
	if len(report.Skipped) != 2 {
		t.Fatalf("skipped=%v, want 2 entries", report.Skipped)
	}
	// 跳过顺序：先最新（gen4，版本不符），再 gen3（损坏）。
	if report.Skipped[0].Generation != 4 || !errors.Is(report.Skipped[0].Reason, ErrVersionMismatch) {
		t.Errorf("skipped[0]=%v, want gen4 with ErrVersionMismatch", report.Skipped[0])
	}
	if report.Skipped[1].Generation != 3 || !errors.Is(report.Skipped[1].Reason, ErrCheckpointCorrupt) {
		t.Errorf("skipped[1]=%v, want gen3 with ErrCheckpointCorrupt", report.Skipped[1])
	}
}

// TestRecoveryFallbackStaleOffset 最新一代位点比当前进度还旧时，
// 不得拿它凑合，应退到位点满足要求的更旧代际。
func TestRecoveryFallbackStaleOffset(t *testing.T) {
	dir := t.TempDir()
	writer := NewGenStore(dir, 10, 0, testLogger(t))
	writeRawGen(t, writer, 1, Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 50}})
	writeRawGen(t, writer, 2, Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 30}})

	// 当前进度要求位点 >= 40：gen2（位点 30）过旧被跳过，回退到 gen1（位点 50）。
	reader := NewGenStore(dir, 10, 40, testLogger(t))
	c, report, err := reader.LoadLatest()
	if err != nil {
		t.Fatalf("LoadLatest: %v", err)
	}
	t.Logf("report: %s", report)
	if report.Generation != 1 || c.Offsets["a"] != 50 {
		t.Errorf("selected gen=%d offset=%d, want gen1 offset=50", report.Generation, c.Offsets["a"])
	}
	if len(report.Skipped) != 1 || !errors.Is(report.Skipped[0].Reason, ErrOffsetRegression) {
		t.Errorf("skipped=%v, want gen2 skipped with ErrOffsetRegression", report.Skipped)
	}
}

// TestRecoveryAllGenerationsUnusable 所有代际都不可用时必须报错，绝不带残缺状态继续。
func TestRecoveryAllGenerationsUnusable(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenStore(dir, 10, 0, testLogger(t))

	// 没有任何存档。
	if _, _, err := gs.LoadLatest(); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("empty store: err=%v, want ErrCheckpointNotFound", err)
	}

	// 唯一的存档损坏。
	writeRawGen(t, gs, 1, Checkpoint{Version: CheckpointVersion, Offsets: map[string]int64{"a": 1}})
	raw, _ := os.ReadFile(gs.genPath(1))
	raw[len(raw)/2] ^= 0xFF
	os.WriteFile(gs.genPath(1), raw, 0o644)

	_, report, err := gs.LoadLatest()
	if err == nil {
		t.Fatal("LoadLatest with all generations corrupt must fail")
	}
	t.Logf("report: %s, err: %v", report, err)
	if report.Generation != 0 || len(report.Skipped) != 1 {
		t.Errorf("report=%s, want no selection and 1 skip", report)
	}
}

// TestOnlineCheckpointWhileRunning 任务持续处理事件的同时按处理量+时间周期存档：
// 已接收事件不丢、同一分组输出顺序不乱、磁盘上代际数不超过保留上限。
func TestOnlineCheckpointWhileRunning(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenStore(dir, 4, 0, testLogger(t))
	e := NewEngine(Config{
		WindowSize:         100,
		AllowedLateness:    100000,
		InputBuffer:        8,
		OutputBuffer:       8,
		Logger:             testLogger(t),
		CheckpointStore:    gs,
		CheckpointEvery:    25,
		CheckpointInterval: 5 * time.Millisecond,
	})
	cancel, results, _, wait := runEngine(t, e)

	const keys = 4
	const perKey = 100
	for k := 0; k < keys; k++ {
		for i := 0; i < perKey; i++ {
			key := string(rune('a' + k))
			submitAll(t, e, []Event{{Key: key, Value: 1, EventTime: int64(i * 100), Seq: int64(i)}})
		}
	}
	// 引擎仍在运行（未取消）时，存档必须已经发生。
	waitFor(t, func() bool { return e.Metrics().Checkpoints.Load() >= 2 }, "online checkpoints while running")
	t.Logf("checkpoints before shutdown: %d", e.Metrics().Checkpoints.Load())
	waitFor(t, func() bool { return e.Metrics().Accepted.Load() == keys*perKey }, "all accepted")
	cancel()
	wait()

	m := e.Metrics().Snapshot()
	t.Logf("metrics: %+v", m)
	if m.Accepted != keys*perKey || m.Late != 0 || m.Duplicates != 0 {
		t.Errorf("accepted=%d late=%d dup=%d, want accepted=%d others 0 (no event lost)",
			m.Accepted, m.Late, m.Duplicates, keys*perKey)
	}
	if m.CheckpointFailures != 0 {
		t.Errorf("checkpoint failures=%d, want 0", m.CheckpointFailures)
	}
	// 同一分组输出窗口 End 单调不减。
	perKeyEnds := map[string][]int64{}
	for _, r := range *results {
		ends := perKeyEnds[r.Key]
		if len(ends) > 0 && r.Window.End < ends[len(ends)-1] {
			t.Errorf("key=%s output order violated: %v then %v", r.Key, ends, r.Window.End)
		}
		perKeyEnds[r.Key] = append(ends, r.Window.End)
	}
	// 保留上限：磁盘上代际数 <= 4。
	gens, err := gs.List()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("generations on disk: %v (checkpoints total=%d)", gens, m.Checkpoints)
	if len(gens) > 4 {
		t.Errorf("generations=%v exceed retain=4", gens)
	}
	if len(gens) == 0 {
		t.Error("no generation on disk after run")
	}
	if n := countGenFiles(t, dir); n != len(gens) {
		t.Errorf("files=%d != listed gens=%d", n, len(gens))
	}
}

// TestCrashRecoveryMatchesUninterrupted 运行中存档 → 模拟崩溃（最近未存档的进度丢失）
// → 从最新可用代恢复 → 全量重放：每个分组的聚合结论与从未停过的参照运行完全一致，不重不漏。
func TestCrashRecoveryMatchesUninterrupted(t *testing.T) {
	dir := t.TempDir()
	gs := NewGenStore(dir, 10, 0, testLogger(t))
	cfg := Config{WindowSize: 1000, AllowedLateness: 500, Logger: testLogger(t)}

	// batch1 处理完恰好触发一次按量存档（gen1 覆盖 batch1）。
	batch1 := []Event{
		{Key: "a", Value: 1, EventTime: 100, Seq: 0},
		{Key: "a", Value: 2, EventTime: 300, Seq: 1},
		{Key: "a", Value: 3, EventTime: 1200, Seq: 2},
		{Key: "a", Value: 4, EventTime: 2000, Seq: 3}, // 水位线 → 1500，触发 [0,1000)
	}
	// batch1b 被 run1 处理但（模拟）崩溃丢失，恢复后靠重放补齐；落在未触发的 [1000,2000)。
	batch1b := []Event{
		{Key: "a", Value: 5, EventTime: 1600, Seq: 4},
		{Key: "a", Value: 6, EventTime: 1700, Seq: 5},
	}
	batch2 := []Event{
		{Key: "a", Value: 7, EventTime: 2600, Seq: 6}, // 水位线 → 2100，触发 [1000,2000)
		{Key: "a", Value: 8, EventTime: 4000, Seq: 7}, // 水位线 → 3500，触发 [2000,3000)
	}

	// run1：开启运行中存档，处理 batch1+batch1b 后"崩溃"。
	cfg1 := cfg
	cfg1.CheckpointStore = gs
	cfg1.CheckpointEvery = 4
	e1 := NewEngine(cfg1)
	cancel1, results1, _, wait1 := runEngine(t, e1)
	submitAll(t, e1, batch1)
	waitFor(t, func() bool { return e1.Metrics().Checkpoints.Load() >= 1 }, "gen1 checkpoint written")
	submitAll(t, e1, batch1b)
	waitFor(t, func() bool { return e1.Metrics().Accepted.Load() == 6 }, "run1 accepted 6")
	cancel1()
	wait1()
	t.Logf("run1 fired %d results, checkpoints=%d", len(*results1), e1.Metrics().Checkpoints.Load())

	// 模拟崩溃：停机时写的最新一代（覆盖 batch1b）丢失，只剩运行中存的 gen1。
	gens, _ := gs.List()
	latest := gens[len(gens)-1]
	if err := os.Remove(gs.genPath(latest)); err != nil {
		t.Fatal(err)
	}
	t.Logf("simulated crash: lost generation %d, remaining=%v", latest, gens[:len(gens)-1])

	// run2：自动从最新可用代恢复，全量重放 + 追加新事件。
	e2 := NewEngine(cfg)
	report, err := e2.RestoreLatest(gs)
	if err != nil {
		t.Fatalf("RestoreLatest: %v", err)
	}
	t.Logf("recovery report: %s", report)
	if report.Generation != 1 {
		t.Fatalf("recovered from gen=%d, want 1", report.Generation)
	}
	cancel2, results2, _, wait2 := runEngine(t, e2)
	all := append(append(append([]Event{}, batch1...), batch1b...), batch2...)
	submitAll(t, e2, all)
	waitFor(t, func() bool {
		m := e2.Metrics().Snapshot()
		return m.Accepted+m.Duplicates+m.Late == int64(len(all))
	}, "run2 accounted all events")
	cancel2()
	wait2()

	m2 := e2.Metrics().Snapshot()
	t.Logf("run2 metrics: %+v", m2)
	if m2.Duplicates != 4 {
		t.Errorf("duplicates=%d, want 4 (batch1 replayed against gen1)", m2.Duplicates)
	}
	if m2.Accepted != 4 {
		t.Errorf("accepted=%d, want 4 (batch1b re-accepted + batch2)", m2.Accepted)
	}

	// 参照运行：同样的全部事件，从未停过。
	ref := NewEngine(cfg)
	cancelR, resultsR, _, waitR := runEngine(t, ref)
	submitAll(t, ref, all)
	waitFor(t, func() bool { return ref.Metrics().Accepted.Load() == int64(len(all)) }, "ref accepted")
	cancelR()
	waitR()

	// 恢复运行的输出（run1+run2）必须与参照运行完全一致：同序、不重、不漏。
	got := append(append([]WindowResult{}, *results1...), *results2...)
	t.Logf("recovered outputs: %d, reference outputs: %d", len(got), len(*resultsR))
	if len(got) != len(*resultsR) {
		t.Fatalf("output count=%d, want %d (reference)", len(got), len(*resultsR))
	}
	seen := map[Window]bool{}
	for i, r := range got {
		if seen[r.Window] {
			t.Errorf("window %s fired twice (duplicate output)", r.Window)
		}
		seen[r.Window] = true
		if r != (*resultsR)[i] {
			t.Errorf("output[%d]=%+v, want %+v (identical to uninterrupted run)", i, r, (*resultsR)[i])
		}
	}
	// 最终聚合结论一致：逐窗口比对。
	want := map[int64]WindowResult{}
	for _, r := range *resultsR {
		want[r.Window.Start] = r
	}
	for _, r := range got {
		w := want[r.Window.Start]
		if r.Count != w.Count || r.Sum != w.Sum {
			t.Errorf("window %s: got count=%d sum=%d, want count=%d sum=%d",
				r.Window, r.Count, r.Sum, w.Count, w.Sum)
		}
	}
}
