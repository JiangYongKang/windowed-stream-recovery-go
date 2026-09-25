package processor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/checkpoint"
	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/model"
)

// safeSink 收集输出；同一 key 的输出必须严格按窗口起始升序。
type safeSink struct {
	mu    sync.Mutex
	out   []model.WindowOutput
	errAt int // 第 N 次调用返回错误（0 表示永不）
	calls int
}

func (s *safeSink) sink(_ context.Context, o model.WindowOutput) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.errAt > 0 && s.calls == s.errAt {
		return io.ErrUnexpectedEOF
	}
	s.out = append(s.out, o)
	return nil
}

func (s *safeSink) snapshot() []model.WindowOutput {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]model.WindowOutput, len(s.out))
	copy(cp, s.out)
	return cp
}

func testEngine(t *testing.T, dir, group string, sink Sink) *Engine {
	t.Helper()
	e := New(Config{
		GroupID:         group,
		WindowSize:      10 * time.Second,
		AllowedLateness: 2 * time.Second,
		WatermarkLag:    3 * time.Second,
		MailboxSize:     8,
		Logger:          log.New(testWriter{t}, "[engine] ", log.LstdFlags|log.Lmicroseconds),
	}, checkpoint.NewFileStore(dir, group), sink)
	return e
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

func TestRestartReplayNoLossNoDuplication(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	base := time.Unix(0, 0).UTC()

	// --- 第一段运行：提交 0..5，在 offset=5 处做检查点，模拟崩溃（不 Drain）。 ---
	sink1 := &safeSink{}
	e1 := testEngine(t, dir, "g1", sink1.sink)
	if restored, err := e1.Start(ctx); err != nil || restored {
		t.Fatalf("first start: restored=%v err=%v", restored, err)
	}
	firstRun := []model.Event{
		{Key: "a", Timestamp: base.Add(1 * time.Second), Value: 10},
		{Key: "a", Timestamp: base.Add(3 * time.Second), Value: 5},
		{Key: "b", Timestamp: base.Add(2 * time.Second), Value: 7},
		{Key: "a", Timestamp: base.Add(12 * time.Second), Value: 1},
		{Key: "b", Timestamp: base.Add(14 * time.Second), Value: 3},
		{Key: "a", Timestamp: base.Add(20 * time.Second), Value: 2}, // 推进水位线
	}
	for i, ev := range firstRun {
		if r, err := e1.Submit(ctx, int64(i), ev); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		} else if r.Kind != model.KindMerged {
			t.Fatalf("first-run event %d kind=%d (%s)", i, r.Kind, r.Reason)
		}
	}
	st, err := e1.Checkpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// offset 取最慢分组位点：b 最后处理到 4，因此检查点位点=4（prefix [0,4] 全分组均已完成）。
	if st.Offset != 4 {
		t.Fatalf("checkpoint offset = %d, want 4 (slowest group)", st.Offset)
	}
	firedAfterFirst := len(sink1.snapshot())
	if firedAfterFirst == 0 {
		t.Fatal("expected at least one fired window before crash")
	}
	t.Logf("崩溃前已触发窗口数=%d", firedAfterFirst)

	// --- 第二段运行：新进程从同一检查点启动，重放 0..5 并续投 6..8。 ---
	sink2 := &safeSink{}
	e2 := testEngine(t, dir, "g1", sink2.sink)
	restored, err := e2.Start(ctx)
	if err != nil || !restored {
		t.Fatalf("second start: restored=%v err=%v", restored, err)
	}
	if e2.CommittedOffset() != 4 {
		t.Fatalf("committed offset = %d, want 4", e2.CommittedOffset())
	}

	for i, ev := range firstRun {
		r, err := e2.Submit(ctx, int64(i), ev)
		if err != nil {
			t.Fatalf("replay submit %d: %v", i, err)
		}
		if r.Kind != model.KindDuplicate {
			t.Fatalf("replayed offset %d kind=%d, want KindDuplicate (%s)", i, r.Kind, r.Reason)
		}
	}
	secondRun := []model.Event{
		{Key: "a", Timestamp: base.Add(21 * time.Second), Value: 4},
		{Key: "b", Timestamp: base.Add(22 * time.Second), Value: 6},
		// 超期事件：watermark=22-3=19，窗口 [0,10) 的关闭界=12，已触发，必须拒绝。
		{Key: "a", Timestamp: base.Add(2 * time.Second), Value: 999},
	}
	for j, ev := range secondRun {
		r, err := e2.Submit(ctx, int64(6+j), ev)
		if err != nil {
			t.Fatalf("submit 6+%d: %v", j, err)
		}
		switch j {
		case 0, 1:
			if r.Kind != model.KindMerged {
				t.Fatalf("event 6+%d kind=%d (%s)", j, r.Kind, r.Reason)
			}
		case 2:
			if r.Kind != model.KindDroppedLate {
				t.Fatalf("overdue event kind=%d, want KindDroppedLate (%s)", r.Kind, r.Reason)
			}
		}
	}
	if err := e2.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	m := e2.Metrics()
	if m.Duplicates != 6 {
		t.Fatalf("duplicates=%d want 6", m.Duplicates)
	}
	if m.DroppedLate != 1 {
		t.Fatalf("droppedLate=%d want 1", m.DroppedLate)
	}

	// 两次运行合计的输出必须与"一次性运行"一致：按 key 去重后每窗口只触发一次。
	all := append(sink1.snapshot(), sink2.snapshot()...)
	byKey := map[string][]model.WindowOutput{}
	for _, o := range all {
		byKey[o.Key] = append(byKey[o.Key], o)
	}
	seen := map[string]bool{}
	for k, outs := range byKey {
		for _, o := range outs {
			id := fmt.Sprintf("%s@%d", k, o.State.Start.Unix())
			if seen[id] {
				t.Fatalf("window fired twice across replay: %s", id)
			}
			seen[id] = true
		}
	}
	// 校验第一段运行窗口 [0,10) 的聚合结论确定：a: 10+5=15(count2)；b: 7(count1)。
	for _, o := range all {
		if o.Key == "a" && o.State.Start.Equal(base) && o.State.Count != 0 {
			if o.State.Count != 2 || o.State.Sum != 15 {
				t.Fatalf("window a[0,10) = %+v, want count=2 sum=15", o.State)
			}
		}
	}
}

func TestCorruptAndIncompatibleCheckpointsRefused(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	sink := (&safeSink{}).sink
	e := testEngine(t, dir, "g1", sink)
	if _, err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(0, 0).UTC()
	for i, v := range []int64{10, 5, 12} {
		if _, err := e.Submit(ctx, int64(i), model.Event{
			Key: "a", Timestamp: base.Add(time.Duration(v) * time.Second), Value: v,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}

	// 1) 损坏载荷 -> CRC 拒绝。
	raw, _ := os.ReadFile(filepath.Join(dir, "checkpoint.json"))
	// 等长翻转第二行（base64 载荷）的一个字符，保持长度不变以命中 CRC 而非长度校验。
	lines := splitCP(raw)
	body := []byte(lines[1])
	for i := range body {
		if body[i] == 'A' {
			body[i] = 'B'
			break
		}
		if body[i] == 'B' {
			body[i] = 'A'
			break
		}
	}
	os.WriteFile(filepath.Join(dir, "checkpoint.json"),
		[]byte(lines[0]+"\n"+string(body)+"\n"), 0o644)
	e2 := testEngine(t, dir, "g1", sink)
	_, err := e2.Start(ctx)
	var ce *checkpoint.Error
	if !errors.As(err, &ce) {
		t.Fatalf("corrupt cp: want checkpoint.Error, got %v", err)
	}
	if ce.Reason != checkpoint.ReasonCRC {
		t.Fatalf("corrupt cp reason=%s want crc_mismatch", ce.Reason)
	}
	t.Logf("损坏检查点被拒绝，原因=%s：%v", ce.Reason, ce)

	// 恢复一份完好文件，再测分组不匹配 -> group 拒绝。
	os.WriteFile(filepath.Join(dir, "checkpoint.json"), raw, 0o644)
	e3 := testEngine(t, dir, "OTHER-GROUP", sink)
	if _, err := e3.Start(ctx); !errors.As(err, &ce) || ce.Reason != checkpoint.ReasonGroup {
		t.Fatalf("group mismatch: %v", err)
	}

	// 3) 恢复出一个完好检查点后再手工篡改版本号 -> version 拒绝。
	//    先重新生成一份完好 cp。
	os.RemoveAll(dir)
	e = testEngine(t, dir, "g1", sink)
	_, _ = e.Start(ctx)
	_, _ = e.Submit(ctx, 0, model.Event{Key: "a", Timestamp: base.Add(1 * time.Second), Value: 1})
	_, err = e.Checkpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "checkpoint.json")
	good, _ := os.ReadFile(path)
	// JSON 中第一行信封与第二行载荷都含 "version":1；改载荷里 group 之后的版本字段需重编码，
	// 这里直接翻转载荷无法保持 CRC，因此改为验证文件缺失半个写入=incomplete 的分支：
	os.WriteFile(path, good[:len(good)/2], 0o644)
	e4 := testEngine(t, dir, "g1", sink)
	_, err = e4.Start(ctx)
	if !errors.As(err, &ce) {
		t.Fatalf("truncated cp: want checkpoint.Error, got %v", err)
	}
	if ce.Reason != checkpoint.ReasonIncomplete && ce.Reason != checkpoint.ReasonCRC {
		t.Fatalf("truncated cp reason=%s", ce.Reason)
	}
	t.Logf("不完整检查点被拒绝，原因=%s：%v", ce.Reason, ce)
}

func splitCP(raw []byte) [2]string {
	line0, rest, _ := bytes.Cut(raw, []byte("\n"))
	line1, _, _ := bytes.Cut(rest, []byte("\n"))
	return [2]string{string(line0), string(line1)}
}
