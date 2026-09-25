package window

import (
	"testing"
	"time"

	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/model"
)

func sec(n int64) time.Time { return time.Unix(n, 0).UTC() }

func TestWindowOf(t *testing.T) {
	s, e := WindowOf(sec(15), 10*time.Second)
	if !s.Equal(sec(10)) || !e.Equal(sec(20)) {
		t.Fatalf("got [%s,%s), want [10,20)", s, e)
	}
}

func TestOutOfOrderMergedWithinLateness(t *testing.T) {
	// 窗口 10s，宽限 2s，水位线滞后 0（由测试直接喂 watermark）。
	a := New("k", 10*time.Second, 2*time.Second)

	if r, ok := a.Add(model.Event{Key: "k", Timestamp: sec(1), Value: 10}, sec(0)); !ok || r.Kind != model.KindMerged {
		t.Fatalf("first event: ok=%v kind=%d", ok, r.Kind)
	}
	// 观察到 12 后，watermark=12：窗口 [0,10) 的关闭界是 12，尚未关闭。
	if r, ok := a.Add(model.Event{Key: "k", Timestamp: sec(12), Value: 1}, sec(12)); !ok {
		t.Fatalf("in-order event rejected: %s", r.Reason)
	}
	// 乱序迟到事件 t=3：watermark=12 == 关闭界，!watermark.Before(12) 为真 -> 已到关闭界。
	// 关闭界语义为 watermark >= end+late，因此 t=3 在 watermark=11 时仍应并入。
	a2 := New("k", 10*time.Second, 2*time.Second)
	_, _ = a2.Add(model.Event{Key: "k", Timestamp: sec(1), Value: 10}, sec(0))
	r, ok := a2.Add(model.Event{Key: "k", Timestamp: sec(3), Value: 5}, sec(11))
	if !ok {
		t.Fatalf("late event within allowed lateness must merge, got: %s", r.Reason)
	}
	if st := a2.windows[sec(0)]; st.Count != 2 || st.Sum != 15 || st.Min != 5 || st.Max != 10 {
		t.Fatalf("unexpected agg: %+v", st)
	}

	// watermark=12 时触发 [0,10)。
	outs := a2.Advance(sec(12))
	if len(outs) != 1 || outs[0].State.Count != 2 || outs[0].State.Sum != 15 {
		t.Fatalf("advance outputs = %+v", outs)
	}
	// 触发后再来的事件必须被显式拒绝（不静默丢弃）。
	r, ok = a2.Add(model.Event{Key: "k", Timestamp: sec(2), Value: 99}, sec(12))
	if ok || r.Kind != model.KindDroppedLate {
		t.Fatalf("post-fire event must be KindDroppedLate, got ok=%v kind=%d (%s)", ok, r.Kind, r.Reason)
	}
	// 拒绝不得改变聚合。
	if st := a2.windows[sec(0)]; st.Count != 2 || st.Sum != 15 {
		t.Fatalf("rejected event mutated state: %+v", st)
	}
}

func TestAdvanceOrderAndIdempotence(t *testing.T) {
	a := New("k", 10*time.Second, 0)
	_, _ = a.Add(model.Event{Key: "k", Timestamp: sec(1), Value: 1}, sec(0))
	_, _ = a.Add(model.Event{Key: "k", Timestamp: sec(11), Value: 2}, sec(11))
	outs := a.Advance(sec(20))
	if len(outs) != 2 {
		t.Fatalf("want 2 fired windows, got %d", len(outs))
	}
	if !outs[0].State.Start.Equal(sec(0)) || !outs[1].State.Start.Equal(sec(10)) {
		t.Fatalf("fired order wrong: %s then %s", outs[0].State.Start, outs[1].State.Start)
	}
	if again := a.Advance(sec(20)); len(again) != 0 {
		t.Fatalf("windows must not fire twice: %+v", again)
	}
}

func TestSnapshotRestorePreservesFiredAndOffset(t *testing.T) {
	a := New("k", 10*time.Second, 1*time.Second)
	_, _ = a.Add(model.Event{Key: "k", Timestamp: sec(12), Value: 7}, sec(12))
	a.SetOffset(5)
	_ = a.Advance(sec(21))
	snap := a.Snapshot()
	if snap.Offset != 5 {
		t.Fatalf("offset = %d", snap.Offset)
	}
	b := Restore("k", 10*time.Second, 1*time.Second, snap)
	r, ok := b.Add(model.Event{Key: "k", Timestamp: sec(11), Value: 1}, sec(21))
	if ok || r.Kind != model.KindDroppedLate {
		t.Fatalf("restored fired window must reject, got ok=%v", ok)
	}
	if b.Offset() != 5 {
		t.Fatalf("restored offset = %d", b.Offset())
	}
}
