package watermark

import (
	"testing"
	"time"
)

func TestMonotonicWatermark(t *testing.T) {
	w := New(3 * time.Second)
	if !w.Current().IsZero() {
		t.Fatalf("empty watermark must be zero(-inf), got %s", w.Current())
	}
	if got := w.Observe(time.Unix(10, 0).UTC()); !got.Equal(time.Unix(7, 0).UTC()) {
		t.Fatalf("wm = %s, want 7", got)
	}
	// 观察值回退不允许拉低水位线。
	if got := w.Observe(time.Unix(2, 0).UTC()); !got.Equal(time.Unix(7, 0).UTC()) {
		t.Fatalf("wm regressed to %s", got)
	}
	if got := w.Observe(time.Unix(20, 0).UTC()); !got.Equal(time.Unix(17, 0).UTC()) {
		t.Fatalf("wm = %s, want 17", got)
	}
	// 快照与恢复。
	snap := w.Snapshot()
	w2 := New(3 * time.Second)
	w2.Restore(snap)
	if !w2.Current().Equal(time.Unix(17, 0).UTC()) {
		t.Fatalf("restored wm = %s", w2.Current())
	}
}
