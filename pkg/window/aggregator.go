// Package window 实现按事件时间的滚动时间窗口聚合。
package window

import (
	"fmt"
	"sort"
	"time"

	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/model"
)

// Aggregator 管理单个 key 的全部窗口状态。
//
// 窗口生命周期：
//  1. 首个落入窗口 [start,end) 的事件创建窗口；
//  2. 事件时间 < watermark 但窗口尚未关闭的事件属于"迟到但在宽限期内"，照样并入；
//  3. 当 watermark >= end+allowedLateness 时窗口最终触发（Advance），Fired=true；
//  4. 触发后再来的事件（或 watermark 已越过 end+allowedLateness）被显式拒绝，
//     返回 KindDroppedLate，绝不静默丢弃。
type Aggregator struct {
	key     string
	size    time.Duration
	late    time.Duration
	offset  int64
	windows map[time.Time]*model.WindowState
}

// New 创建一个 key 的窗口聚合器。
// size 为窗口长度，allowedLateness 为水位线之外仍接受迟到事件的宽限期。
func New(key string, size, allowedLateness time.Duration) *Aggregator {
	return &Aggregator{
		key:     key,
		size:    size,
		late:    allowedLateness,
		offset:  -1, // 尚未处理任何事件
		windows: make(map[time.Time]*model.WindowState),
	}
}

// WindowOf 返回事件时间所属滚动窗口的 [start, end)。
func WindowOf(ts time.Time, size time.Duration) (start, end time.Time) {
	if size <= 0 {
		return ts, ts
	}
	start = ts.Truncate(size)
	return start, start.Add(size)
}

// Add 把事件并入它所属的窗口。
// 若窗口已最终触发、或已超过宽限期，返回 ok=false，结论为 KindDroppedLate 并附原因。
func (a *Aggregator) Add(e model.Event, watermark time.Time) (model.AcceptResult, bool) {
	start, end := WindowOf(e.Timestamp, a.size)
	res := model.AcceptResult{Key: a.key, WindowStart: start}

	st := a.windows[start]
	closingTime := end.Add(a.late)
	if st != nil && st.Fired {
		res.Kind = model.KindDroppedLate
		res.Reason = fmt.Sprintf(
			"窗口 [%s,%s) 已触发；事件时间 %s 晚于关闭界 %s（watermark=%s）",
			start.Format(time.RFC3339), end.Format(time.RFC3339),
			e.Timestamp.Format(time.RFC3339),
			closingTime.Format(time.RFC3339),
			wmText(watermark))
		return res, false
	}
	if !watermark.IsZero() && !watermark.Before(closingTime) {
		// watermark >= end+late：即便窗口状态缺失（极旧窗口）也必须拒绝而非新建。
		res.Kind = model.KindDroppedLate
		res.Reason = fmt.Sprintf(
			"事件时间 %s 超出窗口 [%s,%s) 宽限关闭界 %s（watermark=%s）",
			e.Timestamp.Format(time.RFC3339),
			start.Format(time.RFC3339), end.Format(time.RFC3339),
			closingTime.Format(time.RFC3339),
			wmText(watermark))
		return res, false
	}

	if st == nil {
		st = &model.WindowState{Start: start, End: end, Min: e.Value, Max: e.Value}
		a.windows[start] = st
	}
	st.Count++
	st.Sum += e.Value
	if e.Value < st.Min {
		st.Min = e.Value
	}
	if e.Value > st.Max {
		st.Max = e.Value
	}

	res.Kind = model.KindMerged
	switch {
	case !watermark.IsZero() && e.Timestamp.Before(watermark):
		res.Reason = fmt.Sprintf("迟到事件（%s < watermark=%s）在宽限期内并入窗口 [%s,%s)",
			e.Timestamp.Format(time.RFC3339), wmText(watermark),
			start.Format(time.RFC3339), end.Format(time.RFC3339))
	default:
		res.Reason = fmt.Sprintf("事件并入窗口 [%s,%s)",
			start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	return res, true
}

// Advance 在水位线推进后触发应关闭的窗口，返回按窗口起始时间升序的输出。
// 触发条件：!window.Fired && watermark >= window.End + allowedLateness。
// 已触发窗口保留在状态中（Fired=true），用于重放时给出可解释的拒绝结论。
func (a *Aggregator) Advance(watermark time.Time) []model.WindowOutput {
	if watermark.IsZero() {
		return nil
	}
	starts := make([]time.Time, 0, len(a.windows))
	for s := range a.windows {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })

	var out []model.WindowOutput
	for _, s := range starts {
		st := a.windows[s]
		if st.Fired {
			continue
		}
		if !watermark.Before(st.End.Add(a.late)) {
			st.Fired = true
			out = append(out, model.WindowOutput{Key: a.key, State: *st})
		}
	}
	return out
}

// KeyedState 是聚合器的可序列化快照。
// Offset 为该分组（key）已完全处理的最大输入位点，用于恢复后按分组判重。
type KeyedState struct {
	Key     string
	Offset  int64
	Windows []model.WindowState
}

// Offset 返回该分组已完全处理的最大位点；尚无处理记录时为 -1。
func (a *Aggregator) Offset() int64 { return a.offset }

// SetOffset 更新分组已处理位点（单调）。
func (a *Aggregator) SetOffset(n int64) {
	if n > a.offset {
		a.offset = n
	}
}

// Snapshot 导出可持久化状态（按窗口起始时间升序）。
func (a *Aggregator) Snapshot() KeyedState {
	starts := make([]time.Time, 0, len(a.windows))
	for s := range a.windows {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	ws := make([]model.WindowState, 0, len(starts))
	for _, s := range starts {
		ws = append(ws, *a.windows[s])
	}
	return KeyedState{Key: a.key, Offset: a.offset, Windows: ws}
}

// Restore 从检查点状态重建聚合器。
func Restore(key string, size, allowedLateness time.Duration, s KeyedState) *Aggregator {
	a := New(key, size, allowedLateness)
	a.offset = s.Offset
	for i := range s.Windows {
		w := s.Windows[i]
		a.windows[w.Start] = &w
	}
	return a
}

func wmText(t time.Time) string {
	if t.IsZero() {
		return "-inf"
	}
	return t.Format(time.RFC3339)
}
