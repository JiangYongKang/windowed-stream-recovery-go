// Package watermark 实现基于事件时间观察值的水位线推进。
package watermark

import "time"

// Watermark 跟踪已观察到的最大事件时间，并按固定滞后量推导水位线。
// 水位线含义：事件时间 t <= watermark 的输入在语义上视为"已到齐"
// （窗口是否最终关闭还受到 allowedLateness 宽限期约束）。
type Watermark struct {
	maxEventTime time.Time
	lag          time.Duration
}

// New 创建水位线跟踪器。lag 为水位线相对最大事件时间的滞后量。
func New(lag time.Duration) *Watermark {
	if lag < 0 {
		lag = 0
	}
	return &Watermark{lag: lag}
}

// Observe 纳入一个事件时间；观察值与水位线均单调不减。
// 返回推进后的水位线。
func (w *Watermark) Observe(t time.Time) time.Time {
	if t.After(w.maxEventTime) {
		w.maxEventTime = t
	}
	return w.Current()
}

// Current 返回当前水位线；尚无观察值时返回零值（语义为 -∞）。
func (w *Watermark) Current() time.Time {
	if w.maxEventTime.IsZero() {
		return time.Time{}
	}
	return w.maxEventTime.Add(-w.lag)
}

// Snapshot 返回可持久化的最大事件时间。
func (w *Watermark) Snapshot() time.Time { return w.maxEventTime }

// Restore 从检查点恢复最大事件时间。
func (w *Watermark) Restore(maxEventTime time.Time) { w.maxEventTime = maxEventTime }
