// Package model 定义事件、处理结果与窗口状态的基础类型。
package model

import "time"

// Event 是一条输入事件，时间戳为事件时间（event time）。
type Event struct {
	Key       string
	Timestamp time.Time
	Value     int64
}

// AcceptKind 区分事件被接收后的处理结论。
type AcceptKind int

const (
	// KindMerged 事件被并入所属窗口（含乱序并入尚未触发的窗口）。
	KindMerged AcceptKind = iota
	// KindDroppedLate 事件晚于水位线+允许延迟，未被并入（显式拒绝，而非静默丢弃）。
	KindDroppedLate
	// KindDuplicate 事件在已提交检查点覆盖的位点范围内，判定为重放重复。
	KindDuplicate
)

// AcceptResult 描述一次投递事件的确定结论。
type AcceptResult struct {
	Kind AcceptKind
	Key  string
	// WindowStart 所属窗口起始时间（被拒绝时同样给出，便于解释）。
	WindowStart time.Time
	// Reason 给出人类可读的判定依据。
	Reason string
}

// WindowState 是单个窗口的可序列化聚合状态。
type WindowState struct {
	Start time.Time
	End   time.Time
	Count int64
	Sum   int64
	Min   int64
	Max   int64
	// Fired 标记窗口是否已产出最终结果。
	Fired bool
}

// WindowOutput 是窗口触发时向下游发出的最终聚合结果。
type WindowOutput struct {
	Key   string
	State WindowState
}
