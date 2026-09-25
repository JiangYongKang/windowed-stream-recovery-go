// Package stream 提供事件时间窗口聚合与故障恢复能力。
package stream

import (
	"errors"
	"fmt"
	"time"
)

// Event 是一条输入事件，EventTime 为事件时间（毫秒），Seq 为源位点。
type Event struct {
	Key       string
	Value     int64
	EventTime int64 // 事件时间，毫秒
	Seq       int64 // 源位点，单调递增，用于检查点与重放去重
}

// Window 表示一个事件时间窗口 [Start, End)。
type Window struct {
	Start int64
	End   int64
}

func (w Window) String() string {
	return fmt.Sprintf("[%d,%d)", w.Start, w.End)
}

// Contains 判断事件时间是否落在窗口内。
func (w Window) Contains(ts int64) bool { return ts >= w.Start && ts < w.End }

// MarshalText 使 Window 可作为 JSON map key（格式 "start:end"）。
func (w Window) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%d:%d", w.Start, w.End)), nil
}

// UnmarshalText 解析 "start:end" 形式的窗口 key。
func (w *Window) UnmarshalText(b []byte) error {
	var start, end int64
	if _, err := fmt.Sscanf(string(b), "%d:%d", &start, &end); err != nil {
		return fmt.Errorf("window: bad key %q: %w", b, err)
	}
	w.Start, w.End = start, end
	return nil
}

// WindowResult 是窗口触发后的聚合输出。
type WindowResult struct {
	Key    string
	Window Window
	Count  int64
	Sum    int64
}

// RejectReason 区分事件被拒绝的原因。
type RejectReason int

const (
	RejectLateEvent RejectReason = iota // 事件时间已低于水位线（超出允许延迟）
	RejectDuplicate                     // 重放产生的重复位点
	RejectShutdown                      // 引擎已停止
)

func (r RejectReason) String() string {
	switch r {
	case RejectLateEvent:
		return "late-event"
	case RejectDuplicate:
		return "duplicate"
	case RejectShutdown:
		return "shutdown"
	}
	return "unknown"
}

// Rejection 记录一条被拒绝的事件及原因，供观测与断言。
type Rejection struct {
	Event  Event
	Reason RejectReason
}

// 检查点相关错误，按原因可区分。
var (
	ErrCheckpointNotFound = errors.New("checkpoint: not found")
	ErrCheckpointCorrupt  = errors.New("checkpoint: corrupt or incomplete")
	ErrVersionMismatch    = errors.New("checkpoint: incompatible version")
	ErrOffsetRegression   = errors.New("checkpoint: offset regression")
	ErrEngineClosed       = errors.New("stream: engine closed")
	ErrBufferFull         = errors.New("stream: input buffer full")
)

// formatDuration 仅供日志可读性使用。
func formatDuration(ms int64) string { return (time.Duration(ms) * time.Millisecond).String() }
