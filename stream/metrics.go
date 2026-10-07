package stream

import "sync/atomic"

// Metrics 暴露背压与处理量的可观测指标（均为原子计数，并发安全）。
type Metrics struct {
	Accepted           atomic.Int64 // 已确认并入窗口的事件数
	Late               atomic.Int64 // 因超期被拒绝的事件数
	Duplicates         atomic.Int64 // 重放去重丢弃的事件数
	Fired              atomic.Int64 // 已触发窗口数
	BackpressureStalls atomic.Int64 // 输出通道阻塞等待次数（下游变慢的观测点）
	Checkpoints        atomic.Int64 // 运行中成功落盘的检查点代际数
	CheckpointFailures atomic.Int64 // 运行中存档失败次数
}

// MetricsSnapshot 是指标的一致性快照。
type MetricsSnapshot struct {
	Accepted           int64
	Late               int64
	Duplicates         int64
	Fired              int64
	BackpressureStalls int64
	Checkpoints        int64
	CheckpointFailures int64
}

// Snapshot 读取当前指标。
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Accepted:           m.Accepted.Load(),
		Late:               m.Late.Load(),
		Duplicates:         m.Duplicates.Load(),
		Fired:              m.Fired.Load(),
		BackpressureStalls: m.BackpressureStalls.Load(),
		Checkpoints:        m.Checkpoints.Load(),
		CheckpointFailures: m.CheckpointFailures.Load(),
	}
}
