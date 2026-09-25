package stream

// WatermarkTracker 跟踪事件时间水位线，并判定事件是否超出允许延迟。
// 水位线 = 已观察到的最大事件时间 - 允许延迟，单调不减。
type WatermarkTracker struct {
	allowedLateness int64
	watermark       int64
	maxSeen         int64
	initialized     bool
}

// NewWatermarkTracker 创建水位线跟踪器，allowedLateness 为允许延迟（毫秒）。
func NewWatermarkTracker(allowedLateness int64) *WatermarkTracker {
	return &WatermarkTracker{allowedLateness: allowedLateness}
}

// Observe 观察一条事件，返回更新后的水位线（单调不减）。
func (t *WatermarkTracker) Observe(eventTime int64) int64 {
	if !t.initialized || eventTime > t.maxSeen {
		t.maxSeen = eventTime
		t.initialized = true
	}
	if wm := t.maxSeen - t.allowedLateness; wm > t.watermark {
		t.watermark = wm
	}
	return t.watermark
}

// Watermark 返回当前水位线。
func (t *WatermarkTracker) Watermark() int64 { return t.watermark }

// MaxSeen 返回已观察到的最大事件时间。
func (t *WatermarkTracker) MaxSeen() int64 { return t.maxSeen }

// Restore 从检查点恢复水位线状态。
func (t *WatermarkTracker) Restore(watermark, maxSeen int64) {
	t.watermark = watermark
	t.maxSeen = maxSeen
	t.initialized = true
}

// IsLate 判断事件时间是否已低于水位线（超出允许延迟）。
// 事件时间恰好等于水位线仍视为可接受（窗口右开区间由窗口语义处理）。
func (t *WatermarkTracker) IsLate(eventTime int64) bool {
	return t.initialized && eventTime < t.watermark
}
