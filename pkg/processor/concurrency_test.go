package processor

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/checkpoint"
	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/model"
)

// orderedSink 验证同一 key 的窗口输出严格按窗口起始时间升序。
// gate 非 nil 时，每次输出都阻塞到 gate 关闭（背压/取消测试用，外部统一释放）。
type orderedSink struct {
	mu         sync.Mutex
	last       map[string]time.Time
	out        []model.WindowOutput
	gate       <-chan struct{}
	beforeSink func(model.WindowOutput)
}

func (s *orderedSink) sink(_ context.Context, o model.WindowOutput) error {
	if s.beforeSink != nil {
		s.beforeSink(o)
	}
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.last[o.Key]; ok && o.State.Start.Before(prev) {
		return io.ErrUnexpectedEOF
	}
	s.last[o.Key] = o.State.Start
	s.out = append(s.out, o)
	return nil
}

func TestConcurrentMultiKeyOrderAndBackpressure(t *testing.T) {
	dir := t.TempDir()
	gate := make(chan struct{})
	s := &orderedSink{last: map[string]time.Time{}, gate: gate}
	e := New(Config{
		GroupID:         "gC",
		WindowSize:      time.Second,
		AllowedLateness: 0,
		WatermarkLag:    0,
		MailboxSize:     2, // 极小邮箱 + 阻塞 sink，强制产生背压
		Logger:          log.New(testWriter{t}, "[engine] ", log.LstdFlags|log.Lmicroseconds),
	}, checkpoint.NewFileStore(dir, "gC"), s.sink)
	ctx := context.Background()
	if _, err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}

	const keys = 8
	const perKey = 60

	// 第一阶段：sink 被 gate 阻塞期间，异步入队大量事件。
	// 每个分组 worker 处理到首个窗口触发后卡在 sink，其余事件堆积在有界邮箱，必然产生背压。
	acksCh := make(chan []Ack, 1)
	go func() {
		var acks []Ack
		for k := 0; k < keys; k++ {
			key := string(rune('a' + k))
			for i := 0; i < perKey; i++ {
				ack, err := e.SubmitAsync(ctx, int64(k*1000+i), model.Event{
					Key: key, Timestamp: time.Unix(int64(i*2), 0).UTC(), Value: int64(i),
				})
				if err != nil {
					t.Errorf("submit key=%s i=%d: %v", key, i, err)
					acksCh <- acks
					return
				}
				acks = append(acks, ack)
			}
		}
		acksCh <- acks
	}()

	// 等待背压确实出现后放开 sink，避免投递协程永久阻塞。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.Metrics().BackpressureBlocks == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if e.Metrics().BackpressureBlocks == 0 {
		close(gate)
		t.Fatalf("阻塞 sink + 小邮箱下应观测到背压，metrics=%+v", e.Metrics())
	}
	close(gate)
	acks := <-acksCh

	// 第二阶段：补发每个 key 的远未来事件，强制触发此前所有窗口。
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := string(rune('a' + k))
			if _, err := e.Submit(ctx, int64(2000+k), model.Event{
				Key: key, Timestamp: time.Unix(10_000, 0).UTC(), Value: 1,
			}); err != nil {
				t.Errorf("flush submit key=%s: %v", key, err)
			}
		}(k)
	}
	// 消费所有回执，确认异步入队事件均被处理。
	for i, ack := range acks {
		r := <-ack
		if r.Err != nil {
			t.Errorf("ack %d: %v", i, r.Err)
		}
	}
	wg.Wait()

	if err := e.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := e.Metrics()
	t.Logf("指标：submitted=%d merged=%d peak=%d backpressureBlocks=%d fired=%d inflight=%d",
		m.Submitted, m.Merged, m.MailboxPeak, m.BackpressureBlocks, m.FiredWindows, m.InFlight)
	if m.BackpressureBlocks == 0 {
		t.Fatal("极小邮箱+阻塞 sink 下应观测到背压，但 BackpressureBlocks=0")
	}
	if m.MailboxPeak != 2 {
		t.Fatalf("MailboxPeak=%d，邮箱容量为 2，峰值应达到 2", m.MailboxPeak)
	}
	if m.InFlight != 0 {
		t.Fatalf("Drain 后 InFlight=%d，应为 0", m.InFlight)
	}
	// 顺序由 orderedSink 在每次输出时校验；这里再断言输出数量。
	s.mu.Lock()
	totalFired := len(s.out)
	s.mu.Unlock()
	if int64(totalFired) != m.FiredWindows {
		t.Fatalf("sink 收到 %d 个输出，引擎统计触发 %d 个", totalFired, m.FiredWindows)
	}
	if totalFired < keys {
		t.Fatalf("fired outputs = %d, want >= %d", totalFired, keys)
	}
}

// TestCancelledSubmitDoesNotLoseAcknowledged 验证：
// ctx 取消只能拦截"尚未入队"的事件；一旦入队（已确认），即使调用方放弃等待，
// 事件仍被处理；未入队事件绝不影响聚合。全部通过通道握手同步，不依赖 sleep 时序。
func TestCancelledSubmitDoesNotLoseAcknowledged(t *testing.T) {
	dir := t.TempDir()
	gate := make(chan struct{})
	s := &orderedSink{last: map[string]time.Time{}, gate: gate}
	e := New(Config{
		GroupID: "gX", WindowSize: 10 * time.Second, AllowedLateness: 0,
		WatermarkLag: 0, MailboxSize: 2,
		Logger: log.New(testWriter{t}, "[engine] ", log.LstdFlags|log.Lmicroseconds),
	}, checkpoint.NewFileStore(dir, "gX"), s.sink)
	ctx := context.Background()
	if _, err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// 让 worker 处理到"触发 [10,20) 的 sink"时永久阻塞：
	// 先投 t=12（仅入窗不触发），再投 t=20（水位线到 20，[10,20) 触发并卡在 sink）。
	first := make(chan struct{})
	s.beforeSink = func(o model.WindowOutput) {
		if o.State.Start.Equal(time.Unix(10, 0).UTC()) {
			close(first)
			<-gate
		}
	}
	// t=12 必须先被同步处理（仅入窗不触发），再投 t=20；不能用两个 goroutine 并发投，
	// 否则入队顺序不保证，可能先处理 t=20 导致空窗口 [10,20) 触发。
	if _, err := e.Submit(ctx, 0, model.Event{Key: "a", Timestamp: time.Unix(12, 0).UTC(), Value: 1}); err != nil {
		t.Fatalf("submit t=12: %v", err)
	}
	go func() {
		if _, err := e.Submit(ctx, 1, model.Event{Key: "a", Timestamp: time.Unix(20, 0).UTC(), Value: 1}); err != nil {
			t.Errorf("submit t=20: %v", err)
		}
	}()
	<-first // worker 已卡在 [10,20) 的 sink；t=20 的 Submit 正等 ack，邮箱为空。

	// 事件2/3 属于尚开放的 [20,30)，依次填满容量 2 的邮箱（均已确认）。
	ack1, err := e.SubmitAsync(ctx, 2, model.Event{Key: "a", Timestamp: time.Unix(21, 0).UTC(), Value: 42})
	if err != nil {
		t.Fatal(err)
	}
	ack2, err := e.SubmitAsync(ctx, 3, model.Event{Key: "a", Timestamp: time.Unix(22, 0).UTC(), Value: 777})
	if err != nil {
		t.Fatal(err)
	}

	// 事件4 无法入队（邮箱满 + worker 卡死）：取消必须让 SubmitAsync 快速失败。
	cctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := e.SubmitAsync(cctx, 4, model.Event{Key: "a", Timestamp: time.Unix(23, 0).UTC(), Value: 2026})
		errCh <- err
	}()
	// 给 errCh 协程一点时间进入"入队阻塞"，然后取消。这里用指标判定：邮箱已被 1、2 填满，
	// 第三个必然阻塞，因此直接取消是确定的。
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("未入队事件取消应返回 context.Canceled，got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel 未解除未入队 Submit 的阻塞")
	}

	close(gate) // 放开 sink：worker 将依次处理已确认的事件 1、2。

	// 投递 t=30 推进水位线触发 [20,30)（必须排在事件 1、2 之后）。
	// 先等 ack1/ack2 完成，保证顺序确定。
	if r := <-ack1; r.Err != nil || r.Res.Kind != model.KindMerged {
		t.Fatalf("ack1: %+v", r)
	}
	if r := <-ack2; r.Err != nil || r.Res.Kind != model.KindMerged {
		t.Fatalf("ack2: %+v", r)
	}
	if _, err := e.Submit(ctx, 5, model.Event{Key: "a", Timestamp: time.Unix(30, 0).UTC(), Value: 0}); err != nil {
		t.Fatal(err)
	}
	if err := e.Drain(ctx); err != nil {
		t.Fatal(err)
	}

	// [20,30) 恰好包含已确认的 t=20(1)、42、777，不包含被取消的 2026。
	var open *model.WindowState
	for i := range s.out {
		if s.out[i].State.Start.Equal(time.Unix(20, 0).UTC()) {
			w := s.out[i].State
			open = &w
		}
	}
	if open == nil {
		t.Fatal("窗口 [20,30) 未触发：已确认事件可能被丢弃")
	}
	if open.Count != 3 || open.Sum != 820 {
		t.Fatalf("窗口 [20,30) = count=%d sum=%d，要求 count=3 sum=820（1+42+777，排除 2026）",
			open.Count, open.Sum)
	}
}
