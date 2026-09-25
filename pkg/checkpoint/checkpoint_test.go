package checkpoint

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/model"
	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/window"
)

func sampleState(offset int64) State {
	return State{
		Version:   Version,
		GroupID:   "g1",
		Seq:       1,
		Offset:    offset,
		Watermark: time.Unix(100, 0).UTC(),
		Keys: []window.KeyedState{{
			Key:    "a",
			Offset: offset,
			Windows: []model.WindowState{
				{Start: time.Unix(0, 0).UTC(), End: time.Unix(10, 0).UTC(), Count: 2, Sum: 9, Min: 4, Max: 5, Fired: true},
			},
		}},
	}
}

func asCheckpointErr(t *testing.T, err error) *Error {
	t.Helper()
	var ce *Error
	if !errors.As(err, &ce) {
		t.Fatalf("want *checkpoint.Error, got %T: %v", err, err)
	}
	return ce
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, "g1")
	ctx := context.Background()
	if _, err := s.Load(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty store: want ErrNotFound, got %v", err)
	}
	if off, err := s.CommittedOffset(ctx); err != nil || off != -1 {
		t.Fatalf("committed offset = %d,%v", off, err)
	}
	if err := s.Save(ctx, sampleState(7)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Offset != 7 || got.GroupID != "g1" || got.Seq != 1 || len(got.Keys) != 1 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	if w := got.Keys[0].Windows[0]; !w.Fired || w.Sum != 9 || w.Count != 2 {
		t.Fatalf("window state mismatch: %+v", w)
	}
	// 第二次保存：seq 自动递增。
	st2 := sampleState(9)
	if err := s.Save(ctx, st2); err != nil {
		t.Fatal(err)
	}
	got2, _ := s.Load(ctx)
	if got2.Seq != 2 {
		t.Fatalf("seq = %d, want 2", got2.Seq)
	}
}

func TestSaveRejectsOffsetRegression(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, "g1")
	ctx := context.Background()
	if err := s.Save(ctx, sampleState(10)); err != nil {
		t.Fatal(err)
	}
	err := s.Save(ctx, sampleState(9))
	ce := asCheckpointErr(t, err)
	if ce.Reason != ReasonOffsetRegress {
		t.Fatalf("reason = %s, want offset_regression", ce.Reason)
	}
}

func TestCorruptPayloadRejectedAsCRC(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, "g1")
	ctx := context.Background()
	if err := s.Save(ctx, sampleState(3)); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, fileName)
	raw, _ := os.ReadFile(p)
	lines := strings.SplitN(string(raw), "\n", 2)
	// 翻转载荷第一个字符（保持 base64 长度），制造 CRC 不匹配。
	payload := []byte(lines[1])
	if payload[0] == 'A' {
		payload[0] = 'B'
	} else {
		payload[0] = 'A'
	}
	os.WriteFile(p, []byte(lines[0]+"\n"+string(payload)), 0o644)
	_, err := s.Load(ctx)
	ce := asCheckpointErr(t, err)
	if ce.Reason != ReasonCRC {
		t.Fatalf("reason = %s, want crc_mismatch (%v)", ce.Reason, ce)
	}
}

func TestTruncatedFileRejectedAsIncomplete(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, "g1")
	ctx := context.Background()
	if err := s.Save(ctx, sampleState(3)); err != nil {
		t.Fatal(err)
	}
	// 截断为只剩半行信封。
	p := filepath.Join(dir, fileName)
	raw, _ := os.ReadFile(p)
	os.WriteFile(p, raw[:len(raw)/4], 0o644)
	_, err := s.Load(ctx)
	ce := asCheckpointErr(t, err)
	if ce.Reason != ReasonIncomplete && ce.Reason != ReasonCRC {
		t.Fatalf("reason = %s, want incomplete/crc", ce.Reason)
	}

	// 残留临时文件必须可被诊断为未完成。
	os.WriteFile(filepath.Join(dir, tmpName), []byte("partial"), 0o644)
	if !s.HasIncompleteTemp() {
		t.Fatal("HasIncompleteTemp should report leftover temp file")
	}
}

func TestValidateReasons(t *testing.T) {
	st := sampleState(5)
	if e := Validate(st, "g1", -1, -1); e != nil {
		t.Fatalf("valid state rejected: %v", e)
	}
	if e := Validate(st, "other", -1, -1); e == nil || e.Reason != ReasonGroup {
		t.Fatalf("group check: %v", e)
	}
	bad := st
	bad.Version = Version + 99
	if e := Validate(bad, "g1", -1, -1); e == nil || e.Reason != ReasonVersion {
		t.Fatalf("version check: %v", e)
	}
	if e := Validate(st, "g1", st.Seq+1, -1); e == nil || e.Reason != ReasonOffsetRegress {
		t.Fatalf("seq regression check: %v", e)
	}
	if e := Validate(st, "g1", -1, st.Offset+1); e == nil || e.Reason != ReasonOffsetRegress {
		t.Fatalf("offset regression check: %v", e)
	}
}

func TestCorruptReasonStableLabels(t *testing.T) {
	cases := map[CorruptReason]string{
		ReasonIncomplete:    "incomplete",
		ReasonCRC:           "crc_mismatch",
		ReasonVersion:       "incompatible_version",
		ReasonGroup:         "group_mismatch",
		ReasonOffsetRegress: "offset_regression",
	}
	for r, want := range cases {
		if r.String() != want {
			t.Fatalf("%d -> %s, want %s", r, r.String(), want)
		}
	}
}
