// Package checkpoint 负责聚合状态的原子持久化、校验与恢复兼容性检查。
//
// 磁盘布局（dir 目录下）：
//   - checkpoint.json : 已提交检查点（一行 JSON 信封 + 一行载荷）
//   - .checkpoint.json.tmp : 原子替换前的临时文件，存在即说明上次写入未完成
//
// 兼容性规则见 Validate：版本必须等于 Version；groupID 必须一致；
// 恢复位点不得小于已提交位点（位点回退一律拒绝）。
package checkpoint

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/highcumontoa/windowed-stream-recovery-go/pkg/window"
)

// Version 是检查点格式版本，不兼容时必须显式拒绝恢复。
const Version = 1

const (
	fileName = "checkpoint.json"
	tmpName  = ".checkpoint.json.tmp"
)

// State 是一次检查点的完整内容（载荷，参与 CRC 校验）。
type State struct {
	Version   int                 `json:"version"`
	GroupID   string              `json:"group_id"`
	Seq       int64               `json:"seq"`
	Offset    int64               `json:"offset"`
	Watermark time.Time           `json:"watermark"`
	Keys      []window.KeyedState `json:"keys"`
}

// CorruptReason 区分检查点被拒绝的具体原因。
type CorruptReason int

const (
	ReasonOK CorruptReason = iota
	// ReasonIncomplete 临时文件残留/文件截断/信封不完整（上次写入未原子提交）。
	ReasonIncomplete
	// ReasonCRC 载荷校验和或长度不匹配（损坏或被篡改）。
	ReasonCRC
	// ReasonVersion 版本不兼容。
	ReasonVersion
	// ReasonGroup 分组标识不匹配。
	ReasonGroup
	// ReasonOffsetRegress 恢复位点相对已提交位点发生回退。
	ReasonOffsetRegress
)

// String 返回原因的稳定标识，便于日志与测试按原因断言。
func (r CorruptReason) String() string {
	switch r {
	case ReasonIncomplete:
		return "incomplete"
	case ReasonCRC:
		return "crc_mismatch"
	case ReasonVersion:
		return "incompatible_version"
	case ReasonGroup:
		return "group_mismatch"
	case ReasonOffsetRegress:
		return "offset_regression"
	default:
		return "ok"
	}
}

// Error 携带拒绝原因，调用方可 errors.As 后按 Reason 区分处理。
type Error struct {
	Reason CorruptReason
	Msg    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("checkpoint rejected (%s): %s", e.Reason, e.Msg)
}

func reject(r CorruptReason, format string, args ...any) *Error {
	return &Error{Reason: r, Msg: fmt.Sprintf(format, args...)}
}

// envelope 是磁盘上的第一行（元信息，不参与载荷 CRC）。
type envelope struct {
	Version int    `json:"version"`
	CRC32   uint32 `json:"crc32"`
	Size    int    `json:"size"`
}

// Store 是检查点存储抽象。
type Store interface {
	Save(ctx context.Context, st State) error
	Load(ctx context.Context) (State, error)
	CommittedOffset(ctx context.Context) (int64, error)
}

// FileStore 以"临时文件 + fsync + 原子重命名"方式写入本地检查点。
type FileStore struct {
	dir     string
	groupID string
}

// NewFileStore 创建本地文件检查点存储（目录会被创建）。
func NewFileStore(dir, groupID string) *FileStore {
	return &FileStore{dir: dir, groupID: groupID}
}

func (s *FileStore) path() string    { return filepath.Join(s.dir, fileName) }
func (s *FileStore) tmpPath() string { return filepath.Join(s.dir, tmpName) }

// ErrNotFound 表示尚无已提交检查点。
var ErrNotFound = errNotFound{}

type errNotFound struct{}

func (errNotFound) Error() string { return "checkpoint not found" }

// Save 原子写入完整检查点。Seq 由存储层按已提交值单调递增分配；
// 候选 offset 若小于已提交 offset 则拒绝（防止位点回退覆盖）。
func (s *FileStore) Save(ctx context.Context, st State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}

	committed, err := s.loadCommittedLocked(s.path())
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err == nil {
		if st.Offset < committed.Offset {
			return reject(ReasonOffsetRegress,
				"refuse to save offset %d behind committed offset %d", st.Offset, committed.Offset)
		}
		st.Seq = committed.Seq + 1
	} else if st.Seq <= 0 {
		st.Seq = 1
	}

	st.Version = Version
	st.GroupID = s.groupID

	payload, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	env := envelope{Version: Version, CRC32: crc32.ChecksumIEEE(payload), Size: len(payload)}
	header, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	tmp := s.tmpPath()
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open temp checkpoint: %w", err)
	}
	if _, err := f.Write(header); err != nil {
		f.Close()
		return fmt.Errorf("write envelope: %w", err)
	}
	if _, err := f.WriteString("\n"); err != nil {
		f.Close()
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if _, err := f.WriteString(base64.StdEncoding.EncodeToString(payload)); err != nil {
		f.Close()
		return fmt.Errorf("write payload: %w", err)
	}
	if _, err := f.WriteString("\n"); err != nil {
		f.Close()
		return fmt.Errorf("finalize checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmp, s.path()); err != nil {
		return fmt.Errorf("atomic rename checkpoint: %w", err)
	}
	// 尽力 fsync 目录，保证重命名落盘。
	if dir, derr := os.Open(s.dir); derr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// Load 读取并做结构性校验（完整性 + CRC）。语义校验（版本/分组/位点回退）
// 由调用方通过 Validate 完成，保证两类拒绝原因可区分。
func (s *FileStore) Load(ctx context.Context) (State, error) {
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	return s.loadCommittedLocked(s.path())
}

func (s *FileStore) loadCommittedLocked(path string) (State, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return State{}, ErrNotFound
		}
		return State{}, fmt.Errorf("read checkpoint: %w", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != 2 || lines[0] == "" || lines[1] == "" {
		return State{}, reject(ReasonIncomplete, "checkpoint file must contain envelope and payload lines")
	}

	var env envelope
	if err := json.Unmarshal([]byte(lines[0]), &env); err != nil {
		return State{}, reject(ReasonIncomplete, "envelope unmarshal: %v", err)
	}
	payload, err := base64.StdEncoding.DecodeString(lines[1])
	if err != nil {
		return State{}, reject(ReasonIncomplete, "payload is not valid base64: %v", err)
	}
	if env.Size != len(payload) {
		return State{}, reject(ReasonCRC, "payload length %d != envelope size %d", len(payload), env.Size)
	}
	if crc32.ChecksumIEEE(payload) != env.CRC32 {
		return State{}, reject(ReasonCRC, "checksum mismatch: payload corrupted or tampered")
	}

	var st State
	if err := json.Unmarshal(payload, &st); err != nil {
		return State{}, reject(ReasonIncomplete, "payload unmarshal: %v", err)
	}
	return st, nil
}

// CommittedOffset 返回最近成功提交的位点；无检查点时返回 -1。
func (s *FileStore) CommittedOffset(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	st, err := s.loadCommittedLocked(s.path())
	if errors.Is(err, ErrNotFound) {
		return -1, nil
	}
	if err != nil {
		return -1, err
	}
	return st.Offset, nil
}

// HasIncompleteTemp 报告是否残留未完成原子提交的临时文件（供启动期诊断）。
func (s *FileStore) HasIncompleteTemp() bool {
	if _, err := os.Stat(s.tmpPath()); err == nil {
		return true
	}
	// 主文件长度为 0 也视为写入未完成。
	if fi, err := os.Stat(s.path()); err == nil && fi.Size() == 0 {
		return true
	}
	return false
}

// Validate 检查候选状态与已提交元信息/运行配置的兼容性。
// committedSeq/committedOffset 为已提交值；无历史时传 -1。
func Validate(st State, groupID string, committedSeq, committedOffset int64) *Error {
	if st.Version != Version {
		return reject(ReasonVersion, "checkpoint version %d, running binary supports %d", st.Version, Version)
	}
	if st.GroupID != groupID {
		return reject(ReasonGroup, "checkpoint group %q != requested group %q", st.GroupID, groupID)
	}
	if committedSeq >= 0 && st.Seq < committedSeq {
		return reject(ReasonOffsetRegress, "checkpoint seq %d behind committed seq %d", st.Seq, committedSeq)
	}
	if committedOffset >= 0 && st.Offset < committedOffset {
		return reject(ReasonOffsetRegress, "checkpoint offset %d behind committed offset %d", st.Offset, committedOffset)
	}
	return nil
}
