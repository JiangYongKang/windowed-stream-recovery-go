package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// CheckpointVersion 为当前检查点格式版本。格式变更时必须递增，
// 旧版本检查点将被明确拒绝（ErrVersionMismatch），不允许带部分状态继续运行。
const CheckpointVersion = 1

// Checkpoint 是引擎状态的可持久化快照。
type Checkpoint struct {
	Version   int
	Offsets   map[string]int64 // 每个分组已确认处理的最大 Seq（重放时按分组去重）
	Watermark int64
	MaxSeen   int64
	States    map[string]map[Window]windowState
}

// maxOffset 返回所有分组位点中的最大值（无分组时为 -1）。
func (c Checkpoint) maxOffset() int64 {
	max := int64(-1)
	for _, off := range c.Offsets {
		if off > max {
			max = off
		}
	}
	return max
}

// validate 校验检查点是否可被接受。
func (c Checkpoint) validate(minOffset int64) error {
	if c.Version != CheckpointVersion {
		return fmt.Errorf("%w: got version %d, want %d", ErrVersionMismatch, c.Version, CheckpointVersion)
	}
	if max := c.maxOffset(); max < minOffset {
		return fmt.Errorf("%w: max offset %d < min %d", ErrOffsetRegression, max, minOffset)
	}
	return nil
}

// Store 是检查点存储接口。
type Store interface {
	Save(c Checkpoint) error
	Load() (Checkpoint, error)
}

// filePayload 是磁盘上的检查点封装：JSON 数据 + CRC32 校验和。
type filePayload struct {
	Data     []byte `json:"data"`
	Checksum uint32 `json:"checksum"`
}

// FileStore 将检查点写入本地文件（先写临时文件再原子替换，避免半截文件）。
type FileStore struct {
	Path      string
	MinOffset int64 // 允许的最小位点，低于此值视为位点回退
}

// NewFileStore 创建文件检查点存储。minOffset 用于拒绝位点回退。
func NewFileStore(path string, minOffset int64) *FileStore {
	return &FileStore{Path: path, MinOffset: minOffset}
}

// encodeCheckpoint 序列化检查点为带 CRC32 校验和的封装字节。
func encodeCheckpoint(c Checkpoint) ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: marshal: %w", err)
	}
	payload, err := json.Marshal(filePayload{Data: data, Checksum: crc32.ChecksumIEEE(data)})
	if err != nil {
		return nil, fmt.Errorf("checkpoint: marshal payload: %w", err)
	}
	return payload, nil
}

// decodeCheckpoint 解析并严格校验封装字节：损坏（CRC 不符/JSON 不完整）
// 会以 ErrCheckpointCorrupt 被拒绝。
func decodeCheckpoint(raw []byte) (Checkpoint, error) {
	var p filePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return Checkpoint{}, fmt.Errorf("%w: bad envelope: %v", ErrCheckpointCorrupt, err)
	}
	if crc32.ChecksumIEEE(p.Data) != p.Checksum {
		return Checkpoint{}, fmt.Errorf("%w: checksum mismatch", ErrCheckpointCorrupt)
	}
	var c Checkpoint
	if err := json.Unmarshal(p.Data, &c); err != nil {
		return Checkpoint{}, fmt.Errorf("%w: bad body: %v", ErrCheckpointCorrupt, err)
	}
	return c, nil
}

// writeFileAtomic 先写临时文件并落盘，再原子替换目标文件，避免半截文件。
func writeFileAtomic(path string, payload []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("checkpoint: write tmp: %w", err)
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		return fmt.Errorf("checkpoint: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("checkpoint: sync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("checkpoint: close tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	return nil
}

// Save 校验并原子写入检查点。
func (fs *FileStore) Save(c Checkpoint) error {
	if err := c.validate(fs.MinOffset); err != nil {
		return err
	}
	payload, err := encodeCheckpoint(c)
	if err != nil {
		return err
	}
	return writeFileAtomic(fs.Path, payload)
}

// Load 读取并严格校验检查点：不存在、损坏（CRC 不符/JSON 不完整）、
// 版本不兼容或位点回退都会以可区分的错误被拒绝。
func (fs *FileStore) Load() (Checkpoint, error) {
	raw, err := os.ReadFile(fs.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Checkpoint{}, ErrCheckpointNotFound
		}
		return Checkpoint{}, fmt.Errorf("checkpoint: read: %w", err)
	}
	c, err := decodeCheckpoint(raw)
	if err != nil {
		return Checkpoint{}, err
	}
	if err := c.validate(fs.MinOffset); err != nil {
		return Checkpoint{}, err
	}
	return c, nil
}

// Remove 删除检查点文件（测试与清理用）。
func (fs *FileStore) Remove() error {
	err := os.Remove(fs.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Dir 返回检查点所在目录。
func (fs *FileStore) Dir() string { return filepath.Dir(fs.Path) }
