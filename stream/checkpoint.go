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

// Save 校验并原子写入检查点。
func (fs *FileStore) Save(c Checkpoint) error {
	if err := c.validate(fs.MinOffset); err != nil {
		return err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}
	payload, err := json.Marshal(filePayload{Data: data, Checksum: crc32.ChecksumIEEE(data)})
	if err != nil {
		return fmt.Errorf("checkpoint: marshal payload: %w", err)
	}
	tmp := fs.Path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return fmt.Errorf("checkpoint: write tmp: %w", err)
	}
	if err := os.Rename(tmp, fs.Path); err != nil {
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	return nil
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
