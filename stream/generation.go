package stream

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// 代际检查点：每次存档生成一个单调递增的代际文件，
// 目录内只保留最近 Retain 代，超出部分自动清理，磁盘占用有明确上界。

// DefaultRetain 是未显式指定时的默认保留代际数。
const DefaultRetain = 3

// generationFilePattern 匹配代际文件名 checkpoint-<id>.json。
var generationFilePattern = regexp.MustCompile(`^checkpoint-(\d+)\.json$`)

// SkippedGeneration 记录恢复时被跳过的一代及原因。
type SkippedGeneration struct {
	Generation int64
	Path       string
	Err        error
}

// RecoveryReport 描述一次恢复的最终选择与跳过记录。
type RecoveryReport struct {
	Generation int64 // 实际选用的代际；-1 表示没有任何可用存档
	Checkpoint Checkpoint
	Skipped    []SkippedGeneration
}

// GenerationStore 在目录内按代际管理检查点文件。
// 写入（临时文件 + 原子替换）与清理（逐文件删除）都是原子的：
// 中途被打断不会留下"看着完整、其实是半截"的存档——
// 未完成的写入只残留 .tmp 文件，未完成的清理只多留几代旧文件，均不影响正确性。
type GenerationStore struct {
	Dir       string
	Retain    int // 保留的最大代际数，<=0 时取 DefaultRetain
	MinOffset int64
	Logger    *log.Logger
}

// NewGenerationStore 创建代际存档存储。minOffset 用于拒绝位点回退。
func NewGenerationStore(dir string, retain int, minOffset int64, logger *log.Logger) *GenerationStore {
	if retain <= 0 {
		retain = DefaultRetain
	}
	if logger == nil {
		logger = log.Default()
	}
	return &GenerationStore{Dir: dir, Retain: retain, MinOffset: minOffset, Logger: logger}
}

func (gs *GenerationStore) logf(format string, args ...any) {
	gs.Logger.Printf("[genstore] %s", fmt.Sprintf(format, args...))
}

// generationPath 返回某一代的文件路径。
func (gs *GenerationStore) generationPath(id int64) string {
	return filepath.Join(gs.Dir, fmt.Sprintf("checkpoint-%06d.json", id))
}

// Generations 列出目录中现存的代际编号（升序）。
func (gs *GenerationStore) Generations() ([]int64, error) {
	entries, err := os.ReadDir(gs.Dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("genstore: read dir: %w", err)
	}
	var ids []int64
	for _, e := range entries {
		m := generationFilePattern.FindStringSubmatch(e.Name())
		if m == nil {
			continue // 忽略 .tmp 残留与其他文件
		}
		id, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// Save 将检查点存为新一代（当前最大代际 + 1），随后轮转清理超出保留上限的旧代。
// 返回新代际编号。
func (gs *GenerationStore) Save(c Checkpoint) (int64, error) {
	if err := c.validate(gs.MinOffset); err != nil {
		return -1, err
	}
	payload, err := encodeCheckpoint(c)
	if err != nil {
		return -1, err
	}
	if err := os.MkdirAll(gs.Dir, 0o755); err != nil {
		return -1, fmt.Errorf("genstore: mkdir: %w", err)
	}
	ids, err := gs.Generations()
	if err != nil {
		return -1, err
	}
	next := int64(1)
	if len(ids) > 0 {
		next = ids[len(ids)-1] + 1
	}
	path := gs.generationPath(next)
	if err := writeFileAtomic(path, payload); err != nil {
		return -1, err
	}
	gs.logf("saved generation %d: offsets=%v watermark=%d keys=%d",
		next, c.Offsets, c.Watermark, len(c.States))
	gs.rotate(append(ids, next))
	return next, nil
}

// rotate 删除超出保留上限的最旧代际。逐文件删除，单文件删除是原子的；
// 中途崩溃只会多留几代旧文件，不会破坏任何一代的完整性。
func (gs *GenerationStore) rotate(ids []int64) {
	for len(ids) > gs.Retain {
		victim := ids[0]
		path := gs.generationPath(victim)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			gs.logf("rotate: failed to remove generation %d (%s): %v", victim, path, err)
			return // 清理失败不影响正确性，下次存档时再试
		}
		gs.logf("rotated out generation %d (retain=%d, remaining=%d)", victim, gs.Retain, len(ids)-1)
		ids = ids[1:]
	}
}

// loadOne 读取并校验指定代际；错误可用 errors.Is 区分
// （ErrCheckpointNotFound / ErrCheckpointCorrupt / ErrVersionMismatch / ErrOffsetRegression）。
func (gs *GenerationStore) loadOne(id int64) (Checkpoint, error) {
	path := gs.generationPath(id)
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Checkpoint{}, ErrCheckpointNotFound
		}
		return Checkpoint{}, fmt.Errorf("genstore: read: %w", err)
	}
	c, err := decodeCheckpoint(raw)
	if err != nil {
		return Checkpoint{}, err
	}
	if err := c.validate(gs.MinOffset); err != nil {
		return Checkpoint{}, err
	}
	return c, nil
}

// Recover 从最新一代开始逐代回退，返回第一个可用存档及跳过记录。
// 最新一代损坏、版本不兼容或位点回退时不会拿残缺状态凑合，而是明确记录原因并退到更旧一代。
// 没有任何可用代际时返回 ErrCheckpointNotFound（报告中仍含全部跳过记录）。
func (gs *GenerationStore) Recover() (RecoveryReport, error) {
	report := RecoveryReport{Generation: -1}
	ids, err := gs.Generations()
	if err != nil {
		return report, err
	}
	for i := len(ids) - 1; i >= 0; i-- {
		id := ids[i]
		c, err := gs.loadOne(id)
		if err != nil {
			gs.logf("skip generation %d: %v", id, err)
			report.Skipped = append(report.Skipped, SkippedGeneration{
				Generation: id, Path: gs.generationPath(id), Err: err,
			})
			continue
		}
		report.Generation = id
		report.Checkpoint = c
		gs.logf("selected generation %d for recovery: offsets=%v watermark=%d skipped=%d",
			id, c.Offsets, c.Watermark, len(report.Skipped))
		return report, nil
	}
	gs.logf("no usable generation found (checked %d, skipped %d)", len(ids), len(report.Skipped))
	return report, ErrCheckpointNotFound
}
