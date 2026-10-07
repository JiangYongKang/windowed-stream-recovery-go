package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// 代际文件命名：ckpt-000001.json；写入中的临时文件带 .tmp 后缀，
// 未完成的写入永远不会以正式名字出现，因此磁盘上不存在"看着完整的半截存档"。
const (
	genFilePrefix = "ckpt-"
	genFileSuffix = ".json"
	genTmpSuffix  = ".tmp"
)

// SkippedGeneration 记录恢复时被跳过的一代存档及原因。
type SkippedGeneration struct {
	Generation int
	Reason     error
}

func (s SkippedGeneration) String() string {
	return fmt.Sprintf("gen=%d reason=%v", s.Generation, s.Reason)
}

// LoadReport 描述一次恢复加载的结论：实际采用哪一代、跳过了哪些及原因。
type LoadReport struct {
	Generation int // 实际采用的存档代际（0 表示未采用任何一代）
	Skipped    []SkippedGeneration
}

func (r LoadReport) String() string {
	if r.Generation == 0 {
		return fmt.Sprintf("no generation selected (skipped=%d)", len(r.Skipped))
	}
	if len(r.Skipped) == 0 {
		return fmt.Sprintf("selected gen=%d (no skips)", r.Generation)
	}
	parts := make([]string, len(r.Skipped))
	for i, s := range r.Skipped {
		parts[i] = s.String()
	}
	return fmt.Sprintf("selected gen=%d, skipped: %s", r.Generation, strings.Join(parts, "; "))
}

// GenStore 按代际管理检查点：每存一次产生一个单调递增的代际文件，
// 超过保留上限的旧代际被自动清理，磁盘占用有明确上界（Retain 份）。
// 写入采用"临时文件 + 原子改名"，清理由 unlink 完成，均为原子操作，
// 中途被打断不会留下看似完整实则半截的存档。
type GenStore struct {
	Dir       string
	Retain    int // 最多保留的代际数（<=0 时按 1 处理）
	MinOffset int64
	Logger    *log.Logger
}

// NewGenStore 创建代际检查点存储。retain 为保留的代际上限。
func NewGenStore(dir string, retain int, minOffset int64, logger *log.Logger) *GenStore {
	if retain <= 0 {
		retain = 1
	}
	if logger == nil {
		logger = log.Default()
	}
	return &GenStore{Dir: dir, Retain: retain, MinOffset: minOffset, Logger: logger}
}

func (gs *GenStore) genPath(gen int) string {
	return filepath.Join(gs.Dir, fmt.Sprintf("%s%06d%s", genFilePrefix, gen, genFileSuffix))
}

// parseGen 从文件名解析代际编号；非代际文件返回 false。
func parseGen(name string) (int, bool) {
	if !strings.HasPrefix(name, genFilePrefix) || !strings.HasSuffix(name, genFileSuffix) {
		return 0, false
	}
	mid := name[len(genFilePrefix) : len(name)-len(genFileSuffix)]
	n, err := strconv.Atoi(mid)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// list 返回当前存在的全部代际编号（升序），并顺带清理中断写入残留的临时文件。
func (gs *GenStore) list() ([]int, error) {
	entries, err := os.ReadDir(gs.Dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("genstore: read dir: %w", err)
	}
	var gens []int
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, genTmpSuffix) {
			// 上次写入被打断留下的临时文件：不是有效存档，清理掉。
			if err := os.Remove(filepath.Join(gs.Dir, name)); err == nil {
				gs.logf("removed stale temp file %s from interrupted write", name)
			}
			continue
		}
		if gen, ok := parseGen(name); ok {
			gens = append(gens, gen)
		}
	}
	sort.Ints(gens)
	return gens, nil
}

// List 返回当前存在的全部代际编号（升序）。
func (gs *GenStore) List() ([]int, error) { return gs.list() }

// Save 将检查点存为新一代，返回代际编号；随后按保留上限轮转清理旧代际。
func (gs *GenStore) Save(c Checkpoint) (int, error) {
	if err := c.validate(gs.MinOffset); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(gs.Dir, 0o755); err != nil {
		return 0, fmt.Errorf("genstore: mkdir: %w", err)
	}
	gens, err := gs.list()
	if err != nil {
		return 0, err
	}
	next := 1
	if len(gens) > 0 {
		next = gens[len(gens)-1] + 1
	}
	data, err := json.Marshal(c)
	if err != nil {
		return 0, fmt.Errorf("genstore: marshal: %w", err)
	}
	payload, err := json.Marshal(filePayload{Data: data, Checksum: crc32.ChecksumIEEE(data)})
	if err != nil {
		return 0, fmt.Errorf("genstore: marshal payload: %w", err)
	}
	final := gs.genPath(next)
	tmp := final + genTmpSuffix
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return 0, fmt.Errorf("genstore: write tmp: %w", err)
	}
	// 原子改名：此刻起该代要么完整可见，要么完全不可见。
	if err := os.Rename(tmp, final); err != nil {
		return 0, fmt.Errorf("genstore: rename: %w", err)
	}
	gs.logf("checkpoint generation %d written: offsets=%v watermark=%d states=%d",
		next, c.Offsets, c.Watermark, len(c.States))
	if err := gs.enforceRetention(); err != nil {
		return next, err
	}
	return next, nil
}

// enforceRetention 删除超出保留上限的最旧代际（unlink 为原子操作）。
func (gs *GenStore) enforceRetention() error {
	gens, err := gs.list()
	if err != nil {
		return err
	}
	for len(gens) > gs.Retain {
		oldest := gens[0]
		if err := os.Remove(gs.genPath(oldest)); err != nil {
			return fmt.Errorf("genstore: rotate out gen %d: %w", oldest, err)
		}
		gs.logf("checkpoint generation %d rotated out (retain=%d, remaining=%d)",
			oldest, gs.Retain, len(gens)-1)
		gens = gens[1:]
	}
	return nil
}

// Load 加载指定代际的检查点（含 CRC、版本与位点校验）。
func (gs *GenStore) Load(gen int) (Checkpoint, error) {
	raw, err := os.ReadFile(gs.genPath(gen))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Checkpoint{}, fmt.Errorf("gen %d: %w", gen, ErrCheckpointNotFound)
		}
		return Checkpoint{}, fmt.Errorf("gen %d: read: %w", gen, err)
	}
	var p filePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return Checkpoint{}, fmt.Errorf("gen %d: %w: bad envelope: %v", gen, ErrCheckpointCorrupt, err)
	}
	if crc32.ChecksumIEEE(p.Data) != p.Checksum {
		return Checkpoint{}, fmt.Errorf("gen %d: %w: checksum mismatch", gen, ErrCheckpointCorrupt)
	}
	var c Checkpoint
	if err := json.Unmarshal(p.Data, &c); err != nil {
		return Checkpoint{}, fmt.Errorf("gen %d: %w: bad body: %v", gen, ErrCheckpointCorrupt, err)
	}
	if err := c.validate(gs.MinOffset); err != nil {
		return Checkpoint{}, fmt.Errorf("gen %d: %w", gen, err)
	}
	return c, nil
}

// LoadLatest 从最新一代开始尝试恢复：损坏、版本不符或位点回退的代际被跳过并记录原因，
// 退到更旧的可用代际为止；绝不采用残缺状态凑合运行。
func (gs *GenStore) LoadLatest() (Checkpoint, LoadReport, error) {
	var report LoadReport
	gens, err := gs.list()
	if err != nil {
		return Checkpoint{}, report, err
	}
	if len(gens) == 0 {
		return Checkpoint{}, report, ErrCheckpointNotFound
	}
	for i := len(gens) - 1; i >= 0; i-- {
		gen := gens[i]
		c, err := gs.Load(gen)
		if err != nil {
			report.Skipped = append(report.Skipped, SkippedGeneration{Generation: gen, Reason: err})
			gs.logf("skipping checkpoint generation %d: %v", gen, err)
			continue
		}
		report.Generation = gen
		gs.logf("selected checkpoint generation %d for recovery (offsets=%v watermark=%d)",
			gen, c.Offsets, c.Watermark)
		return c, report, nil
	}
	return Checkpoint{}, report, fmt.Errorf("%w: no usable generation among %d (all skipped)",
		ErrCheckpointCorrupt, len(gens))
}

func (gs *GenStore) logf(format string, args ...any) {
	gs.Logger.Printf("[genstore] %s", fmt.Sprintf(format, args...))
}
