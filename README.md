# windowed-stream-recovery-go

本地可运行的实时**事件时间窗口聚合**与**故障恢复**组件。用本地输入流与文件检查点模拟真实运行，
使乱序、延迟与重启重放后的聚合结论保持确定且可解释。支持**不停机定期存档**：
任务一边处理事件一边按时间/处理量落代际检查点，旧代际自动轮转清理，重启时自动从最新可用代际恢复。

## 快速开始

```bash
go build ./...                 # 编译
go test ./... -count=1         # 单元测试（日志中打印水位线、窗口触发、存档轮转与恢复依据）
go test ./... -count=1 -race   # 竞态检测
go run ./cmd/demo              # 本地演示：乱序输入 → 运行中存档 → 模拟崩溃 → 最新代损坏回退 → 重放恢复
```

## 事件时间与延迟语义

- 每条事件携带 `EventTime`（事件时间，毫秒）与 `Seq`（源位点，分组内单调递增）。
- 事件按事件时间被分配到长度 `WindowSize` 的对齐滚动窗口 `[start, start+size)`（负时间戳同样向下取整对齐）。
- **水位线** = 已观察到的最大事件时间 − `AllowedLateness`，单调不减，乱序事件不会使其回退。
- 事件时间 **≥ 水位线** 的乱序事件会被正确并入所属窗口；**< 水位线** 的事件为超期事件，
  不会被静默丢弃，而是连同原因 `late-event` 发送到 `Rejections()` 通道，并计入 `Metrics.Late`。
- 当窗口 `End ≤ 水位线` 时窗口触发，结果按 `(Window.End, Window.Start, Key)` 确定性排序输出，
  同一分组的输出窗口顺序单调；每个窗口只触发一次。

## 运行中存档（不停机检查点）

- 配置 `Config.Checkpoints`（`*GenerationStore`）后，引擎在**处理循环内**定期存档：
  - `CheckpointInterval`：按时间间隔落一代；
  - `CheckpointEvery`：每确认 N 条事件落一代；
  - `CheckpointOnStop`：停止前排空输入缓冲后落最终一代。
- **一致性语义**：快照与事件处理在同一个 goroutine 中串行进行，快照要么包含某条事件的全部
  影响、要么完全不包含，不存在"半截状态"；存档过程不阻塞事件接收（有界缓冲未满时）、
  不丢弃已接收事件、不改变任何分组的输出顺序。
- 恢复后配合**按分组位点重放去重**，每个分组的聚合结论与从未停机完全一致（状态级 exactly-once，
  不重不漏）。注意：若崩溃发生在"窗口已触发输出"与"下一次存档"之间，恢复后这些窗口结果
  可能被**重新输出**（at-least-once 送达），下游如需幂等可按 `(Key, Window)` 去重；
  聚合状态本身不会重复计入。
- 存档写失败不中断处理：计入 `Metrics.CheckpointErrors` 并记日志，旧代际仍可用于恢复；
  成功落盘计入 `Metrics.Checkpoints`。

## 代际保留与回退规则

- 每次存档在目录中生成一个单调递增的代际文件 `checkpoint-<id>.json`。
- **保留上限**：目录中只保留最近 `Retain` 代（默认 3），超出部分在每次存档后自动删除，
  磁盘占用有明确上界（`Retain × 单代大小`）。
- **原子性**：写入先落临时文件（含 fsync）再原子替换，清理逐文件删除；中途被打断只会
  残留 `.tmp` 文件或多留几代旧文件，绝不会留下"看着完整、其实是半截"的存档，
  `.tmp` 残留与无关文件在列举代际时被忽略。
- **回退恢复**：`Engine.RestoreLatest(store)` 从最新一代开始逐代尝试，第一代通过全部校验的
  代际被选中；被跳过的每一代连同原因（损坏 / 版本不兼容 / 位点回退）记录在
  `RecoveryReport.Skipped` 中并打印日志，绝不拿残缺状态凑合继续跑。
  所有代际都不可用时返回 `ErrCheckpointNotFound`（报告中仍含全部跳过记录）。

## 检查点格式与兼容规则

- 检查点内容：格式版本、**按分组的位点**、水位线、各 `(key, window)` 聚合中间态；
  封装为 JSON + CRC32 校验和。`FileStore`（单文件）与 `GenerationStore`（代际）共用同一格式。
- 加载/恢复时以下情况被**明确拒绝**（错误可用 `errors.Is` 区分），绝不带部分状态继续运行：
  - `ErrCheckpointNotFound`：检查点不存在（或没有任何可用代际）；
  - `ErrCheckpointCorrupt`：内容被篡改（CRC 不符）或文件不完整（JSON 无法解析）；
  - `ErrVersionMismatch`：格式版本与当前 `CheckpointVersion` 不一致；
  - `ErrOffsetRegression`：检查点位点低于允许的最小位点，或低于引擎当前任一分组的位点。
- **兼容范围**：只保证与当前 `CheckpointVersion` 完全一致的存档可恢复；格式变更会递增版本号，
  旧版本存档被明确拒绝（`ErrVersionMismatch`），不做静默迁移。
- **重放去重**：恢复后，`Seq ≤ 该分组已确认位点` 的重放事件被判为 `duplicate`，
  计入 `Metrics.Duplicates` 并送入拒绝通道，不会重复计入窗口；位点按分组独立维护，
  多分组并发时互不干扰。

## 背压与资源上限

- 输入、输出均为**有界缓冲**（`InputBuffer` / `OutputBuffer`）。
- 输入缓冲满时 `Submit` 立即返回 `ErrBufferFull`，背压对上游可观测，由上游决定重试或降级。
- 输出缓冲满（下游变慢）时先计入 `Metrics.BackpressureStalls` 再阻塞等待；
  已触发的窗口结果与已确认的事件**不会因取消而丢失**——`Run` 在 `ctx` 取消后会先排空输入缓冲再退出。
- 所有可变状态仅由 `Run` 所在 goroutine 访问，外部通过通道交互；指标为原子计数，
  并发执行在 `-race` 下无数据竞争。
- 可观测指标（`Metrics.Snapshot()`）：`Accepted` / `Late` / `Duplicates` / `Fired` /
  `BackpressureStalls` / `Checkpoints` / `CheckpointErrors`。

## 本地验证方法

1. `go run ./cmd/demo`：观察运行中逐代存档、旧代际轮转清理、最新一代被篡改后回退到
   更旧可用代际、重放事件全部判重（`duplicate`）、超期事件被拒绝（`late-event`）。
2. `go test ./stream/ -v -count=1`：单测日志中打印每一代存档的生成（`saved generation`）、
   轮转（`rotated out`）、被选中（`selected generation`）或被跳过（`skip generation`）及原因。
   关键场景：
   - `TestPeriodicCheckpointWhileRunning`：运行中持续存档，事件不丢、输出顺序不乱、保留上限生效；
   - `TestGenerationRotation`：代际轮转清理，磁盘上只剩最近 Retain 代；
   - `TestRecoverFallback` / `TestRecoverFallbackOffsetRegression`：最新代损坏/版本不符/位点回退时
     逐代回退并记录原因；
   - `TestRecoveryExactlyOnce`：故障运行（存档→停止→恢复→全量重放）与不间断参考运行的
     逐窗口聚合结论完全一致，不重不漏。
3. 损坏检查点场景另见 `TestCheckpointCorrupt`（篡改字节、截断文件），
   版本与位点回退见 `TestCheckpointVersionMismatch` / `TestCheckpointOffsetRegression` /
   `TestRestoreRejectsRegression`。
4. `go test ./... -count=1 -race`：竞态检测下全部用例通过。

## 目录结构

```
stream/                核心库
  types.go             事件、窗口、拒绝原因、错误定义
  watermark.go         水位线跟踪与延迟判定
  window.go            窗口分配与聚合状态（触发排序确定）
  checkpoint.go        检查点格式、校验、原子写入与单文件存储
  generation.go        代际存档：保留上限、轮转清理、回退恢复
  engine.go            处理循环：去重 → 延迟判定 → 聚合 → 触发 → 定期存档
  metrics.go           原子指标
  stream_test.go       乱序、超期、重放、损坏检查点、背压、并发测试
  generation_test.go   运行中存档、代际轮转、回退恢复、恢复后不重不漏测试
cmd/demo/              本地可运行的崩溃-恢复演示（含最新代损坏回退）
```
