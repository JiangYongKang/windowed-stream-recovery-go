# windowed-stream-recovery-go

本地可运行的实时**事件时间窗口聚合**与**故障恢复**组件。用本地输入流与文件检查点模拟真实运行，
使乱序、延迟与重启重放后的聚合结论保持确定且可解释。

## 快速开始

```bash
go build ./...                 # 编译
go test ./... -count=1         # 单元测试（日志中打印水位线、窗口触发与恢复依据）
go test ./... -count=1 -race   # 竞态检测
go run ./cmd/demo              # 本地演示：运行中存档 → 模拟崩溃（最新一代损坏）→ 回退恢复 → 重放
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

- 配置 `Config.CheckpointStore`（`*GenStore`）后，引擎**一边处理事件一边存档**：
  - `CheckpointEvery`：每确认 N 条事件落一代；
  - `CheckpointInterval`：每隔该时长落一代（空闲时也会触发）；
  - 两者可同时设置，先到先触发；正常停止（`ctx` 取消并排空输入后）也会落最后一代。
- **一致性语义**：快照与写入都在 `Run` 所在 goroutine 内、事件处理之间的边界上完成，
  因此每代存档都是某一确定位点上的完整一致状态（按分组位点 + 水位线 + 全部窗口中间态），
  无需停流、无需加锁；存档期间已接收的事件不会丢失，同一分组的输出顺序不会被打乱。
- 用任一代存档恢复后，重放 `Seq ≤ 已确认位点` 的事件会被判重丢弃，
  每个分组的聚合结论与从未停过的运行**完全一致，不重不漏**。
- 存档成功/失败分别计入 `Metrics.Checkpoints` / `Metrics.CheckpointFailures`，
  日志中可见每一代的生成（`checkpoint generation N saved (reason=count|interval|shutdown)`）。

## 代际管理与自动清理

- `GenStore` 每存一次产生一个单调递增的代际文件 `ckpt-000001.json`、`ckpt-000002.json`……
- **保留上限**：`Retain` 为最多保留的代际数，超出后最旧的代际被自动轮转清理，
  磁盘占用有明确上界（Retain 份），日志可见 `rotated out` 记录。
- **原子性**：写入采用"临时文件 + 原子改名"——中途被打断只会残留 `.tmp` 临时文件，
  磁盘上不存在"看着完整、其实半截"的存档；残留的 `.tmp` 在下次列举时被自动清理。
  清理由 unlink 完成，同为原子操作。

## 恢复与回退规则

- `Engine.RestoreLatest(store)` 自动从**最新一代可用存档**恢复，并返回 `LoadReport`
  如实说明：实际采用了哪一代（`Generation`）、跳过了哪些（`Skipped`，含各自原因）。
- 以下情况的代际会被**跳过并回退到更旧的可用代际**，绝不拿残缺状态凑合继续跑：
  - 内容损坏（CRC 不符）或不完整（JSON 无法解析）→ `ErrCheckpointCorrupt`；
  - 格式版本与当前 `CheckpointVersion` 不一致 → `ErrVersionMismatch`；
  - 位点低于允许的最小位点（比当前进度还旧）→ `ErrOffsetRegression`。
- 所有代际都不可用（或目录为空）时返回错误，启动方应将其视为全新启动或人工介入的信号。
- **兼容范围**：仅兼容 `CheckpointVersion` 相同的存档；格式变更会递增版本号，
  旧版本存档被明确拒绝（跳过），不会带部分状态继续运行。

## 检查点格式与校验

- `Engine.Snapshot()` 生成检查点：格式版本、**按分组的位点**、水位线、各 `(key, window)` 聚合中间态。
- 单文件存储 `FileStore` 与代际存储 `GenStore` 使用同一封装：JSON + CRC32 校验和，
  先写临时文件再原子替换。
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
  `BackpressureStalls` / `Checkpoints` / `CheckpointFailures`。

## 本地验证方法

1. `go run ./cmd/demo`：观察运行中逐代存档（count/interval/shutdown 三种触发）、
   代际轮转清理、最新一代被篡改后恢复时跳过它并回退到更旧可用代际、
   重放事件全部判重（`duplicate`）、超期事件被拒绝（`late-event`）。
2. `go test ./stream/ -v -count=1`：单测日志中打印每一代存档的生成、轮转、被选中或被跳过的情况：
   - 运行中持续存档：`TestOnlineCheckpointWhileRunning`；
   - 代际轮转清理：`TestGenerationRotation`；
   - 写入被打断的原子性：`TestInterruptedWriteLeavesNoValidGeneration`；
   - 最新一代不可用时的回退恢复：`TestRecoveryFallback`（损坏/版本不符）、
     `TestRecoveryFallbackStaleOffset`（位点过旧）、`TestRecoveryAllGenerationsUnusable`（全部不可用）；
   - 恢复后聚合不重不漏：`TestCrashRecoveryMatchesUninterrupted`（与从未停过的参照运行逐条比对输出）。
3. 竞态检测：`go test ./... -count=1 -race`。
4. 单文件检查点的损坏/版本/位点回退场景见 `TestCheckpointCorrupt` /
   `TestCheckpointVersionMismatch` / `TestCheckpointOffsetRegression` / `TestRestoreRejectsRegression`。

## 目录结构

```
stream/            核心库
  types.go         事件、窗口、拒绝原因、错误定义
  watermark.go     水位线跟踪与延迟判定
  window.go        窗口分配与聚合状态（触发排序确定）
  checkpoint.go    检查点格式、校验与单文件存储
  genstore.go      代际检查点存储：原子写入、保留上限轮转、回退加载
  engine.go        处理循环：去重 → 延迟判定 → 聚合 → 触发；运行中周期存档
  metrics.go       原子指标
  stream_test.go   乱序、超期、重放、损坏检查点、背压、并发测试
  genstore_test.go 运行中存档、代际轮转、回退恢复、恢复后不重不漏测试
cmd/demo/          本地可运行的运行中存档 + 崩溃回退恢复演示
```
