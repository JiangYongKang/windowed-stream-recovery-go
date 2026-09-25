# windowed-stream-recovery-go

本地可运行的实时**事件时间窗口聚合**与**故障恢复**组件。用本地输入流与文件检查点模拟真实运行，
使乱序、延迟与重启重放后的聚合结论保持确定且可解释。

## 快速开始

```bash
go build ./...                 # 编译
go test ./... -count=1         # 单元测试（日志中打印水位线、窗口触发与恢复依据）
go test ./... -count=1 -race   # 竞态检测
go run ./cmd/demo              # 本地演示：乱序输入 → 检查点 → 模拟崩溃 → 重放恢复
```

## 事件时间与延迟语义

- 每条事件携带 `EventTime`（事件时间，毫秒）与 `Seq`（源位点，分组内单调递增）。
- 事件按事件时间被分配到长度 `WindowSize` 的对齐滚动窗口 `[start, start+size)`（负时间戳同样向下取整对齐）。
- **水位线** = 已观察到的最大事件时间 − `AllowedLateness`，单调不减，乱序事件不会使其回退。
- 事件时间 **≥ 水位线** 的乱序事件会被正确并入所属窗口；**< 水位线** 的事件为超期事件，
  不会被静默丢弃，而是连同原因 `late-event` 发送到 `Rejections()` 通道，并计入 `Metrics.Late`。
- 当窗口 `End ≤ 水位线` 时窗口触发，结果按 `(Window.End, Window.Start, Key)` 确定性排序输出，
  同一分组的输出窗口顺序单调；每个窗口只触发一次。

## 检查点与兼容规则

- `Engine.Snapshot()` 生成检查点：格式版本、**按分组的位点**、水位线、各 `(key, window)` 聚合中间态。
- `FileStore` 以 JSON + CRC32 校验和写入，先写临时文件再原子替换，避免半截文件。
- 加载/恢复时以下情况被**明确拒绝**（错误可用 `errors.Is` 区分），绝不带部分状态继续运行：
  - `ErrCheckpointNotFound`：检查点不存在；
  - `ErrCheckpointCorrupt`：内容被篡改（CRC 不符）或文件不完整（JSON 无法解析）；
  - `ErrVersionMismatch`：格式版本与当前 `CheckpointVersion` 不一致；
  - `ErrOffsetRegression`：检查点位点低于允许的最小位点，或低于引擎当前任一分组的位点。
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
- 可观测指标（`Metrics.Snapshot()`）：`Accepted` / `Late` / `Duplicates` / `Fired` / `BackpressureStalls`。

## 本地验证方法

1. `go run ./cmd/demo`：观察第一轮运行的窗口触发日志、检查点保存、第二轮从检查点恢复后
   重放事件全部判重（`duplicate`）、超期事件被拒绝（`late-event`）、新事件继续聚合。
2. `go test ./stream/ -v -count=1`：单测日志中打印水位线推进、每次窗口触发（key/窗口/计数/水位线）
   与恢复依据（位点、水位线、分组数）。
3. 损坏检查点场景见 `TestCheckpointCorrupt`（篡改字节、截断文件），
   版本与位点回退见 `TestCheckpointVersionMismatch` / `TestCheckpointOffsetRegression` /
   `TestRestoreRejectsRegression`。

## 目录结构

```
stream/            核心库
  types.go         事件、窗口、拒绝原因、错误定义
  watermark.go     水位线跟踪与延迟判定
  window.go        窗口分配与聚合状态（触发排序确定）
  checkpoint.go    检查点格式、校验与文件存储
  engine.go        处理循环：去重 → 延迟判定 → 聚合 → 触发
  metrics.go       原子指标
  stream_test.go   乱序、超期、重放、损坏检查点、背压、并发测试
cmd/demo/          本地可运行的崩溃-恢复演示
```
