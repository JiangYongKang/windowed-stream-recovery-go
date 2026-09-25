# windowed-stream-recovery-go

本地可运行的**实时事件时间窗口聚合 + 故障恢复**组件。用本地输入流（带位点的事件）
与本地文件检查点模拟真实流处理，使乱序、迟到、重启重放三种情况下的聚合结论
**确定、可复现、可解释**。

无第三方依赖，标准库实现；所有并发测试在 `-race` 下通过。

## 目录结构

```
pkg/model        事件 / 处理结论 / 窗口状态等基础类型
pkg/watermark    单调水位线（max(eventTime) - lag）
pkg/window       单分组滚动窗口聚合：乱序并入、宽限期、触发、快照/恢复
pkg/checkpoint   原子文件检查点：CRC32、版本/分组/位点回退校验
pkg/processor    多分组并发引擎：有界邮箱、背压、屏障检查点、恢复去重
cmd/demo         本地端到端演示
```

## 1. 事件时间（event time）与延迟语义

- **窗口划分按事件时间，不按处理时间**。滚动窗口 `[start, start+size)`，
  `start = eventTime.Truncate(size)`（`pkg/window.WindowOf`）。
- **水位线（watermark）**：`watermark = max(已观察事件时间) - WatermarkLag`，
  全局共享、**单调不减**。它表达"事件时间早于此值的输入语义上已到齐"，
  滞后量 `WatermarkLag` 给传输抖动留余量。
- **允许延迟（allowed lateness）**：窗口并不会在 watermark 越过窗口末尾时
  立刻关闭，而是等到

  ```
  watermark >= windowEnd + AllowedLateness
  ```

  才**最终触发（fire）**。因此：

  - 乱序 / 迟到但仍在宽限期内的事件 → **并入所属窗口**，结论 `KindMerged`，
    原因字符串标明"迟到事件在宽限期内并入"；
  - 窗口最终触发后（或 watermark 已越过关闭界）才到达的事件 →
    **显式拒绝**，结论 `KindDroppedLate`，并返回所属窗口与关闭界，
    **绝不静默丢弃**；拒绝不改变任何已聚合状态。
- 触发结果对同一 key **严格按窗口起始时间升序**输出。

三种投递结论（`model.AcceptResult.Kind`）可被调用方按原因区分处理：

| Kind | 含义 | 是否改变聚合 |
|---|---|---|
| `KindMerged` | 正常并入（含宽限期内迟到） | 是 |
| `KindDroppedLate` | 超过宽限关闭界 / 窗口已触发 | 否（显式拒绝） |
| `KindDuplicate` | 位点 ≤ 该分组已提交位点，判定为重放 | 否（幂等忽略） |

## 2. 检查点与恢复兼容规则

磁盘布局（`dir` 目录）：

- `checkpoint.json`：两行文本——第一行 JSON 信封（版本/CRC32/长度），
  第二行 base64 载荷；
- `.checkpoint.json.tmp`：原子替换前的临时文件。写入流程为
  **临时文件 → fsync → rename → fsync 目录**，崩溃时只会留下完整旧文件或
  未完成临时文件，不会出现半提交状态。

检查点内容：格式版本、分组 ID、单调序号 `Seq`、**已完全处理的输入位点
`Offset`**、全局水位线、每个分组的窗口状态（含各窗口是否已 `Fired`）与
该分组已处理位点。

启动恢复时依次执行下列校验，任何一项不过都**直接拒绝启动**，绝不用部分
状态继续运行，错误为 `*checkpoint.Error`，按 `Reason` 可区分：

| Reason | 触发条件 |
|---|---|
| `incomplete` | 文件截断 / 信封或载荷缺失 / 非法 base64（上次写入未原子完成） |
| `crc_mismatch` | 载荷长度不符或 CRC32 不匹配（损坏或被篡改） |
| `incompatible_version` | 检查点版本 ≠ 当前二进制支持的 `checkpoint.Version` |
| `group_mismatch` | 检查点 `GroupID` 与启动配置不一致（防止串用别的作业状态） |
| `offset_regression` | 恢复/保存位点小于已提交位点，或 Seq 回退 |

**不重不漏的依据**：

- 检查点的 `Offset` 取所有分组中**最慢分组**的已处理位点，表示此前的输入
  在所有分组都已完成处理并产出；
- 恢复后重放时，事件按**分组位点**判重：`offset <= 该分组已提交位点`
  即返回 `KindDuplicate`，不改变聚合；已触发窗口的 `Fired` 标记也被恢复，
  因此重启不会二次触发、也不会漏掉未触发窗口。

`Engine.Checkpoint` 使用**屏障**：屏障按邮箱顺序排在各分组全部在途事件之后，
只有屏障之前的事件都已完成下游 sink，才采集一致状态并落盘。

## 3. 并发、有界缓冲与背压

- 每个分组（key）一个**串行 worker** + 容量 `MailboxSize` 的有界邮箱；
  不同分组并行，同一分组的去重 → 并入 → 触发 → 输出严格串行，顺序确定。
- 下游变慢时：邮箱填满后 `Submit/SubmitAsync` **阻塞形成背压**，
  **不会丢弃任何已入队（已确认）事件**。
- 任务取消：`ctx` 取消只能使**尚未入队**的投递失败（事件未被确认）；
  一旦入队成功，即使调用方不再等待，事件仍会被处理到底。
- 可观测指标（`processor.Metrics`，全部原子读写）：
  `Submitted / Merged / DroppedLate / Duplicates / FiredWindows /
  InFlight / MailboxPeak / BackpressureBlocks / Checkpoints`。
- 下游 sink 返回错误会使引擎进入 fatal 状态，屏障检查点拒绝产出，
  避免把未完成输出的状态标记为已提交。
- 引擎内部两把锁分离（状态锁 / 邮箱生命周期读写锁），入队阻塞期间不持有
  worker 需要的状态锁，避免背压自死锁；`go test -race` 无数据竞争报告。

## 4. 本地验证方法

要求 Go 1.25+。

```bash
# 全部单元/集成测试，带竞态检测
go test -race ./...

# 多跑几轮观察稳定性（测试均为确定性同步，不依赖时序 sleep）
go test -race -count=5 ./...

# 查看水位线推进 / 窗口触发 / 超期拒绝 / 恢复依据等日志
go test -race ./... -v
```

端到端演示（三个阶段）：

```bash
# 阶段1：乱序并入，写检查点后模拟崩溃（不 Drain 直接退出）
go run ./cmd/demo -dir ./ckpt-demo -phase 1

# 阶段2：从检查点恢复，重放旧事件（全部判重）+ 续投新事件 + 超期事件被拒绝
go run ./cmd/demo -dir ./ckpt-demo -phase 2

# 破坏检查点后启动：必须因 crc_mismatch 被明确拒绝
go run ./cmd/demo -dir ./ckpt-demo -phase corrupt
```

测试覆盖点：

- `pkg/window`：窗口归属、宽限期内乱序并入、触发后超期拒绝不静默丢弃、
  触发顺序与幂等、快照/恢复保留 `Fired` 与位点；
- `pkg/watermark`：单调不减、lag 推导、快照恢复；
- `pkg/checkpoint`：往返一致、等长篡改 → CRC、截断 → incomplete、
  版本/分组/位点回退分别拒绝、原因标签稳定；
- `pkg/processor`：崩溃重启重放**不重不漏**（跨两段运行校验窗口不重复触发、
  聚合值确定）、损坏/不兼容检查点拒绝、8 分组并发下同 key 输出有序、
  小邮箱 + 慢下游的背压指标、取消时未入队事件不影响聚合而已确认事件不丢。
