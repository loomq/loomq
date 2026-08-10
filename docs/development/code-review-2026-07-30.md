# LoomQ 第一性原理代码审查（2026-07-30，v0.9.2）

> 审查范围：loomq-core 全部主源码（约 75 个文件，核心路径逐行精读 + 外围两组独立审阅）。
> 方法：不从"代码像不像教科书"出发，而从问题本质推导系统必须维持的不变量，再逐条对照实现。

## 0. 审查基准：从问题本质推导不变量

LoomQ 的本质是把"在未来某时刻可靠触发一次事件"压缩为三个不可再约的需求：

1. **持久化承诺**：状态一旦对调用方可见，必须已对磁盘可见（或明确声明的崩溃窗口内）。
2. **时间兑现**：到点必须恰好触发一次，或落入明确终态。
3. **崩溃可重建**：重启后系统状态 = 磁盘内容的确定性函数。

由此推出实现层必须维持的四条不变量，本次审查即对照它们：

- **I1 单持有方**：任一 intentId 在任意时刻最多存在于一个调度结构（bucket / cohort / 派发队列 / promotion cohort）之一。重复持有 = 重复投递。
- **I2 持久化先于承诺**：任何对调用方可见的状态迁移，必须先于（或同步于）其对磁盘可见。
- **I3 revision 单调 + 终态不可逆**：append-only + max-revision 去重的全部正确性基础。
- **I4 索引即所有权账本**：谁摘除索引项，谁拥有该 intent 的处置权；所有认领必须经账本互斥。

## 1. 总体结论

**架构方向正确，工程水准高于平均，但存在两处与自身第一性承诺相矛盾的缺口，以及调度所有权模型的一处结构性裂缝。**

做对了的（值得点名）：

- "磁盘 append-only + revision 去重 = 唯一事实源，内存全是可重建缓存"——这是"内核"的正确定义，恢复模型由此变得简单可证。
- GroupCommitBarrier 的 ticket 模型（先快照 pending 再 force 再发布 frontier）在协议层面正确，含内存可见性（volatile 链）推导；慢盘单飞行兜底 + 平台线程避免 VT pin，处理教科书级。
- Bucket 写/force 读写锁分离（P1-4 注释所述）正确关闭了"memcpy 与 force 竞争漏写"的微窗口。
- promote ↔ cancelCold 的双向清理（先指向 CANCELED 新槽再 awaitCommit + promote 后复核索引）是有完整因果论证的 TOCTOU 收口。
- 测试资产体现了"故障优先"思维（Bug*Test、崩溃注入、并发测试、时钟回拨）。

但这些"修过的疤"（P0-x/P1-x/Fix N 注释）同时说明：系统复杂度已高到会持续产生边界竞态。第 6 节回到这一点。

## 2. 严重发现

### P1-1　投递路径终态不落盘（I2 破缺）

**位置**：`PrecisionScheduler.finalizeIntent`（SUCCESS/DEAD_LETTER/EXPIRED 分支，~行 770-870）、`handleExpired`（~行 996）、`handleDeliveryFailure` 的 maxAttempts 分支（~行 924）。

**事实**：调度器内唯一落盘路径是 RETRY 重排程（`persistStateChange`，Fix 6 接入）。所有终态迁移（ACKED / DEAD_LETTERED / EXPIRED）只做 `intentStore.update`（内存）+ 经 observer 移除 `locationIndex`，**从不写入 PHTW**。

**后果**：
1. 崩溃重启后，已成功投递并 ACK 的 Intent 在磁盘上的最新记录仍是 SCHEDULED；`WheelRecovery` 走 overdue 分支将其改写为 EXPIRED/DEAD_LETTERED（依 `expiredAction`）——**磁盘权威记录了从未发生的结局**，审计/查询语义错误。
2. 当前"不重复投递"完全依赖一条时序偶然性：投递必发生在 executeAt 之后 ⇒ 崩溃时该记录必然 overdue ⇒ 被 overdue 分支兜底。**这不是显式不变量，是运气。**一旦 overdue 策略未来调整为"补投"，所有崩溃前已 ACK 的 Intent 将全体重复投递。

**修复**：终态分支统一经 `stateChangePersister` 落盘（复用 RETRY 已验证的通道，改动约 5 行），并补崩溃恢复测试：ACKED/DEAD_LETTERED 重启后必须保持原终态而非被改写为 EXPIRED。

### P1-2　调度所有权的 limbo 裂缝：scan 摘除期 × fireNow/reschedule 竞态 → 重复投递（I1 破缺）

**位置**：`BucketGroup.scanDue`（先 `buckets.remove(bucketKey)` 整体摘除，再逐个 `intentIndex.remove(id, bucketKey)`，行 149-165）× `IntentCommandService.fireNow`（行 531-540）/ `updateIntent` 改期路径。

**精确交错**：
1. scan 线程摘除桶 M（含 intent X），尚未迭代到 X；
2. fireNow 线程（持 synchronized(X)）：`removeFromSchedule` 找不到 X（既不在桶 map 也无法被索引扫描命中）→ `setExecuteAt(now)` → 落盘 → `restore` 重新入桶：同时间窗口 ⇒ **同数值 bucketKey** 的新 map M2，`intentIndex[X]=bucketKey`；
3. scan 线程迭代到 X：`intentIndex.remove(X, bucketKey)` 条件删除**成功**（数值相同，无法区分新旧注册）→ X 进入 due 列表 → 投递 #1；
4. 下一扫描周期，M2 中的 X 再次被取出 → **投递 #2**。且 M2 的索引项已被误删，后续 cancel 无法摘除 X（可能第 3 次投递）。

**评估**：窗口 = scan 排空一个桶的时长（微秒至毫秒级，桶越大窗口越大），概率低；但后果恰是本系统最着力防御的失败模式（重复投递）。现有 BugGhostDeliveryTest 等防护的是 promote/cancel 路径，此裂缝无测试覆盖。updateIntent 改期路径同构。

**修复方向**：将 `intentIndex` 升级为唯一认领账本（落实 I4）——scan 处理某 intent 前必须以"成功摘除其索引项"为认领凭据；重注册须携带可区分代际（如 (bucketKey, epoch) 或单调 claim 版本号），使步骤 3 的条件删除对"新注册"失败。并写确定性复现测试（扫描线程在桶摘除后阻塞，注入 fireNow）。

### P1-3　取消不保证不投递：语义必须在契约层显式化

cancel 与在途投递的竞争已被 `synchronized(intent)` + 状态机正确收口（`transitionTo(DUE)` from CANCELED 抛 ISE，记录保持 CANCELED）——**记录是一致的，但异步投递已完成的事件已到达下游**。这是 at-least-once + 异步投递的固有语义，本身不是问题；问题是文档未将其写成契约。使用方会合理误以为"cancel 成功 = 事件不会发生"。

**修复**：在 `cancelIntent` javadoc、`DeliveryHandler` 契约与 ARCHITECTURE.md 明确：cancel 是 best-effort，在途投递不可撤销；下游须具备幂等能力。纯文档改动。

## 3. 生命周期与运维

| # | 发现 | 位置 | 说明 |
|---|------|------|------|
| P1-4 | **热 store 终态永不驱逐** | `ConcurrentIntentStore` | `intents` map 单调增长：ACKED/CANCELED/EXPIRED 永驻内存直到重启，长跑内存线性上涨。需终态延迟驱逐；注意与幂等记录 24h 窗口耦合（`delete` 连带删幂等记录，驱逐后幂等语义需决策） |
| P2-1 | **running 闸门开启过早** | `LoomqEngine.start()` 行 215 | 先置 running=true 再做恢复；恢复窗口内的 DURABLE createIntent 必走 10s awaitCommit 超时 + inline force 兜底。应将放行移到全部子系统启动之后 |
| P2-2 | **"冷 Intent 不占内存"只对了一半** | `TailIndex.byId/byExecuteAt` + `PromotionDaemon.cohorts` + `IntentLocationIndex` | >30d tail Intent 的三处内存镜像全量常驻。远期海量调度场景下内存模型与 §3.3 文档承诺不符，应写明或外置冷索引 |
| P2-3 | **桶删除与在途写的理论竞态** | `BucketReclaimer.deleteBucket` vs `WheelStore.put` | put 持有已从 wheels 摘除的 Bucket 对象时，写入落在已删文件的 mmap 上（共享 Arena 仍存活），DURABLE 静默失守。触发条件苛刻（>retention 的过期 pending Intent 的冷取消），建议 per-bucket 写引用计数或 reclaim 前复查 |

## 4. 设计一致性与 API

| # | 发现 | 位置 | 说明 |
|---|------|------|------|
| P2-4 | **`Intent.ackLevel` 是死字段** | `Intent.java:89` / `IntentCommandService.resolveWalMode` | resolveWalMode 只认参数 ackMode / `intent.walMode` / 档位默认，从不读 `intent.ackLevel`。一个 Intent 上并存两套 durability 表述且其一无效，是 API 陷阱；删除或接线 |
| P2-5 | **只读视图漏水** | `ReadOnlyIntentStoreView.java:31` / `LoomqEngine.getIntentStoreInternal()` | 视图转发 `findByIdInternal`（可变活对象），外部可绕过命令服务直接 mutate 调度中对象；`getIntentStoreInternal()` 为 public。视图的只读承诺不成立 |
| P2-6 | **内核边界零校验** | `IntentCommandService.createIntent` | `IntentValidator` 存在但未接入：null/past executeAt → NPE；deadline < executeAt 可入库；>24B intentId 到 SlotCodec 才以 SlotOverflowException 爆炸。校验应在内核入口 |
| P2-7 | **单意图消费循环 acquire-then-poll 顺序倒置** | `PrecisionScheduler.runSingleIntentConsumer` | 批量循环是正确的 drain-then-acquire；单发循环先取 permit 再 poll——空队列时 permit 空转，permit 被借用耗尽时消费者阻塞在 acquire 上制造自我背压 |
| P2-8 | **createIntents 以首元素 walMode 代表全批** | `IntentCommandService.java:234` | 混合持久化语义的批次被静默归一。文档化或按元素分组 |
| P2-9 | **DeliveryHandler 异步契约无强制** | `PrecisionScheduler` 消费循环直接调用 `deliverAsync` | 同步阻塞实现会持 permit 占住消费 VT。可接受（SPI 契约），但应显式声明违规代价或在消费侧统一 submit 兜底 |

## 5. 卫生与文档漂移

- **文档滞后于代码**：CLAUDE.md / ARCHITECTURE.md §10 称"桶从不删除（无删除实现）""ResizableSemaphore.resize 未接线""MetricsCollector 全局单例"——实际 `BucketReclaimer` 已实现并在引擎接线、resize 已整体删除（类内注释已更新）、MetricsCollector 已改为引擎注入。架构债清单需要重刷。
- **死代码**（外围审阅，关键项已抽验）：retry 全包（4 文件）、SimpleYamlConfigLoader（335 行）、IdGenerator、IntentValidator、IntentMetricsRegistry、HealthNarrator、TimelineService 在主代码零接线，多指向已拆除的 HTTP 适配层。其中 **HealthNarrator 内含真 bug**：`HealthNarrator.java:34` 每次 `new LoomQMetrics()` 新建空 `PipelineMetricsRegistry`（LoomQMetrics.java:22），投递指标恒零/成功率恒 100%——死代码中的烂代码，建议整批删除而非修补。另存在**两个手写 YAML 解析器**（SimpleYamlConfigLoader 与 LoomqEngineFactory.parseSimpleYaml）。
- 命名残留：`MetricsCollector.recordWebhookLatency`（内核无 HTTP）；CohortManager 类注释引用 "DeepSeek V4 CSA"（营销式注释，建议改为机制本身的因果说明）；`WheelConfig.fromProperties` 未暴露 bucketRetentionMs / compactionThresholdBytes。

## 6. 第一性原理总评：复杂度预算已超支

机制清单——五档精度 × Arrow 借用 × AdapTBF × cohort 唤醒 × 冷热分层 × tail 尾区 × 桶回收 × group-commit——已经超过"问题本质"所需的最小集。每个机制都在解决真实问题（benchmark 与 Bug*Test 系列为证），但：

1. **ARCHITECTURE.md 只记录了 how，没有记录 why-not-simpler。** 建议补"机制必要性论证"一节：每个机制必须回答"去掉它会破坏哪条不变量或哪项 SLO"。答不上来的应继续删减（resize 被删除已证明这条路径可行）。
2. **不变量应当显式化、集中化。** I1-I4 目前分散在几十个方法的注释里各自维护，P1-1/P1-2 正是"不变量无人总负责"的产物。建议在 PrecisionScheduler/IntentCommandService 类注释中集中声明不变量清单，每个公开方法注明自己维护哪几条。
3. **复杂度的上限不是能实现多少机制，而是能同时维持多少不变量为真。** 当前测试策略（故障注入优先）是对的，应把它升级为不变量驱动：每条不变量至少一个确定性破坏测试。

## 7. 优先级行动清单

1. **P1-1 终态落盘**——改动约 5 行 + 1 个恢复测试，语义补全，收益最大。
2. **P1-2 scan limbo 竞态修复**（intentIndex 认领账本 + 代际区分）+ 确定性复现测试。
3. **P1-4 热 store 终态驱逐策略**（含幂等语义决策）。
4. **P1-3 cancel 语义契约化**——纯文档，半天。
5. **死代码清除 + 文档同步**（§5 全项）——降低后续维护者认知税，也消除"死代码里的烂代码被误用"的风险。
6. P2 批次：running 闸门后移、ackLevel 死字段、只读视图堵漏、内核入口校验接线、单发循环顺序对齐批量循环。
