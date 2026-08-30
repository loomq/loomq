# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LoomQ is a durable time kernel for distributed systems. It schedules, persists, and delivers future events called **Intent**s. Built on Java 25 Virtual Threads. Single module: `loomq-core` (embeddable, HTTP-free kernel).

## Build & Run Commands

```bash
# Build
mvn clean package              # full build with tests
mvn clean package -DskipTests  # fast build, skip tests
make build / make build-fast   # Makefile shortcuts

# Formatting (Spotless — enforced in CI; import ordering + unused import removal)
make format                    # apply formatting
make check-format              # verify formatting (same as CI gate)

# Test (Maven Surefire profiles with JUnit 5 tags)
mvn test                       # default: excludes benchmark/slow/integration
mvn test -Pfast-tests          # same as default
mvn test -Pslow-tests          # @Tag("slow") only
mvn test -Pfull-tests          # everything including slow/benchmark
mvn test -Dtest=ClassName      # single test class
mvn test -Dtest=ClassName#methodName  # single test method

# Run benchmark suite (MD reports)
benchmark\scripts\benchmark.ps1          # Windows full suite
benchmark\scripts\benchmark.ps1 -Quick   # quick validation
./benchmark/scripts/benchmark.sh         # Linux/macOS
./benchmark/scripts/benchmark.sh --quick # quick validation

# Pre-push gate (same checks CI runs)
make check                     # check-format + test
```

### JUnit 5 Tag System

Tests are categorized with `@Tag` annotations. Maven Surefire uses `groups`/`excludedGroups` properties to include/exclude:

| Tag | Maven Profile | What |
|-----|--------------|------|
| *(none)* | default / `fast-tests` | Fast unit tests, always run |
| `slow` | `slow-tests` | 慢速竞态/恢复/背压测试(类级与方法级 `@Tag("slow")` 混合,如 PrecisionSchedulerTest、HotColdStraddleTest、LoomqEnginePhtwRecoveryTest;完整清单以 `mvn test -Pslow-tests` 实际运行为准) |
| `integration` | `integration-tests` | Engine-level tests (mutation isolation, lock-free dispatch, recovery) |
| `benchmark` | (included in `full-tests`) | Performance benchmarks |

## Architecture

```
loomq-core (embeddable kernel, zero HTTP/JSON deps)
    ├── LoomqEngine           — builder-pattern entry point
    ├── PrecisionScheduler    — facade entry point(门面:生命周期 + 调度入口 + 组装)
    │   ├── ScanCoordinator    — 事件驱动/fixed-rate 扫描 + 过期分频检查 + pause 语义
    │   ├── DispatchPipeline   — 档位级消费循环 + permit 跨档借用(AdapTBF,releasePermit/decrementBorrowed 配对单所有者)+ 背压状态
    │   ├── SettlementEngine   — 结算/重试/死信/过期终态化 + I5 collect-then-defer 收口(DeferredOutcome/flushOutcome)
    │   ├── StatePersistence   — StateChangeSink 包装:锁内 put / 锁外 awaitCommit;I6 容错单一收口(persistFailures)
    │   ├── RetryPolicy / ObserverNotifier / ExpiryIndex / DispatchLagTracker / InFlightCounters — 纯逻辑叶子组件
    │   ├── CohortManager      — CSA-style batched wakeup (replaces per-intent VT sleep)
    │   ├── BucketGroupManager — per-tier time-bucket storage
    │   └── ResizableSemaphore — extends Semaphore, cross-tier borrowing tracking
    ├── IntentStore           — in-memory hot-state store (ConcurrentIntentStore)
    ├── WheelStore            — persistent hierarchical timing wheel (4-tier mmap: sec/min/hour/day)
    ├── TailIndex             — durable run-file for intents beyond the day-wheel horizon (>30 days)
    ├── GroupCommitBarrier    — rendezvous msync daemon; DURABLE writers awaitCommit()
    ├── PromotionDaemon       — cold→hot cohort promotion (mirrors CohortManager; wakes at executeAt - WheelConfig.promotionLeadMs(), default 60s)
    ├── IntentLocationIndex   — intentId→SlotLocation index for cold cancel/reschedule/fireNow
    ├── WheelRecovery         — scan-based recovery on restart (replaces snapshot+WAL replay)
    └── SPI interfaces        — DeliveryHandler, CallbackHandler, IntentObserver, RedeliveryDecider
```

**Intent lifecycle:** CREATED → SCHEDULED → DUE → DISPATCHING → DELIVERED → ACKED (branches: CANCELED, EXPIRED, DEAD_LETTERED)

**Four precision tiers:** ULTRA(10ms, 200 slots), FAST(50ms, 150 slots), STANDARD(500ms, 50 slots), MILLI(1ms) —— `ULTRA`/`MILLI` 事件驱动自适应扫描,`FAST`/`STANDARD` 固定轮询;`MILLI` cohort 旁路直插桶,毫秒级触发.

## Key Design Decisions

- **"Intent" is the public model** — older docs/code may use legacy terminology; always use "Intent" in new code.
- **Core has zero HTTP/JSON dependencies** — `loomq-core` depends only on `slf4j-api` at compile scope.
- **DeliveryHandler SPI** — the scheduler in core delegates delivery through this interface. Embedders supply their own.
- **Virtual threads everywhere** — `Executors.newVirtualThreadPerTaskExecutor()` for batch consumers; no traditional thread pool tuning.
- **Cohort-based wakeup (CSA-inspired)** — intents with delay > precision window are grouped by cohort key; one daemon thread wakes thousands, replacing per-intent VT sleep.
- **Arrow cross-tier borrowing** — when a tier's semaphore is full, consumers borrow slots from lower-priority tiers via `tryAcquire(100ms)`. AdapTBF bounds lending to 50% of a tier's slots to prevent starvation.
- **ResizableSemaphore extends Semaphore** — zero-overhead acquire/tryAcquire (inherited); no overridden release() (resize was removed as dead code). Tracks `currentMax` and `borrowedCount` per tier.跨档借用的 permit 释放统一走 `releasePermit` 配对 `decrementBorrowed`,杜绝借用计数泄漏(全部在 DispatchPipeline 单所有者内)。
- **Persistent Hierarchical Timing Wheel (PHTW)** — durability lives in a 4-tier mmap wheel (sec/min/hour/day) plus `TailIndex` (run-file for intents beyond the day-wheel horizon, >30 days). `WheelStore` 非终态状态变更 append,终态经 `persistTerminalInPlace` 原地覆写 + `reclaimTerminal` 单槽回收(free-list 复用);recovery 按 max revision per intentId 去重。`BucketReclaimer` 按窗口过期回收桶文件。**单槽 compaction**：终态槽不残留——终态经 `persistTerminalInPlace` 原地覆写 + `reclaimTerminal` 清空并回收入 free-list 复用，桶容量 ∝ 活跃在途而非吞吐史；重排程（RETRY/改期）过的多槽 Intent 保留终态槽作 tombstone（不回收），靠 recovery max-revision 去重抑制跨桶 stale sibling。桶满（活跃在途 > 桶容量）走溢出链 spill 到下一层更粗档（SEC→MIN→HOUR→DAY），突破单桶硬顶；仅当整条链都满（DAY 满）才抛 `SlotOverflowException` 兜底——compaction 后需同一自然日内活跃在途超桶容量，实际不可达。
- **Group-commit durability** — `GroupCommitBarrier` runs a rendezvous msync daemon; `DURABLE` writers `awaitCommit()` until a force covering their write completes. `ASYNC` returns after mmap (crash window); state-change ops (update/cancel/fireNow) hardcode `DURABLE`. 重试重排程亦 DURABLE 落盘(经 `StateChangeSink` 钩子;stale 时经守卫跳过,磁盘保留冷写者最新态——round 15 F2/F3)。慢盘超时走单飞行内联 force(平台线程,避免 VT pin)。**I6 容错**：`persistStateChange` 吞掉持久化失败(如 SEC 桶 `SlotOverflowException`)并记录 `persistFailures`,调度流程继续——`onDelivered` 不依赖终态落盘成功,避免已投递 intent 的通知被吞、槽位泄漏、引擎死锁(I6 容错单一收口见 `StatePersistence`)。代价:终态未落盘时崩溃恢复可能按旧 revision 重投。
- **Cold→hot promotion** — `PromotionDaemon` registers intents due beyond `WheelConfig.hotBoundaryMs()` (default 60min, create/recovery hot threshold) as cohorts, mirroring `CohortManager`; on wake (at `executeAt - WheelConfig.promotionLeadMs()`, default 60s) it loads the slot into memory and hands off to the scheduler. `IntentLocationIndex` (intentId→`SlotLocation`) enables **cold cancel/cold reschedule/cold fireNow**(round 13/14:updateIntent 对冷 Intent 走 updateCold,fireNow 对冷 Intent 走 fireNowCold——per-id 冷锁(WheelPersistence 簿记缝共享;round 15 F2 起为**既有 Intent 的所有状态变更持久化写者**的序列化点——热命令写者 updateIntent/fireNow/cancelIntent 写槽前持冷锁复核 revision,stale 热副本委托冷路径 fresh 重放,投递重试路径走 `persistStateChangePutOnly`(round 15 终审 F6 起为单一守卫门面);create 主写入路径(W8)例外不入冷锁,其 hasActiveDuplicate 活跃副本预检为既有守卫域)+ persistToWheel 写协议 + cohort 安全网;冷 fireNow 把 executeAt 改写为 now 后立即热载投递,不跑 IntentValidator 镜像热路径)。promote↔cold-command(cancelCold/updateCold/fireNowCold)经 ColdHotReconciler 单一权威实现双向清理收口 TOCTOU。
- **IntentStore is hot-state only** — `ConcurrentIntentStore` holds the in-memory hot window (≤`WheelConfig.hotBoundaryMs()`, default 60min); the wheel is the durable authority. Use `upsert()` for current-state writes.
- **Recovery overdue 语义** — `WheelRecovery` 对停机窗口期间到期的 Intent 不再静默丢弃:按 `ExpiredAction` 置 EXPIRED/DEAD_LETTERED 终态并持久化终态 revision,下次重启被 terminal 跳过(不补投,避免对下游产生过时事件)。
- **枚举序即持久化序** — `PrecisionTier` 新增档必须追加末尾,禁止插入重排(`SlotCodec` 按 ordinal 持久化)。v0.9.x 精简(6→4)为一次性 ordinal 断裂,断裂前 wheel 数据目录必须清空。
- **事件驱动扫描 + 信号驱动消费** — `ScanCoordinator.adaptiveScanLoop` 按 tier 事件驱动触发扫描(空闲时休眠,事件到来即唤醒),替代固定轮询;`MILLI` 档走 cohort 旁路直插桶,consumer 由信号唤醒而非批量睡眠,使 1ms 级触发可用而空闲 CPU 不劣化。

## CI

GitHub Actions (`.github/workflows/ci.yml`): Oracle JDK 25. Jobs: `format-check` → `fast-tests`, `slow-tests`, `integration-tests`. On push to main: `package`. Use `make check` locally to simulate the CI gate.

## Language

Documentation and configuration guides are written in Chinese (中文). Code comments and commit messages may be in Chinese or English.
