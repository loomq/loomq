# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

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

# Run benchmark suite (Excel + MD reports)
benchmark\benchmark.bat                  # Windows full suite
benchmark\benchmark.bat --quick          # quick validation
benchmark\benchmark.bat --stress         # full + stress sweep
./benchmark/scripts/benchmark.sh         # Linux/macOS

# Pre-push gate (same checks CI runs)
make check                     # check-format + test
```

### JUnit 5 Tag System

Tests are categorized with `@Tag` annotations. Maven Surefire uses `groups`/`excludedGroups` properties to include/exclude:

| Tag | Maven Profile | What |
|-----|--------------|------|
| *(none)* | default / `fast-tests` | Fast unit tests, always run |
| `slow` | `slow-tests` | PrecisionSchedulerTest, LoomqEnginePhtwRecoveryTest |
| `integration` | `integration-tests` | Engine-level tests (mutation isolation, lock-free dispatch, recovery) |
| `benchmark` | (included in `full-tests`) | Performance benchmarks |

## Architecture

```
loomq-core (embeddable kernel, zero HTTP/JSON deps)
    ├── LoomqEngine           — builder-pattern entry point
    ├── PrecisionScheduler    — time-wheel buckets, per-tier scan + batch consumers
    │   ├── CohortManager     — CSA-style batched wakeup (replaces per-intent VT sleep)
    │   ├── BucketGroupManager — per-tier time-bucket storage
    │   └── ResizableSemaphore — extends Semaphore, cross-tier borrowing tracking
    ├── IntentStore           — in-memory hot-state store (ConcurrentIntentStore)
    ├── WheelStore            — persistent hierarchical timing wheel (4-tier mmap: sec/min/hour/day)
    ├── TailIndex             — durable run-file for intents beyond the day-wheel horizon (>30 days)
    ├── GroupCommitBarrier    — rendezvous msync daemon; DURABLE writers awaitCommit()
    ├── PromotionDaemon       — cold→hot cohort promotion (mirrors CohortManager; wakes at executeAt - PROMOTION_LEAD_MS, default 60s)
    ├── IntentLocationIndex   — intentId→SlotLocation index for cold cancel/reschedule
    ├── WheelRecovery         — scan-based recovery on restart (replaces snapshot+WAL replay)
    └── SPI interfaces        — DeliveryHandler, CallbackHandler, IntentObserver, RedeliveryDecider
```

**Intent lifecycle:** CREATED → SCHEDULED → DUE → DISPATCHING → DELIVERED → ACKED (branches: CANCELLED, EXPIRED, DEAD_LETTERED)

**Six precision tiers:** ULTRA(10ms, 200 slots), FAST(50ms, 150 slots), HIGH(100ms, 50 slots), STANDARD(500ms, 50 slots), ECONOMY(1000ms, 50 slots), MILLI(1ms) —— 事件驱动扫描,毫秒级触发(cohort 旁路直插桶).

## Key Design Decisions

- **"Intent" is the public model** — older docs/code may use legacy terminology; always use "Intent" in new code.
- **Core has zero HTTP/JSON dependencies** — `loomq-core` depends only on `slf4j-api` at compile scope.
- **DeliveryHandler SPI** — the scheduler in core delegates delivery through this interface. Embedders supply their own.
- **Virtual threads everywhere** — `Executors.newVirtualThreadPerTaskExecutor()` for batch consumers; no traditional thread pool tuning.
- **Cohort-based wakeup (CSA-inspired)** — intents with delay > precision window are grouped by cohort key; one daemon thread wakes thousands, replacing per-intent VT sleep.
- **Arrow cross-tier borrowing** — when a tier's semaphore is full, consumers borrow slots from lower-priority tiers via `tryAcquire(100ms)`. AdapTBF bounds lending to 50% of a tier's slots to prevent starvation.
- **ResizableSemaphore extends Semaphore** — zero-overhead acquire/tryAcquire (inherited); no overridden release() (resize was removed as dead code). Tracks `currentMax` and `borrowedCount` per tier.跨档借用的 permit 释放统一走 `releasePermit` 配对 `decrementBorrowed`,杜绝借用计数泄漏。
- **Persistent Hierarchical Timing Wheel (PHTW)** — durability lives in a 4-tier mmap wheel (sec/min/hour/day) plus `TailIndex` (run-file for intents beyond the day-wheel horizon, >30 days). `WheelStore` is append-only; recovery dedups by max revision per intentId. `BucketReclaimer` 按窗口过期回收桶文件;无 compaction(旧槽残留由 recovery 去重)。桶满(1024 槽)抛 `SlotOverflowException`。
- **Group-commit durability** — `GroupCommitBarrier` runs a rendezvous msync daemon; `DURABLE` writers `awaitCommit()` until a force covering their write completes. `ASYNC` returns after mmap (crash window); state-change ops (update/cancel/fireNow) hardcode `DURABLE`. 重试重排程亦 DURABLE 落盘(经 `stateChangePersister` 钩子)。慢盘超时走单飞行内联 force(平台线程,避免 VT pin)。
- **Cold→hot promotion** — `PromotionDaemon` registers intents due beyond `WheelConfig.hotBoundaryMs()` (default 60min, create/recovery hot threshold) as cohorts, mirroring `CohortManager`; on wake (at `executeAt - WheelConfig.promotionLeadMs()`, default 60s) it loads the slot into memory and hands off to the scheduler. `IntentLocationIndex` (intentId→`SlotLocation`) enables **cold cancel**(冷改期/冷 fireNow 未实现——`updateIntent`/`fireNow` 仅作用于热内存态)。promote↔cancelCold 用双向清理收口 TOCTOU。
- **IntentStore is hot-state only** — `ConcurrentIntentStore` holds the in-memory hot window (≤`WheelConfig.hotBoundaryMs()`, default 60min); the wheel is the durable authority. Use `upsert()` for current-state writes.
- **Recovery overdue 语义** — `WheelRecovery` 对停机窗口期间到期的 Intent 不再静默丢弃:按 `ExpiredAction` 置 EXPIRED/DEAD_LETTERED 终态并持久化终态 revision,下次重启被 terminal 跳过(不补投,避免对下游产生过时事件)。
- **枚举序即持久化序** — `PrecisionTier` 新增档必须追加末尾,禁止插入重排(`SlotCodec` 按 ordinal 持久化)。
- **事件驱动扫描 + 信号驱动消费** — `PrecisionScheduler.adaptiveScanLoop` 按 tier 事件驱动触发扫描(空闲时休眠,事件到来即唤醒),替代固定轮询;`MILLI` 档走 cohort 旁路直插桶,consumer 由信号唤醒而非批量睡眠,使 1ms 级触发可用而空闲 CPU 不劣化。

## CI

GitHub Actions (`.github/workflows/ci.yml`): Oracle JDK 25. Jobs: `format-check` → `fast-tests`, `slow-tests`, `integration-tests`. On push to main: `package`. Use `make check` locally to simulate the CI gate.

## Language

Documentation and configuration guides are written in Chinese (中文). Code comments and commit messages may be in Chinese or English.
