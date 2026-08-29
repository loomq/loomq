package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryContext;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.IntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 投递结算引擎:终态化/重试/死信/过期 + 结算任务提交。
 * 持有 I2/I3(revision 递增 + 终态落盘)与 I5(锁内快照、锁外派发)的执行主体;
 * 消费循环经 {@link DeliverySettlement} 回调进入本类。
 */
final class SettlementEngine implements DeliverySettlement {

    private static final Logger logger = LoggerFactory.getLogger(SettlementEngine.class);

    private final IntentStore intentStore;
    private final StatePersistence persistence;
    private final ObserverNotifier notifier;
    private final RetryPolicy retryPolicy;
    private final ExpiryIndex expiryIndex;
    private final DispatchLagTracker lagTracker;
    private final InFlightCounters inFlightCounters;
    private final MetricsCollector metrics;
    private final IntentTraceStore traceStore;
    private final RedeliveryDecider redeliveryDecider;
    private final Rescheduler rescheduler; // 锁外重排程端口(schedule() 经此触发,不依赖门面整体)

    private volatile ExecutorService executor; // bindExecutor 在 start() 注入;消费者仅在绑定后运行,提交路径不可达 null
    private final ConcurrentLinkedQueue<String> finalizeExceptionSamples = new ConcurrentLinkedQueue<>();

    SettlementEngine(IntentStore intentStore, StatePersistence persistence, ObserverNotifier notifier,
                     RetryPolicy retryPolicy, ExpiryIndex expiryIndex, DispatchLagTracker lagTracker,
                     InFlightCounters inFlightCounters, MetricsCollector metrics, IntentTraceStore traceStore,
                     RedeliveryDecider redeliveryDecider, Rescheduler rescheduler) {
        this.intentStore = intentStore;
        this.persistence = persistence;
        this.notifier = notifier;
        this.retryPolicy = retryPolicy;
        this.expiryIndex = expiryIndex;
        this.lagTracker = lagTracker;
        this.inFlightCounters = inFlightCounters;
        this.metrics = metrics;
        this.traceStore = traceStore;
        this.redeliveryDecider = redeliveryDecider;
        this.rescheduler = rescheduler;
    }

    /** 结算任务执行器(sharedExecutor);start() 时由门面注入。 */
    void bindExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    // ---- DeliverySettlement 实现 ----

    @Override
    public void onCompleted(Intent intent, PrecisionTier tier, DeliveryResult result, Throwable error) {
        submitFinalize(intent, tier, () -> {
            if (error != null) {
                handleDeliveryException(intent, tier, error);
            } else {
                finalizeIntent(intent, tier, result);
            }
        });
    }

    @Override
    public void onException(Intent intent, PrecisionTier tier, Throwable error) {
        submitFinalize(intent, tier, () -> handleDeliveryException(intent, tier, error));
    }

    @Override
    public void onExpiredInFlight(Intent intent, PrecisionTier tier) {
        // 消费循环过期闸门路径(投递前最后一道闸):锁外终态化
        handleExpired(intent);
    }

    // ---- 以下为结算逻辑(终态化/重试/死信/过期) ----

    /**
     * 异步投递异常处理。
     *
     * 注意：onDeliveryFailed 通知在 retry/dead-letter 决策之前触发。
     * 观察器接收到的是派发时刻的防御性快照，其状态反映投递失败时的值。
     * 观察器不应依赖快照状态来推断调度器的后续决策。
     */
    private void handleDeliveryException(Intent intent, PrecisionTier tier, Throwable ex) {
        // I5: 快照在锁内取，dispatch 在锁外
        final Intent snapshot;
        synchronized (intent) {
            snapshot = intent.copy();
        }
        notifier.notifyObservers(o -> o.onDeliveryFailed(snapshot, ex));
        traceStore.recordFailure(intent.getIntentId(), ex.getMessage(), null);
        if (ex instanceof TimeoutException) {
            logger.warn("Delivery timeout for intent {}", intent.getIntentId());
        } else {
            logger.error("Delivery exception for intent {}: {}", intent.getIntentId(), ex.getMessage(), ex);
        }
        // RedeliveryDecider 判定不可重投(永久失败,如业务 4xx/非法请求)→ 直接终态
        // DEAD_LETTERED;可重投 → 走既有重试链(attempts >= maxAttempts 终态)。
        // 默认 decider(DefaultRedeliveryDecider)对异常恒可重投。
        handleDeliveryFailure(intent, !shouldRedeliver(intent, ex));
    }

    /**
     * 用 RedeliveryDecider 判定异常是否值得重投。
     *
     * <p>decider 抛异常时保守按"可重投"处理(与无 decider 的既有行为一致),不因用户 SPI
     * 异常阻断调度链。deliveryId 用 lastDeliveryId(缺省回退 intentId)仅作上下文标识。</p>
     */
    private boolean shouldRedeliver(Intent intent, Throwable ex) {
        DeliveryContext ctx = new DeliveryContext(
            intent.getLastDeliveryId() != null ? intent.getLastDeliveryId() : intent.getIntentId(),
            intent.getIntentId(),
            // attempt 语义为"本次失败投递是第几次尝试"(从 1 开始)。此时 incrementAttempts()
            // 尚未执行(它在 handleDeliveryFailure 内),故当前 attempts 是上一次的值,需 +1。
            intent.getAttempts() + 1);
        ctx.markFailure(ex);
        try {
            return redeliveryDecider.shouldRedeliver(ctx);
        } catch (Exception deciderEx) {
            logger.error("RedeliveryDecider threw for intent {}; defaulting to redeliver",
                intent.getIntentId(), deciderEx);
            return true;
        }
    }

    /**
     * 终态处理——根据异步投递结果更新状态并持久化。
     *
     * 在结算执行器中执行。单次 intentStore.update() 写入终态。
     *
     * @implNote 维护 I2/I3：终态落盘（persistStateChange）确保磁盘权威记录。
     */
    private void finalizeIntent(Intent intent, PrecisionTier tier, DeliveryResult result) {
        long startTime = System.nanoTime();
        // I5: collect-then-defer -- 锁内取快照 + 收集 deferred action，锁外派发
        DeferredOutcome outcome = new DeferredOutcome();
        try {
            synchronized (intent) {
                // 终态守卫：在途投递期间 intent 可能已被并发终态化——deadline 在飞行中
                // 越过（checkExpiredIntents → handleExpired 标 EXPIRED）或 cancel 竞态
                // （标 CANCELED）。终态胜者在途投递结果：直接跳过结算（观察器已由
                // onExpired/取消路径通知），否则 transitionTo(DUE) 从终态抛 ISE →
                // onDelivered 丢失 + ACK 未落盘 → 重启按旧 SCHEDULED 槽重复投递。
                if (intent.getStatus().isTerminal()) {
                    lagTracker.clear(intent.getIntentId());
                    return;
                }
                // 内存中状态转换（不持久化 — 终态才做一次 upsert）
                // 结算前奏容错:updater 无状态白名单可把 SCHEDULED 置为 DUE(状态机
                // 唯一可达的非终态迁移),故 DUE 起步时跳过 transitionTo(DUE)
                // (DUE→DUE 非法,抛 ISE 被吞)——直接推进 DISPATCHING 续链。
                if (intent.getStatus() != IntentStatus.DUE) {
                    intent.transitionTo(IntentStatus.DUE);
                }
                intent.transitionTo(IntentStatus.DISPATCHING);
                intent.incrementAttempts();

                // Trace: record delivered
                traceStore.recordDelivered(intent.getIntentId());

                final DeliveryResult finalResult = result != null ? result : DeliveryResult.RETRY;
                if (result == null) {
                    logger.warn("Delivery handler returned null for intent {}, treating as RETRY", intent.getIntentId());
                }

                switch (finalResult) {
                    case SUCCESS:
                        // DELIVERED→ACKED 双迁移与 recordAcked 内联于 SUCCESS 分支(状态机中间步,须先于收口)
                        intent.transitionTo(IntentStatus.DELIVERED);
                        intent.transitionTo(IntentStatus.ACKED);
                        // Trace: record acked
                        traceStore.recordAcked(intent.getIntentId());
                        settleToTerminal(intent, IntentStatus.ACKED,
                            "Intent {} delivered successfully", snap -> o -> o.onDelivered(snap, finalResult), outcome);
                        break;

                    case RETRY: {
                        // 与 handleDeliveryFailure(异常路径)对齐:RETRY 结果同样受 maxAttempts
                        // 约束——否则 handler 恒返回 RETRY 时无限重排程,绕过重试上限契约,
                        // 与异常路径(attempts >= maxAttempts → DEAD_LETTERED)语义不一致。
                        int maxAttempts = retryPolicy.maxAttempts(intent);
                        if (intent.getAttempts() >= maxAttempts) {
                            traceStore.recordFailure(intent.getIntentId(), "DEAD_LETTER", null);
                            // 与 DEAD_LETTER 分支同型收口(日志文案保留"after max attempts (RETRY result)")
                            settleToTerminal(intent, IntentStatus.DEAD_LETTERED,
                                "Intent dead-lettered after max attempts (RETRY result): id={}",
                                snap -> o -> o.onDeadLettered(snap), outcome);
                            break;
                        }
                        traceStore.recordFailure(intent.getIntentId(), "RETRY", null);
                        long oldExecuteAtMs = executeAtMs(intent);
                        scheduleRetryOrHonorReschedule(intent, tier, oldExecuteAtMs);
                        // I5: schedule() 移到锁外 (deferred)
                        outcome.markPersisted();
                        outcome.markReschedule();
                        break;
                    }

                    case DEAD_LETTER:
                        traceStore.recordFailure(intent.getIntentId(), "DEAD_LETTER", null);
                        settleToTerminal(intent, IntentStatus.DEAD_LETTERED,
                            "Intent {} dead-lettered", snap -> o -> o.onDeadLettered(snap), outcome);
                        break;

                    case EXPIRED:
                        settleToTerminal(intent, IntentStatus.EXPIRED,
                            "Intent {} expired", snap -> o -> o.onExpired(snap), outcome);
                        break;
                }
            }
            // 锁外收尾:awaitCommit → reclaimTerminal → 观察器通知 → 重排程(顺序固定)
            flushOutcome(outcome, intent);
        } finally {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            metrics.recordFinalizeDuration(durationMs);
            // 结算路径不再计数 intent_total(创建数统计在 createIntent/createIntents,HELP 语义)——重试不再虚增
        }
    }

    /**
     * 处理投递失败
     *
     * @param permanent true 表示 RedeliveryDecider 判定不可重投(永久失败),直接终态,
     *                  不再按 maxAttempts 反复重试
     * @implNote 维护 I2/I3：死信终态落盘；重试路径 persistStateChange 递增 revision。
     */
    private void handleDeliveryFailure(Intent intent, boolean permanent) {
        // I5: collect-then-defer -- 锁内取快照 + 收集 deferred action，锁外派发
        DeferredOutcome outcome = new DeferredOutcome();
        synchronized (intent) {
            // 终态守卫（同 finalizeIntent）：在途投递失败结算时 intent 可能已被
            // handleExpired/cancel 终态化——跳过重试/死信决策，终态保持。
            if (intent.getStatus().isTerminal()) {
                lagTracker.clear(intent.getIntentId());
                return;
            }
            int maxAttempts = retryPolicy.maxAttempts(intent);

            // 结算前奏容错(同 finalizeIntent):DUE 起步时跳过 transitionTo(DUE),
            // 直接推进 DISPATCHING。
            if (intent.getStatus() != IntentStatus.DUE) {
                intent.transitionTo(IntentStatus.DUE);
            }
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.incrementAttempts();

            if (permanent || intent.getAttempts() >= maxAttempts) {
                settleToTerminal(intent, IntentStatus.DEAD_LETTERED,
                    "Intent dead-lettered after max attempts: id={}", snap -> o -> o.onDeadLettered(snap), outcome);
            } else {
                long oldExecuteAtMs = executeAtMs(intent);
                scheduleRetryOrHonorReschedule(intent, intent.getPrecisionTier(), oldExecuteAtMs);
                // I5: schedule() 移到锁外 (deferred)
                outcome.markPersisted();
                outcome.markReschedule();
            }
        }
        // 锁外收尾:awaitCommit → reclaimTerminal → 观察器通知 → 重排程(顺序固定)
        flushOutcome(outcome, intent);
    }

    /**
     * 终态分支统一收口(锁内调用):状态迁移 + 内存镜像 + 终态覆写 + 索引摘除 + trace 更新 +
     * 日志 + 观察器快照,统一写入 outcome。调用方须持 intent 锁;锁外收尾由
     * {@link #flushOutcome} 完成。
     *
     * <p>ACKED 的 DELIVERED→ACKED 双迁移与 recordAcked 由调用方保留内联(状态机中间步);
     * 其余终态经 {@link #settleTerminal} 完成迁移。日志级别按 status 语义映射:
     * ACKED=debug / EXPIRED=info / DEAD_LETTERED=warn。</p>
     *
     * @param intent        已持锁的 Intent
     * @param status        目标终态(ACKED / DEAD_LETTERED / EXPIRED)
     * @param logMessage    SLF4J 日志消息(占位符填 intentId)
     * @param notifyFactory 观察器通知工厂(输入为锁内快照,输出观察器动作;observers 为空时跳过)
     * @param outcome       锁内收集结果(本方法填 persisted/terminalId/deferredNotify)
     */
    private void settleToTerminal(Intent intent, IntentStatus status, String logMessage,
                                  Function<Intent, Consumer<IntentObserver>> notifyFactory,
                                  DeferredOutcome outcome) {
        if (status == IntentStatus.ACKED) {
            // SUCCESS 分支已内联 DELIVERED→ACKED 双迁移 + recordAcked,此处仅补齐持久化序列
            // (updateStatus 不能省——recordAcked 只记 ackedAt,trace 状态推进靠 updateStatus)
            updateStoreBestEffort(intent);
            persistence.persistTerminal(intent);
            traceStore.updateStatus(intent.getIntentId(), status);
        } else {
            settleTerminal(intent, status);
        }
        expiryIndex.unindex(intent.getIntentId(), executeAtMs(intent));
        switch (status) {
            case ACKED -> logger.debug(logMessage, intent.getIntentId());
            case EXPIRED -> logger.info(logMessage, intent.getIntentId());
            default -> logger.warn(logMessage, intent.getIntentId());
        }
        outcome.markPersisted();
        outcome.markTerminal(intent.getIntentId());
        // observers 为空时跳过快照拷贝(与现行为一致)
        outcome.deferNotify(notifier.isEmpty() ? null : notifyFactory.apply(intent.copy()));
    }

    /**
     * 处理过期任务。
     *
     * synchronized(intent) 与 finalizeIntent 串行化:transitionTo 的 validate+set
     * 非原子,需锁内互斥防状态撕裂(EXPIRED 被 DUE 回跳等)。
     *
     * @implNote 维护 I2/I3:过期终态落盘,防止恢复时被改写。
     */
    void handleExpired(Intent intent) {
        // I5: collect-then-defer -- 锁内取快照，锁外派发
        DeferredOutcome outcome = new DeferredOutcome();
        synchronized (intent) {
            // 终态守卫:consumer 过期闸门与扫描线程的 checkExpiredIntents 可并发进入
            // handleExpired——锁内复查,竞态败者幂等跳过(终态已由先到者落盘+回收)。
            if (intent.getStatus().isTerminal()) {
                return;
            }
            expiryIndex.unindex(intent.getIntentId(), executeAtMs(intent));
            logger.info("Intent expired: id={}, deadline={}", intent.getIntentId(), intent.getDeadline());

            // expiredAction 可为 null(API 无守卫):switch(null) 抛 NPE——
            // 防御性默认 DISCARD(与 Intent 构造器/解码路径的默认语义一致)。
            IntentStatus terminalStatus = switch (intent.getExpiredAction() != null
                ? intent.getExpiredAction() : ExpiredAction.DISCARD) {
                case DISCARD -> IntentStatus.EXPIRED;
                case DEAD_LETTER -> IntentStatus.DEAD_LETTERED;
            };
            settleTerminal(intent, terminalStatus);
            outcome.markTerminal(intent.getIntentId());
            outcome.markPersisted();

            if (!notifier.isEmpty()) {
                final Intent snapshot = intent.copy();
                outcome.deferNotify(o -> o.onExpired(snapshot));
            }
        }
        // 锁外收尾:awaitCommit → reclaimTerminal → 观察器通知(顺序固定)
        flushOutcome(outcome, intent);
    }

    /**
     * 重试/改期结算收口:在途改期尊重 + backoff 重排程
     * (finalizeIntent RETRY 与 handleDeliveryFailure 共用)。调用方须持 intent 锁,
     * 并在返回后置 needReschedule/persisted。
     */
    private void scheduleRetryOrHonorReschedule(Intent intent, PrecisionTier tier, long oldExecuteAtMs) {
        // 在途改期尊重:投递期间用户改期会把活对象 executeAt 置为将来时刻并 DURABLE
        // 持久化(更新在重试路径确定生效)。判据:任何"将来时刻"必是用户改期——未被
        // 改期的在途 intent 的 executeAt 恒 ≤ now(scanDue 只摘到期条目),故无需精度
        // 窗口裕量;窗口只会制造死区:改期到 (now, now+window] 的 Intent 被 backoff 覆写。
        Instant now = Instant.now();
        if (intent.getExecuteAt().isAfter(now)) {
            intent.transitionTo(IntentStatus.SCHEDULED);
            updateStoreBestEffort(intent);
            // 重排程是新的调度承诺而非中间态——必须 DURABLE 落盘,否则崩溃恢复
            // 按旧 executeAt 槽恢复,重试链静默丢失。
            // (I6 容错下持久化失败仅记 persistFailures,不阻塞调度)
            persistence.persistStateChange(intent);
            expiryIndex.unindex(intent.getIntentId(), oldExecuteAtMs);
            logger.info("Honoring mid-flight reschedule for intent={}, executeAt={} (no backoff)",
                intent.getIntentId(), intent.getExecuteAt());
            return;
        }
        long delayMs = retryPolicy.backoffDelayMs(intent);
        logger.info("Scheduling redelivery for intent={}, attempt={}, delay={}ms",
            intent.getIntentId(), intent.getAttempts(), delayMs);
        intent.setExecuteAt(Instant.now().plusMillis(delayMs));
        intent.transitionTo(IntentStatus.SCHEDULED);
        updateStoreBestEffort(intent);
        // 同上:重排程必须 DURABLE 落盘
        persistence.persistStateChange(intent);
        expiryIndex.unindex(intent.getIntentId(), oldExecuteAtMs);
    }

    /**
     * 锁外结算收尾(顺序固定):awaitCommit → reclaimTerminal → 观察器通知 → 重排程。
     * 收敛三处 collect-then-defer 尾部样板;须在 synchronized(intent) 之外调用。
     */
    private void flushOutcome(DeferredOutcome outcome, Intent intent) {
        // 持久化等待移到锁外:VT 在此正常 unmount,避免在 synchronized 内 park 而 pin carrier
        if (outcome.hasPersisted()) {
            persistence.awaitCommit();
        }
        // 终态槽回收:须在 awaitCommit 之后,确保原地覆写已落盘再释放槽位
        String terminalId = outcome.terminalId();
        if (terminalId != null) {
            persistence.reclaimTerminal(terminalId);
        }
        // I5: 观察器通知在锁外派发
        Consumer<IntentObserver> deferredNotify = outcome.deferredNotify();
        if (deferredNotify != null) {
            notifier.notifyObservers(deferredNotify);
        }
        // I5: schedule() 在锁外调用(schedule() 有自己的 synchronized + 终态检查)
        if (outcome.hasReschedule()) {
            rescheduler.schedule(intent);
        }
    }

    /** 终态落盘样板:transition+store 更新+原地覆写+trace(过期/死信共用)。
     *  索引清理/日志/observers 派发由调用方按上下文处理(快照语义与 unindex 时机不同)。 */
    private void settleTerminal(Intent intent, IntentStatus status) {
        intent.transitionTo(status);
        updateStoreBestEffort(intent);
        persistence.persistTerminal(intent);              // 终态原地覆写（不追加）
        // 终态须反映到 trace,否则死信/过期 Intent 的 trace 停在 CREATED
        traceStore.updateStatus(intent.getIntentId(), status);
    }

    /**
     * 内存镜像更新 best-effort（I6 同构）：IntentStore.update 只是热态镜像，不是持久化
     * 权威；失败时不能阻断终态持久化/重排程/观察器通知，否则已投递 Intent 的 ACK 丢失、
     * 重启后按旧 SCHEDULED 槽重复投递。
     */
    private void updateStoreBestEffort(Intent intent) {
        try {
            intentStore.update(intent);
        } catch (Exception e) {
            persistence.countFailure();
            logger.error("intentStore.update failed for intent {}: {}", intent.getIntentId(), e.getMessage(), e);
            // 内存镜像更新失败时，用 save 做 best-effort 补偿，尽量让状态计数与 live 状态一致。
            try {
                intentStore.save(intent);
            } catch (Exception saveEx) {
                logger.warn("Fallback save after store.update failure also failed for intent {}: {}",
                    intent.getIntentId(), saveEx.getMessage(), saveEx);
            }
        }
    }

    /**
     * 把投递结算任务提交到结算执行器,统一处理 RejectedExecutionException,
     * 并在任务结束时 -1 在途计数。stop() 等在途归零后才关 executor,正常路径
     * 不应 reject;此处的兜底仅作防御,避免在途决策静默丢失。
     */
    private void submitFinalize(Intent intent, PrecisionTier tier, Runnable task) {
        Runnable guarded = () -> runFinalizeTask(intent, tier, task);
        ExecutorService ex = executor;
        if (ex == null) {
            // 防御:start() 前被调用(正常流程不可达,消费者仅在绑定后运行);原地执行保证结算决策不丢
            runFinalizeTask(intent, tier, task);
            return;
        }
        try {
            ex.submit(guarded);
        } catch (RejectedExecutionException e) {
            // stop() 排空窗口的 TOCTOU：consumer 越过 while(running) 后新入的在途投递，
            // 其结算提交会被已关闭的 executor 拒绝。原地执行保证 ACK/重试决策不丢——
            // 否则投递已成功的 intent 重启后按旧 SCHEDULED 槽重投（重复投递）。
            logger.warn("finalize executor rejected finalize for intent {}; running inline to preserve outcome",
                intent.getIntentId(), e);
            runFinalizeTask(intent, tier, task);
        }
    }

    /** 结算任务主体:try/catch/finally 包裹,结束时 -1 在途计数。 */
    private void runFinalizeTask(Intent intent, PrecisionTier tier, Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            metrics.incrementFinalizeTaskExceptions();
            if (finalizeExceptionSamples.size() < 20) {
                finalizeExceptionSamples.add(e.getClass().getSimpleName() + ": "
                    + (e.getMessage() != null ? e.getMessage() : "(null)"));
            }
            logger.error("Error in delivery callback for intent {}", intent.getIntentId(), e);
        } finally {
            inFlightCounters.decrement(tier);
        }
    }

    private static long executeAtMs(Intent intent) {
        return intent.getExecuteAt() != null ? intent.getExecuteAt().toEpochMilli() : 0L;
    }

    List<String> finalizeExceptionSamples() {
        return new ArrayList<>(finalizeExceptionSamples);
    }
}
