package com.loomq;

import com.loomq.application.command.IntentCommandService;
import com.loomq.application.recovery.WheelRecovery;
import com.loomq.application.recovery.WheelRecoveryReport;
import com.loomq.application.scheduler.BucketGroupManager;
import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.BucketReclaimer;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.CallbackHandler;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.IntentObserver;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import com.loomq.store.ReadOnlyIntentStoreView;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoomQ 核心引擎 - 嵌入式内核
 *
 * 纯 Java 实现，零外部依赖（仅 SLF4J API）。
 * 提供 Intent 队列的核心能力，通过 DeliveryHandler 投递 Intent。
 *
 * <p>持久化由持久化分层时间轮(PHTW)栈承担:{@link WheelStore}(四轮 mmap 槽位)+
 * {@link TailIndex}(超 day 视界 run 文件)+ {@link GroupCommitBarrier}(group-commit msync)+
 * {@link IntentLocationIndex}(intentId→槽位)+ {@link PromotionDaemon}(冷→热提升 cohort)+
 * {@link WheelRecovery}(重启扫描重建)。</p>
 *
 * @author loomq
 */
public class LoomqEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LoomqEngine.class);

    /**
     * 默认投递处理器:未配置 deliveryHandler 时使用,直接判为 DEAD_LETTER。
     * 使引擎开箱即用(嵌入式场景下调用方可能仅用于调度/持久化,不关心投递)。
     */
    private static final DeliveryHandler DEFAULT_DELIVERY_HANDLER = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER);

    // ========== 核心组件 ==========
    private final IntentStore intentStore;
    private final WheelStore wheelStore;
    private final TailIndex tailIndex;
    private final GroupCommitBarrier commitBarrier;
    private final IntentLocationIndex locationIndex;
    private final PromotionDaemon promotionDaemon;
    private final WheelRecovery wheelRecovery;
    private final MetricsCollector metricsCollector;
    private final PrecisionScheduler scheduler;
    private final IntentCommandService commandService;
    private final com.loomq.tracing.IntentTraceStore traceStore;
    private final BucketReclaimer bucketReclaimer;

    // ========== 观察器 ==========
    private final List<IntentObserver> observers = new CopyOnWriteArrayList<>();

    // ========== 回调机制 ==========
    private final Executor callbackExecutor;
    private final java.util.concurrent.ExecutorService operationExecutor;

    // ========== 状态 ==========
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicLong sequenceNumber = new AtomicLong(0);

    // ========== 配置 ==========
    private final Path dataDir;
    private final String nodeId;
    private final PrecisionTier defaultTier;
    private final boolean deliveryHandlerConfigured;
    /** tail run 文件 compaction 阈值(close 时触发,见 {@link #close()})。 */
    private final long compactionThresholdBytes;

    private LoomqEngine(Builder builder) {
        this.nodeId = builder.nodeId != null ? builder.nodeId : "default-node";
        this.defaultTier = builder.defaultTier;
        this.deliveryHandlerConfigured = builder.deliveryHandler != null;
        this.callbackExecutor = builder.callbackExecutor != null
            ? builder.callbackExecutor
            : Executors.newVirtualThreadPerTaskExecutor();
        this.operationExecutor = Executors.newVirtualThreadPerTaskExecutor();

        try {
            WheelConfig wheelConfig;
            if (builder.wheelConfig != null) {
                wheelConfig = builder.wheelConfig;
                if (builder.dataDir != null) {
                    logger.warn("Both wheelConfig and dataDir/walDir set on Builder; wheelConfig wins (its dataDir={})",
                        wheelConfig.dataDir());
                }
            } else {
                Path dir = builder.dataDir != null ? builder.dataDir : Path.of("./data");
                wheelConfig = WheelConfig.defaultConfig().withDataDir(dir.toString());
            }
            this.dataDir = Path.of(wheelConfig.dataDir());
            this.compactionThresholdBytes = wheelConfig.compactionThresholdBytes();
            Files.createDirectories(dataDir);

            // 初始化组件
            this.intentStore = builder.intentStore != null
                ? builder.intentStore
                : new ConcurrentIntentStore();
            this.wheelStore = new WheelStore(wheelConfig, System::currentTimeMillis);
            this.tailIndex = new TailIndex(dataDir, System::currentTimeMillis);
            // Spec B: 恢复时若 tail run 文件超过阈值则 compaction(无并发写入)
            tailIndex.compactIfNeeded(wheelConfig.compactionThresholdBytes());
            this.commitBarrier = new GroupCommitBarrier(
                wheelStore, tailIndex,
                wheelConfig.groupCommitIntervalMs(),
                wheelConfig.awaitCommitTimeoutMs());
            this.locationIndex = new IntentLocationIndex();
            this.metricsCollector = builder.metricsCollector != null ? builder.metricsCollector : new MetricsCollector();

            // 初始化调度器(未配置 deliveryHandler 时使用默认 DEAD_LETTER 处理器)
            DeliveryHandler deliveryHandler = builder.deliveryHandler != null
                ? builder.deliveryHandler
                : DEFAULT_DELIVERY_HANDLER;
            // R21: traceStore 单实例——调度器与命令服务共享(冷 create/取消的 trace 归命令服务管)
            this.traceStore = builder.intentTraceStore != null
                ? builder.intentTraceStore : new com.loomq.tracing.IntentTraceStore();
            this.scheduler = new PrecisionScheduler(
                intentStore,
                deliveryHandler,
                builder.redeliveryDecider,
                builder.precisionTierCatalog,
                metricsCollector,
                traceStore
            );

            // 初始化冷→热提升 daemon:到点把冷 Intent 从磁盘载入内存并调度
            this.promotionDaemon = new PromotionDaemon(wheelStore, tailIndex, locationIndex, System::currentTimeMillis, (intent, loc) -> {
                if (intentStore.findByIdInternal(intent.getIntentId()) == null) {
                    intentStore.upsert(intent);          // 幂等:已在内存则跳过
                    scheduler.schedule(intent);
                    // P1-2: promote↔cancelCold TOCTOU 收口(promote 侧)。promote 读槽→落地
                    // 期间若发生冷取消,索引已迁移到 CANCELED 槽或被移除。复核不匹配则回滚
                    // 热载,杜绝 ghost 投递。与 cancelCold 末尾的 store 复查形成双向清理,
                    // 确定性关闭竞态窗口(两可见动作 upsert 与 index.put 有全序,后到者必见先到者)。
                    SlotLocation after = locationIndex.get(intent.getIntentId());
                    if (after == null || !after.equals(loc)) {
                        scheduler.removeFromSchedule(intent);
                        intentStore.delete(intent.getIntentId());
                        logger.warn("Promotion rolled back for intent {} (raced with cold cancel)", intent.getIntentId());
                    }
                }
            }, wheelConfig.promotionLeadMs());

            this.wheelRecovery = new WheelRecovery(wheelStore, tailIndex, wheelConfig.hotBoundaryMs(), metricsCollector);

            this.commandService = new IntentCommandService(
                intentStore, scheduler,
                new IntentCommandService.PhtwStack(wheelStore, tailIndex, commitBarrier, locationIndex, promotionDaemon),
                metricsCollector, callbackExecutor, running, sequenceNumber,
                builder.callbackHandler,
                new IntentCommandService.CommandConfig(defaultTier, wheelConfig.groupCommitIntervalMs(),
                    wheelConfig.hotBoundaryMs(), builder.precisionTierCatalog),
                traceStore);

            // Fix 6: 把重试重排程的 DURABLE 落盘接到调度器,使崩溃恢复能看到新调度。
            scheduler.setStateChangeSink(new PrecisionScheduler.StateChangeSink() {
                @Override public void persist(Intent intent) { commandService.persistStateChangePutOnly(intent); }
                @Override public void persistTerminalInPlace(Intent intent) { commandService.persistTerminalInPlace(intent); }
                @Override public void awaitCommit() { commandService.awaitDurableCommit(); }
                @Override public void reclaimTerminal(String intentId) { commandService.reclaimTerminal(intentId); }
            });

            // Spec B: 终态 Intent 从 locationIndex 移除(桶回收依赖索引判断活跃桶)。
            // 调度器的 finalizeIntent/handleExpired/handleDeliveryFailure 经 observer 回调清理,
            // cancelIntent(热路径)和 cancelCold 在 IntentCommandService 内直接清理。
            scheduler.addObserver(new IntentObserver() {
                @Override public void onScheduled(Intent intent) { /* no-op */ }
                @Override public void onDelivered(Intent i, com.loomq.spi.DeliveryHandler.DeliveryResult r) {
                    locationIndex.remove(i.getIntentId());
                }
                @Override public void onDeadLettered(Intent i) {
                    locationIndex.remove(i.getIntentId());
                }
                @Override public void onExpired(Intent i) {
                    locationIndex.remove(i.getIntentId());
                }
                @Override public void onDeliveryFailed(Intent i, Throwable e) { /* no-op */ }
            });

            this.bucketReclaimer = new BucketReclaimer(wheelStore, locationIndex, wheelConfig.bucketRetentionMs());

            logger.info(
                "LoomqEngine created: nodeId={}, dataDir={}, horizonDays={}, slotsPerBucket={}, groupCommitIntervalMs={}, hotBoundaryMs={}, promotionLeadMs={}",
                nodeId, dataDir, wheelConfig.horizonDays(), wheelConfig.slotsPerBucket(),
                wheelConfig.groupCommitIntervalMs(), wheelConfig.hotBoundaryMs(), wheelConfig.promotionLeadMs());

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize LoomqEngine", e);
        }
    }

    /**
     * 启动引擎
     */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("Engine has been closed and cannot be restarted");
        }
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Engine is already running");
        }
        try {
            logger.info("╔════════════════════════════════════════════════════════╗");
            logger.info("║       LoomQ Core Engine Starting...                    ║");
            logger.info("║       Mode: Embedded                                   ║");
            logger.info("║       Persistence: PHTW (Layered Time Wheel)           ║");
            logger.info("╚════════════════════════════════════════════════════════╝");

            if (!deliveryHandlerConfigured) {
                logger.warn("No DeliveryHandler configured; intents will be silently dead-lettered. "
                    + "Supply one via Builder.deliveryHandler(...) to enable delivery.");
            }

            // 1. 恢复:扫所有槽 -> 重建索引 + 热载入内存 + 冷注册 promotion cohort
            WheelRecoveryReport recoveryReport =
                wheelRecovery.recover(intentStore, scheduler, locationIndex, promotionDaemon);
            if (recoveryReport.hotRestored() > 0 || recoveryReport.coldRegistered() > 0) {
                logger.info("WheelRecovery: hotRestored={}, coldRegistered={}",
                    recoveryReport.hotRestored(), recoveryReport.coldRegistered());
            }

            // F1:恢复期重建 multiSlot 标记,必须在 scheduler.start() 之前注入——
            // 否则重排程过的 Intent 在重启后会被误判单槽,终态回收掉当前槽后,
            // 陈旧兄弟槽会在下次重启按 max-revision 复活重投(幽灵投递)。
            if (!recoveryReport.multiSlotIntentIds().isEmpty()) {
                commandService.markMultiSlot(recoveryReport.multiSlotIntentIds());
            }

            // R8:恢复期注入磁盘历史最高 revision,使 createIntent 重建同 intentId 时能把新
            // Intent 的 revision 抬升到旧终态墓碑之上——否则 recovery max-revision 去重会
            // 遮蔽重建的新 Intent(静默丢失)。
            if (!recoveryReport.maxRevisions().isEmpty()) {
                commandService.markMaxRevisions(recoveryReport.maxRevisions());
            }

            // 2. 启动 group-commit daemon(DURABLE 写者依赖其 msync)
            commitBarrier.start();

            // 3. 启动 promotion daemon(冷->热提升 cohort)
            promotionDaemon.start();

            // 4. 启动调度器
            scheduler.setObservers(observers);
            scheduler.start();

            // 5. 启动桶回收 daemon(删除过期无活跃引用的桶文件)
            bucketReclaimer.start();

            // P2-1: running 闸门在所有 daemon 就绪后开放。此前 ensureRunning() 拒绝
            // 所有写操作，杜绝恢复期间 DURABLE 写入命中 awaitCommit 超时兖底慢路径。
            running.set(true);
            logger.info("Engine started successfully");
        } catch (Exception e) {
            // Best-effort 清理已部分启动的 daemon（各 daemon 有自身防重入守卫，双关闭安全）
            try { commitBarrier.close(); } catch (Exception ignored) {}
            try { promotionDaemon.close(); } catch (Exception ignored) {}
            try { scheduler.stop(); } catch (Exception ignored) {}
            try { bucketReclaimer.close(); } catch (Exception ignored) {}
            started.set(false);  // 允许重试
            throw e;
        }
    }

    /**
     * 停止引擎
     */
    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) return;  // 并发首次关闭守卫（唯一门控）

        running.set(false);
        started.set(false);
        logger.info("Shutting down LoomqEngine...");

        // P1-8: 先关操作执行器,等在途 createIntent 排空——否则在途 create 越过
        // ensureRunning 后,其调度/注册会落到已停止的 scheduler/promotionDaemon
        // (register/schedule 不查 running),intent 落盘却永不投递,直到重启。
        // 须在 daemon/scheduler 停止之前完成,使在途 create 仍能正常完成调度。
        operationExecutor.shutdown();
        try {
            if (!operationExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                operationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            operationExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 停止桶回收 daemon(在 wheelStore 关闭前停止)
        bucketReclaimer.close();

        // 停止提升 daemon
        promotionDaemon.close();

        // 停止调度器(内部排空在途投递)
        scheduler.stop();

        // 停止存储后台清理线程
        intentStore.shutdown();

        // 关闭 group-commit daemon
        commitBarrier.close();

        // 关闭时间轮(强制脏桶落盘)
        wheelStore.close();

        // C3-9: 关闭期压缩 tail run 文件(javadoc 承诺的"优雅关闭时(所有写入已停止)"时机)。
        // 长运行进程中 run 文件只增不减(PUT+TOMBSTONE 累积),若不压缩只能等下次启动;
        // 此时所有写入已停止(operationExecutor 已排空、daemon 已停),compaction 安全。
        try {
            tailIndex.compactIfNeeded(compactionThresholdBytes);
        } catch (Exception e) {
            logger.error("Tail compaction on close failed; run file left uncompacted", e);
        }

        // 关闭 tail run 文件
        tailIndex.close();

        // 关闭回调执行器
        if (callbackExecutor instanceof AutoCloseable) {
            ((AutoCloseable) callbackExecutor).close();
        }

        logger.info("Engine shutdown complete");
    }

    /**
     * 模拟进程崩溃（测试专用）：停止所有后台线程但不调用 wheelStore.close()/tailIndex.close()。
     *
     * <p>验证范围：终态写入不依赖 close() 的最终 forceDirty——若终态从未写盘，重开引擎时
     * recovery 会把它当作 SCHEDULED 处理（overdue 补终态），测试即失败。</p>
     *
     * <p>已知局限：进程退出后 OS 仍会把脏 mmap 页写回 page cache，且 stopWithoutFlush 后
     * daemon 可能完成一次在途 force。因此本测试<b>不能</b>区分 awaitCommit 的 msync 与
     * page-cache writeback；它证明的是终态记录已进入 wheel（可被 recovery 读取并跳过），
     * 而非内核级持久化。内核级验证需 OS crash 注入，超出 JVM 测试范围。</p>
     */
    void simulateCrash() {
        if (!started.get()) return;
        if (!closed.compareAndSet(false, true)) return;
        running.set(false);
        started.set(false);
        logger.info("Simulating crash (no flush)...");

        bucketReclaimer.close();
        promotionDaemon.close();
        scheduler.stop();
        operationExecutor.shutdownNow();
        intentStore.shutdown();
        commitBarrier.stopWithoutFlush();
        // 故意不调用 wheelStore.close() / tailIndex.close() / commitBarrier.close()
        // -- 测试正是要证明 awaitCommit 的 msync 已足够
    }

    /**
     * 创建 Intent（异步）
     *
     * @param intent  Intent 对象
     * @param ackMode 确认模式
     * @return CompletableFuture<Long> 序列号
     */
    public CompletableFuture<Long> createIntent(Intent intent, AckMode ackMode) {
        return CompletableFuture.supplyAsync(() -> commandService.createIntent(intent, ackMode), operationExecutor);
    }

    /**
     * 批量创建 Intent
     *
     * @param intents Intent 列表
     * @param ackMode 确认模式
     * @return CompletableFuture<List<Long>> 序列号列表
     */
    public CompletableFuture<List<Long>> createIntents(List<Intent> intents, AckMode ackMode) {
        return CompletableFuture.supplyAsync(() -> commandService.createIntents(intents, ackMode), operationExecutor);
    }

    /**
     * 查询 Intent
     *
     * <p><b>可见性窗口：</b>[create, terminal + 24h]。终态 Intent 在驱逐周期（默认 24h）
     * 后从内存 store 移除，此时 getIntent 返回 empty。磁盘仍保留权威记录，
     * 重启后由 recovery 跳过（终态不重新加载入内存）。</p>
     *
     * @param intentId Intent ID
     * @return Optional<Intent>
     */
    public Optional<Intent> getIntent(String intentId) {
        return Optional.ofNullable(intentStore.findById(intentId));
    }

    /**
     * 取消 Intent
     *
     * @param intentId Intent ID
     * @return true 如果成功取消
     */
    public boolean cancelIntent(String intentId) {
        return commandService.cancelIntent(intentId);
    }

    /**
     * 立即触发 Intent
     *
     * @param intentId Intent ID
     * @return true 如果成功触发
     */
    public boolean fireNow(String intentId) {
        return commandService.fireNow(intentId);
    }

    /**
     * 更新 Intent 并持久化。
     *
     * 适合 PATCH 场景：调用方只负责修改对象内容，核心负责落库。
     *
     * @param intentId Intent ID
     * @param updater  修改函数
     * @return 更新后的 Intent；不存在时返回 empty
     */
    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater) {
        return commandService.updateIntent(intentId, updater, null);
    }

    /**
     * 更新 Intent 并可选重调度。
     *
     * @param intentId Intent ID
     * @param updater 修改函数
     * @param newExecuteAt 新的执行时间，传 null 表示不调整调度
     * @return 更新后的 Intent；不存在时返回 empty
     */
    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater, Instant newExecuteAt) {
        return commandService.updateIntent(intentId, updater, newExecuteAt);
    }

    /**
     * 注册回调处理器
     *
     * @param handler CallbackHandler
     */
    public void registerCallbackHandler(CallbackHandler handler) {
        commandService.registerCallbackHandler(handler);
    }

    /**
     * 注册 Intent 生命周期观察器。
     * 可在引擎运行中随时注册/移除,线程安全——直接路由到调度器的 CopyOnWriteArrayList,
     * 修复原"启动后注册的 observer 永远收不到事件"的问题(start 时 setObservers 拷贝快照)。
     */
    public void registerObserver(IntentObserver observer) {
        observers.add(observer);
        scheduler.addObserver(observer);
    }

    /**
     * 移除 Intent 生命周期观察器。
     */
    public void removeObserver(IntentObserver observer) {
        observers.remove(observer);
        scheduler.removeObserver(observer);
    }

    /**
     * 检查幂等性。
     */
    public IdempotencyResult checkIdempotency(String idempotencyKey) {
        return commandService.checkIdempotency(idempotencyKey);
    }

    /**
     * 获取调度器（高级使用）
     *
     * @return PrecisionScheduler
     */
    public PrecisionScheduler getScheduler() {
        return scheduler;
    }

    /** 获取引擎级 MetricsCollector 实例。 */
    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    /**
     * 获取 Intent 存储只读视图。
     *
     * 返回的视图仅暴露读操作，写操作抛出 UnsupportedOperationException。
     * 需要写入 Intent 请通过 {@link #getCommandService()}。
     */
    public IntentStore getIntentStore() {
        return new ReadOnlyIntentStoreView(intentStore);
    }

    /**
     * 获取原始 Intent 存储（包级可见，内部使用）。
     */
    IntentStore getIntentStoreInternal() {
        return intentStore;
    }

    /**
     * 获取命令服务。
     */
    public IntentCommandService getCommandService() {
        return commandService;
    }

    /**
     * 获取 intent 位置索引(包级可见,供同包测试断言冷取消后的索引状态)。
     * 不作为公共 API:生产调用方应通过 commandService 间接操作。
     */
    IntentLocationIndex getLocationIndex() {
        return locationIndex;
    }

    /**
     * 获取运行状态标志。
     */
    public AtomicBoolean getRunning() {
        return running;
    }

    /**
     * 获取桶组管理器（高级使用）
     *
     * @return BucketGroupManager
     */
    public BucketGroupManager getBucketGroupManager() {
        return scheduler.getBucketGroupManager();
    }

    /**
     * 获取引擎状态
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 获取统计信息
     */
    public EngineStats getStats() {
        return new EngineStats(
            intentStore.getPendingCount(),
            sequenceNumber.get(),
            metricsCollector.getIntentCountsByTier()
        );
    }

    // ========== Builder ==========

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Path dataDir;
        private String nodeId;
        private WheelConfig wheelConfig;
        private Executor callbackExecutor;
        private CallbackHandler callbackHandler;
        private DeliveryHandler deliveryHandler;
        private RedeliveryDecider redeliveryDecider;
        private PrecisionTier defaultTier;
        private IntentStore intentStore;
        private MetricsCollector metricsCollector;
        private com.loomq.tracing.IntentTraceStore intentTraceStore;
        private PrecisionTierCatalog precisionTierCatalog;

        /**
         * @deprecated 改用 {@link #dataDir(Path)};PHTW 已无 WAL,字段名陈旧。委托 dataDir。
         */
        @Deprecated
        public Builder walDir(Path walDir) {
            this.dataDir = walDir;
            return this;
        }

        /** 数据目录(PHTW wheel + tail 根目录)。与 {@link #wheelConfig(WheelConfig)} 互斥,后者优先。 */
        public Builder dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        /** 完整 PHTW 配置;设了则覆盖 dataDir(用 wheelConfig.dataDir())。 */
        public Builder wheelConfig(WheelConfig wheelConfig) {
            this.wheelConfig = wheelConfig;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder defaultTier(PrecisionTier tier) {
            this.defaultTier = tier;
            return this;
        }

        public Builder callbackExecutor(Executor executor) {
            this.callbackExecutor = executor;
            return this;
        }

        public Builder callbackHandler(CallbackHandler handler) {
            this.callbackHandler = handler;
            return this;
        }

        public Builder deliveryHandler(DeliveryHandler handler) {
            this.deliveryHandler = handler;
            return this;
        }

        public Builder redeliveryDecider(RedeliveryDecider decider) {
            this.redeliveryDecider = decider;
            return this;
        }

        public Builder intentStore(IntentStore store) {
            this.intentStore = store;
            return this;
        }

        /** Inject a custom MetricsCollector (default: new instance per engine). */
        public Builder metricsCollector(MetricsCollector metrics) {
            this.metricsCollector = metrics;
            return this;
        }

        /** Inject a custom IntentTraceStore (default: new instance per engine). */
        public Builder intentTraceStore(com.loomq.tracing.IntentTraceStore store) {
            this.intentTraceStore = store;
            return this;
        }

        /** Inject a custom precision tier catalog (default: {@link PrecisionTierCatalog#defaultCatalog()}). */
        public Builder catalog(PrecisionTierCatalog catalog) {
            this.precisionTierCatalog = catalog;
            return this;
        }

        public LoomqEngine build() {
            return new LoomqEngine(this);
        }
    }

    // ========== 统计类 ==========

    public record EngineStats(
        long pendingCount,
        long totalSequence,
        Map<PrecisionTier, Long> intentCountsByTier
    ) {}
}
