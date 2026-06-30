package com.loomq;

import com.loomq.application.command.IntentCommandService;
import com.loomq.application.scheduler.BucketGroupManager;
import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.application.swap.ColdIntentSwapper;
import com.loomq.common.MetricsCollector;
import com.loomq.config.WalConfig;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.recovery.RecoveryPipeline;
import com.loomq.snapshot.SnapshotManager.SnapshotInfo;
import com.loomq.spi.CallbackHandler;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.IntentObserver;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.spi.WalAccessor;
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
 * @author loomq
 */
public class LoomqEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LoomqEngine.class);

    // ========== 核心组件 ==========
    private final IntentStore intentStore;
    private final SimpleWalWriter walWriter;
    private final MetricsCollector metricsCollector;
    private final PrecisionScheduler scheduler;
    private final RecoveryPipeline recoveryPipeline;
    private final IntentCommandService commandService;
    private final ColdIntentSwapper coldSwapper;

    // ========== 观察器 ==========
    private final List<IntentObserver> observers = new CopyOnWriteArrayList<>();

    // ========== 回调机制 ==========
    private final Executor callbackExecutor;
    private final Executor walWriteExecutor;
    private final java.util.concurrent.ExecutorService operationExecutor;

    // ========== 状态 ==========
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong sequenceNumber = new AtomicLong(0);

    // ========== 配置 ==========
    private final Path walDir;
    private final String nodeId;
    private final WalConfig walConfig;
    private final PrecisionTier defaultTier;

    private LoomqEngine(Builder builder) {
        this.nodeId = builder.nodeId != null ? builder.nodeId : "default-node";
        this.walDir = builder.walDir != null ? builder.walDir : Path.of("./data");
        this.walConfig = builder.walConfig != null ? builder.walConfig : defaultWalConfig();
        this.defaultTier = builder.defaultTier;
        this.callbackExecutor = builder.callbackExecutor != null
            ? builder.callbackExecutor
            : Executors.newVirtualThreadPerTaskExecutor();
        this.walWriteExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.operationExecutor = Executors.newVirtualThreadPerTaskExecutor();

        try {
            // 确保数据目录存在
            Files.createDirectories(walDir);

            // 初始化组件
            this.intentStore = builder.intentStore != null
                ? builder.intentStore
                : new ConcurrentIntentStore();
            this.walWriter = new SimpleWalWriter(walConfig, "shard-0");
            this.metricsCollector = MetricsCollector.getInstance();
            this.recoveryPipeline = new RecoveryPipeline(walDir);

            // 初始化调度器
            this.scheduler = new PrecisionScheduler(
                intentStore,
                builder.deliveryHandler,
                builder.redeliveryDecider
            );

            // 初始化冷热交换器（依赖 scheduler）
            this.coldSwapper = new ColdIntentSwapper(intentStore, scheduler, walWriter);

            this.commandService = new IntentCommandService(
                intentStore,
                scheduler,
                walWriter,
                metricsCollector,
                callbackExecutor,
                walWriteExecutor,
                running,
                sequenceNumber,
                builder.callbackHandler,
                defaultTier,
                coldSwapper
            );

            logger.info(
                "LoomqEngine created: nodeId={}, walDir={}, walEngine={}, flushStrategy={}, syncOnWrite={}, segmentSizeMb={}, flushThresholdKb={}, stripeCount={}",
                nodeId,
                walDir,
                walConfig.engine(),
                walConfig.flushStrategy(),
                walConfig.syncOnWrite(),
                walConfig.segmentSizeMb(),
                walConfig.memorySegmentFlushThresholdKb(),
                walConfig.memorySegmentStripeCount()
            );

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize LoomqEngine", e);
        }
    }

    /**
     * 启动引擎
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Engine is already running");
        }

        logger.info("╔════════════════════════════════════════════════════════╗");
        logger.info("║       LoomQ Core Engine Starting...                    ║");
        logger.info("║       Mode: Embedded (Zero HTTP dependencies)          ║");
        logger.info("╚════════════════════════════════════════════════════════╝");

        // 先恢复快照和 WAL 增量，再启动运行时
        RecoveryPipeline.RecoveryReport recoveryReport = recoveryPipeline.recover(intentStore, scheduler, walWriter);
        if (recoveryReport.restoredTotal() > 0) {
            logger.info("Recovered {} intents from snapshot/WAL (snapshot={}, wal={})",
                recoveryReport.restoredTotal(),
                recoveryReport.restoredFromSnapshot(),
                recoveryReport.restoredFromWal());
        }

        // 启动 WAL
        walWriter.start();

        // 启动调度器
        scheduler.setObservers(observers);
        scheduler.start();

        // 启动冷热交换器（长延迟 Intent 内存换出/换入）
        coldSwapper.start();

        // 启动定期快照（快照完成后自动截断旧 WAL 段，冷 Intent 的 WAL 段受保护不被截断）
        recoveryPipeline.startSnapshots(intentStore, walWriter::getWritePosition, () -> walWriter,
            coldSwapper::getMinRequiredWalPosition);

        logger.info("Engine started successfully");
    }

    /**
     * 停止引擎
     */
    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        running.set(false);
        logger.info("Shutting down LoomqEngine...");

        // 停止恢复/快照管线
        recoveryPipeline.close();

        // 停止调度器
        scheduler.stop();

        // 停止冷热交换器
        coldSwapper.close();

        // 停止存储后台清理线程
        intentStore.shutdown();

        // 关闭 WAL
        walWriter.close();

        // 关闭 WAL 写入执行器
        if (walWriteExecutor instanceof AutoCloseable ac) {
            ac.close();
        }

        // 关闭回调执行器
        if (callbackExecutor instanceof AutoCloseable) {
            ((AutoCloseable) callbackExecutor).close();
        }

        // 关闭操作执行器
        operationExecutor.close();

        logger.info("Engine shutdown complete");
    }

    /**
     * 创建 Intent（异步）
     *
     * @param intent  Intent 对象
     * @param ackMode 确认模式
     * @return CompletableFuture<Long> WAL 序列号
     */
    public CompletableFuture<Long> createIntent(Intent intent, AckMode ackMode) {
        return CompletableFuture.supplyAsync(() -> commandService.createIntent(intent, ackMode), operationExecutor);
    }

    /**
     * 批量创建 Intent
     *
     * @param intents Intent 列表
     * @param ackMode 确认模式
     * @return CompletableFuture<List<Long>> WAL 序列号列表
     */
    public CompletableFuture<List<Long>> createIntents(List<Intent> intents, AckMode ackMode) {
        return CompletableFuture.supplyAsync(() -> commandService.createIntents(intents, ackMode), operationExecutor);
    }

    /**
     * 查询 Intent
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
     * 可在引擎运行中随时注册/移除，线程安全。
     */
    public void registerObserver(IntentObserver observer) {
        observers.add(observer);
    }

    /**
     * 移除 Intent 生命周期观察器。
     */
    public void removeObserver(IntentObserver observer) {
        observers.remove(observer);
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
     * 获取原始 Intent 存储（内部使用，可读写）。
     *
     * 外部调用方应使用 {@link #getIntentStore()} 只读视图。
     */
    public IntentStore getIntentStoreInternal() {
        return intentStore;
    }

    /**
     * 获取 WAL 访问器（供服务层读取/截断 WAL）
     *
     * @return WalAccessor
     */
    public WalAccessor getWalAccessor() {
        return walWriter;
    }

    /**
     * 获取命令服务。
     */
    public IntentCommandService getCommandService() {
        return commandService;
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

    /**
     * 获取 WAL 健康状态
     */
    public WalHealthStatus getWalHealth() {
        return new WalHealthStatus(
            walWriter.getWalHealth(),
            walWriter.getUnflushedBytes(),
            walWriter.getLastFsyncMsAgo(),
            walWriter.getWritePosition(),
            walWriter.getFlushedPosition()
        );
    }

    /**
     * WAL 健康状态
     */
    public record WalHealthStatus(
        String status,
        long unflushedBytes,
        long lastFsyncMsAgo,
        long writePosition,
        long flushedPosition
    ) {}

    /**
     * 立即生成一次快照。
     */
    public SnapshotInfo createSnapshot() {
        ensureRunning();
        return recoveryPipeline.checkpoint(intentStore, walWriter::getWritePosition);
    }

    /**
     * 获取冷热交换统计。
     */
    public ColdSwapStats getColdSwapStats() {
        return new ColdSwapStats(
            coldSwapper.coldIntentCount(),
            coldSwapper.getTotalSwappedOut(),
            coldSwapper.getTotalSwappedIn(),
            coldSwapper.getSwapInErrors(),
            coldSwapper.getColdThreshold()
        );
    }

    /**
     * 冷热交换统计。
     */
    public record ColdSwapStats(
        int coldIntentCount,
        long totalSwappedOut,
        long totalSwappedIn,
        long swapInErrors,
        java.time.Duration coldThreshold
    ) {}

    // ========== 内部方法 ==========

    private void ensureRunning() {
        if (!running.get()) {
            throw new IllegalStateException("Engine is not running");
        }
    }

    private WalConfig defaultWalConfig() {
        return WalConfig.defaultConfig().withDataDir(walDir.toString());
    }

    // ========== Builder ==========

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Path walDir;
        private String nodeId;
        private WalConfig walConfig;
        private Executor callbackExecutor;
        private CallbackHandler callbackHandler;
        private DeliveryHandler deliveryHandler;
        private RedeliveryDecider redeliveryDecider;
        private PrecisionTier defaultTier;
        private IntentStore intentStore;

        public Builder walDir(Path walDir) {
            this.walDir = walDir;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder walConfig(WalConfig walConfig) {
            this.walConfig = walConfig;
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
