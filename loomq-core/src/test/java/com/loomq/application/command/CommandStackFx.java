package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * 冷命令域共享测试夹具(round 16 收口):真实组件栈单点装配——
 * WheelStore / TailIndex / GroupCommitBarrier / IntentLocationIndex / ConcurrentIntentStore /
 * PrecisionScheduler / PromotionDaemon / IntentCommandService(+MetricsCollector)九件,
 * start 三件套(barrier/daemon/scheduler)与 close 五件套(scheduler → daemon → barrier →
 * tail → store)配对收口;投递桩统一 DEAD_LETTER 完成态。
 *
 * <p>旋钮化五处既有语义分叉(时钟/promote 回调/revision 种子/是否启动调度器),分叉依据见
 * round 16 spec §4.5——种子与否、启动与否逐字保留各测试原语义,不做顺手统一。失败注入桩
 * (PrecisionScheduler/WheelPersistence 子类、go 门闩、探针)仍留各测试文件本地,本夹具只承载
 * 真实组件栈。先例:testutil/TestWheelConfigs 共享工厂模式。</p>
 */
final class CommandStackFx implements AutoCloseable {

    /** 装配旋钮(null 成员按注释取默认)。 */
    record Options(LongSupplier clock,
                   BiConsumer<Intent, SlotLocation> onHotPromotion,
                   boolean seedMaxRevisions,
                   boolean startScheduler) {
        Options {
            clock = clock != null ? clock : System::currentTimeMillis;
            onHotPromotion = onHotPromotion != null ? onHotPromotion : (i, l) -> { };
        }
    }

    private static final long HOT_BOUNDARY_MS = 60L * 60_000L;
    private static final long PROMOTION_LEAD_MS = 60_000L;

    private final WheelStore store;
    private final TailIndex tail;
    private final GroupCommitBarrier barrier;
    private final IntentLocationIndex idx;
    private final ConcurrentIntentStore memStore;
    private final PrecisionScheduler scheduler;
    private final PromotionDaemon daemon;
    private final IntentCommandService svc;
    private final MetricsCollector mc;
    private final Options options;

    CommandStackFx(Path dir) {
        this(dir, new Options(System::currentTimeMillis, null, false, true));
    }

    CommandStackFx(Path dir, Options options) {
        this.options = options;
        // T2 后 7 参便捷构造:30d / 16 槽 / 1ms / 10s / 60min / 60s(retention/compaction 走默认)
        WheelConfig cfg = new WheelConfig(dir.toString(), 30, 16, 1, 10_000L,
            HOT_BOUNDARY_MS, PROMOTION_LEAD_MS);
        store = new WheelStore(cfg, options.clock());
        tail = new TailIndex(dir, options.clock());
        barrier = new GroupCommitBarrier(store, tail, 1, 10_000);
        idx = new IntentLocationIndex();
        memStore = new ConcurrentIntentStore();
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong seq = new AtomicLong();
        mc = new MetricsCollector();
        scheduler = new PrecisionScheduler(memStore, intent ->
            CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER), null);
        daemon = new PromotionDaemon(store, tail, idx, options.clock(),
            options.onHotPromotion(), PROMOTION_LEAD_MS);
        svc = new IntentCommandService(memStore, scheduler,
            new IntentCommandService.PhtwStack(store, tail, barrier, idx, daemon),
            mc, Executors.newVirtualThreadPerTaskExecutor(),
            running, seq, null,
            new IntentCommandService.CommandConfig(PrecisionTier.STANDARD, 1L, HOT_BOUNDARY_MS,
                PrecisionTierCatalog.defaultCatalog()),
            new IntentTraceStore());
        barrier.start();
        daemon.start();
        if (options.startScheduler()) {
            scheduler.start();
        }
    }

    /** 冷 Intent 落 wheel(+2h,revision=1,不进 memStore);seedMaxRevisions=true 时按 recovery 语义种子。 */
    Intent plantColdWheel(String id, Map<String, String> tags) {
        Intent cold = new Intent(id);
        cold.setExecuteAt(Instant.ofEpochMilli(options.clock().getAsLong() + 2 * 60 * 60 * 1000L));
        cold.setPrecisionTier(PrecisionTier.STANDARD);
        cold.transitionTo(IntentStatus.SCHEDULED);
        cold.incrementRevision();
        if (tags != null) {
            cold.setTags(tags);
        }
        SlotLocation loc = store.put(cold);
        idx.put(cold.getIntentId(), loc);
        seedIfEnabled(cold);
        return cold;
    }

    /** 冷 Intent 落 tail(+31d,revision=1)。 */
    Intent plantColdTail(String id, Map<String, String> tags) {
        long execMs = options.clock().getAsLong() + 31L * 24 * 60 * 60 * 1000L;
        Intent cold = new Intent(id);
        cold.setExecuteAt(Instant.ofEpochMilli(execMs));
        cold.setPrecisionTier(PrecisionTier.STANDARD);
        cold.transitionTo(IntentStatus.SCHEDULED);
        cold.incrementRevision();
        if (tags != null) {
            cold.setTags(tags);
        }
        tail.put(cold);
        idx.put(cold.getIntentId(), SlotLocation.tail(execMs));
        seedIfEnabled(cold);
        return cold;
    }

    /** 冷写者推进磁盘:直写新槽(新内容)+ 索引改指(+种子)——镜像 updateCold 的完整磁盘效果。 */
    SlotLocation advanceDisk(Intent base, Map<String, String> tags) {
        Intent newer = base.copy();
        newer.setTags(tags);
        newer.incrementRevision();
        SlotLocation loc = store.put(newer);
        idx.put(newer.getIntentId(), loc);
        seedIfEnabled(newer);
        return loc;
    }

    /** 植入 stale 热副本(镜像在途 promote:upsert + schedule,旧 revision 副本)。 */
    void plantStaleHot(Intent oldCopy) {
        memStore.upsert(oldCopy);
        scheduler.schedule(oldCopy);
    }

    /** 植入 hot 副本并进调度结构(磁盘与内存同 revision——fresh 场景)。 */
    void plantHotSync(Intent copy) {
        memStore.upsert(copy);
        scheduler.schedule(copy);
    }

    private void seedIfEnabled(Intent intent) {
        if (options.seedMaxRevisions()) {
            svc.markMaxRevisions(Map.of(intent.getIntentId(), (long) intent.getRevision()));
        }
    }

    IntentCommandService svc() { return svc; }

    ConcurrentIntentStore memStore() { return memStore; }

    PrecisionScheduler scheduler() { return scheduler; }

    PromotionDaemon daemon() { return daemon; }

    GroupCommitBarrier barrier() { return barrier; }

    IntentLocationIndex idx() { return idx; }

    WheelStore store() { return store; }

    TailIndex tail() { return tail; }

    MetricsCollector mc() { return mc; }

    @Override public void close() {
        if (options.startScheduler()) {
            scheduler.stop();
        }
        daemon.close();
        barrier.close();
        tail.close();
        store.close();
    }
}
