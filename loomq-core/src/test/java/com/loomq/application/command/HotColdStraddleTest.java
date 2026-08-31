package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotEntry;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * F2 冷热写者 straddle 收口(round 15):热写者(updateIntent/fireNow/cancel)在 per-id 冷锁内
 * 复核 revision 后原子写;stale 热副本(在途 promote 旧副本被并发冷写者推进)委托冷路径
 * fresh 重放,杜绝同 revision 双写。夹具为共享 CommandStackFx(真实组件、真实时钟,
 * seedMaxRevisions=true——recovery 种子是 W1-W3 守卫复核的前提)。
 */
class HotColdStraddleTest {
    private static final long HOT_BOUNDARY_MS = 60L * 60_000L;
    private static final long PROMOTION_LEAD_MS = 60_000L;

    /** Straddle 场景固定旋钮:系统时钟 + recovery 种子(markMaxRevisions,守卫前提)+ 启动全件。 */
    private static final CommandStackFx.Options STRADDLE_FX_OPTS =
        new CommandStackFx.Options(System::currentTimeMillis, null, true, true);

    @TempDir Path tmp;

    @Test
    @org.junit.jupiter.api.DisplayName("F2 W1:磁盘已前进 → 热 updateIntent 委托冷路径 fresh 重放,revision 线性无平局")
    void hotUpdateOnStaleCopyDelegatesToColdPath() {
        try (CommandStackFx fx = new CommandStackFx(tmp.resolve("w1stale"), STRADDLE_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_w1stale001", Map.of("v", "r0"));
            fx.advanceDisk(planted, Map.of("v", "r2"));
            fx.plantStaleHot(planted);   // 内存 stale 副本 R1(镜像在途 promote 载入)

            Optional<Intent> updated = fx.svc().updateIntent("intent_w1stale001",
                i -> i.setTags(Map.of("v", "r2", "hot", "1")), null);

            assertTrue(updated.isPresent(), "stale 委托冷路径后更新生效");
            SlotLocation latest = fx.idx().get("intent_w1stale001");
            Intent disk = fx.store().readSlot(latest);
            // 冷写者 R2 + 委托重放 R3:严格线性,无同 revision 双写
            assertEquals(3, disk.getRevision(), "revision 线性:冷写 R2 → 委托重放 R3");
            assertEquals("r2", disk.getTags().get("v"), "重放作用于磁盘 fresh 态");
            assertEquals("1", disk.getTags().get("hot"));
            // stale 热副本被收口:窗口外冷写路由 demote,store 不残留旧副本
            assertNull(fx.memStore().findByIdInternal("intent_w1stale001"),
                "stale 热副本由 stale 分支显式 removeHotCopyIfPresent 收口"
                    + "(routeColdAfterPersist 的 removeHotCopyIfStale 为竞态兜底)");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("F2 W1 fresh 路径:磁盘未前进 → 行为与现状一致(直写新槽)")
    void hotUpdateOnFreshCopyWritesDirectly() {
        try (CommandStackFx fx = new CommandStackFx(tmp.resolve("w1fresh"), STRADDLE_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_w1fresh01", Map.of("v", "r0"));
            fx.plantHotSync(planted);   // 内存副本 R1 = 磁盘 R1,fresh

            Optional<Intent> updated = fx.svc().updateIntent("intent_w1fresh01",
                i -> i.setTags(Map.of("v", "r1-hot")), null);

            assertTrue(updated.isPresent());
            SlotLocation latest = fx.idx().get("intent_w1fresh01");
            Intent disk = fx.store().readSlot(latest);
            assertEquals(2, disk.getRevision());
            assertEquals("r1-hot", disk.getTags().get("v"));
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("F2 W1:委托重放中 updater 抛 IAE → 原样传播,不复活 demote 的 stale 副本")
    void delegatedReplayFailurePropagatesWithoutResurrectingStaleCopy() {
        try (CommandStackFx fx = new CommandStackFx(tmp.resolve("w1iaefail"), STRADDLE_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_w1iaef001", Map.of("v", "r0"));
            fx.advanceDisk(planted, Map.of("v", "r2"));
            fx.plantStaleHot(planted);

            // updater 首次作用于 stale 副本(v=r0)通过;冷路径重放作用于 fresh(v=r2)时抛出
            assertThrows(IllegalArgumentException.class,
                () -> fx.svc().updateIntent("intent_w1iaef001",
                    i -> {
                        if ("r2".equals(i.getTags().get("v"))) {
                            throw new IllegalArgumentException("replay rejected");
                        }
                        i.setTags(Map.of("v", "mutated"));
                    }, null));

            assertNull(fx.memStore().findByIdInternal("intent_w1iaef001"),
                "stale 副本保持 demote,不得被外层回滚 restore 复活");
            SlotLocation latest = fx.idx().get("intent_w1iaef001");
            assertEquals("r2", fx.store().readSlot(latest).getTags().get("v"), "磁盘权威态不被污染");
        }
    }

    /** 合并式 tag 更新(setTags 为整表替换,并发双写者须显式合并才能断言复合生效)。 */
    private static Consumer<Intent> putTag(String key, String value) {
        return i -> {
            Map<String, String> merged = new HashMap<>(i.getTags() != null ? i.getTags() : Map.of());
            merged.put(key, value);
            i.setTags(merged);
        };
    }

    /** 全轮扫描取该 intentId 的最高 revision 槽内容——冷取消成功后索引已清,权威终态槽以此定位。 */
    private static Intent maxRevisionSlotFor(CommandStackFx fx, String intentId) {
        Intent best = null;
        var slots = fx.store().scanSlotsFrom(Instant.now().minusSeconds(60));
        while (slots.hasNext()) {
            SlotEntry e = slots.next();
            if (e.intent().getIntentId().equals(intentId)
                && (best == null || e.intent().getRevision() > best.getRevision())) {
                best = e.intent();
            }
        }
        return best;
    }

    @Test
    @org.junit.jupiter.api.DisplayName("F2 W2:磁盘已前进 → 热 fireNow demote stale 副本并委托冷 fireNow")
    void hotFireNowOnStaleCopyDelegatesToColdFireNow() {
        try (CommandStackFx fx = new CommandStackFx(tmp.resolve("w2stale"), STRADDLE_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_w2stale001", Map.of("v", "r0"));
            fx.advanceDisk(planted, Map.of("v", "r2"));
            fx.plantStaleHot(planted);

            assertTrue(fx.svc().fireNow("intent_w2stale001"), "委托冷 fireNow 生效返回 true");

            Intent disk = fx.store().readSlot(fx.idx().get("intent_w2stale001"));
            assertNotNull(disk);
            // 冷 fireNow 从磁盘 fresh 态(R2)重放:executeAt=now + R3
            assertEquals(3, disk.getRevision(), "revision 线性:冷写 R2 → 冷 fireNow R3");
            assertTrue(disk.getExecuteAt().toEpochMilli() <= System.currentTimeMillis() + 5_000,
                "executeAt 已改写为 now");
            assertNotNull(fx.memStore().findByIdInternal("intent_w2stale001"),
                "executeAt=now 在热窗口内:冷路径已热载 fresh 副本");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("F2 W3:磁盘已前进 → 热 cancel 回滚 transitionTo 并委托冷取消")
    void hotCancelOnStaleCopyDelegatesToColdCancel() {
        try (CommandStackFx fx = new CommandStackFx(tmp.resolve("w3stale"), STRADDLE_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_w3stale001", Map.of("v", "r0"));
            fx.advanceDisk(planted, Map.of("v", "r2"));
            fx.plantStaleHot(planted);

            assertTrue(fx.svc().cancelIntent("intent_w3stale001"), "委托冷取消生效");

            // 冷取消清索引 + removeHotCopyIfPresent 收口热副本;索引已清不可按址读槽,
            // 改经全轮扫描取该 intent 最高 revision 槽(=权威终态槽)
            assertNull(fx.idx().get("intent_w3stale001"), "冷取消已清索引");
            Intent disk = maxRevisionSlotFor(fx, "intent_w3stale001");
            assertNotNull(disk, "磁盘存在 CANCELED 终态槽");
            assertEquals(IntentStatus.CANCELED, disk.getStatus(), "磁盘权威槽被取消");
            assertEquals(3, disk.getRevision(), "revision 线性:冷写 R2 → 冷取消 R3");
            assertEquals("r2", disk.getTags().get("v"), "取消作用在 fresh 内容上,非 stale 覆写");
            assertNull(fx.memStore().findByIdInternal("intent_w3stale001"), "stale 热副本已收口");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("F2 W3 fresh 路径:磁盘未前进 → 热取消行为与现状一致(原地覆写终态)")
    void hotCancelOnFreshCopyPersistsTerminalInPlace() {
        try (CommandStackFx fx = new CommandStackFx(tmp.resolve("w3fresh"), STRADDLE_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_w3fresh01", Map.of("v", "r0"));
            fx.plantHotSync(planted);
            SlotLocation plantedLoc = fx.idx().get("intent_w3fresh01");
            // 预标 multi-slot(recovery 注入缝同款):单槽终态会被 reclaimTerminal 清索引并
            // 清空槽位复用,原地覆写证据随即消失;multi-slot 终态保留槽位(墓碑),使覆写可观测。
            fx.svc().markMultiSlot(Set.of("intent_w3fresh01"));

            assertTrue(fx.svc().cancelIntent("intent_w3fresh01"));

            // 原地覆写:终态落在原槽位(非新槽);索引随后被 reclaimTerminal 定向清除(现状行为)
            Intent disk = fx.store().readSlot(plantedLoc);
            assertNotNull(disk, "multi-slot 终态槽保留不 free,原地覆写可观测");
            assertEquals(IntentStatus.CANCELED, disk.getStatus());
            assertEquals(2, disk.getRevision());
            assertNull(fx.idx().get("intent_w3fresh01"), "reclaimTerminal 定向清索引(现状行为)");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("F2 W4:投递重试持久化守卫——磁盘已到/超过本次 revision → 跳过;新鲜 → 落盘")
    void guardedDeliveryPersistSkipsStaleCopy() {
        try (CommandStackFx fx = new CommandStackFx(tmp.resolve("w4guard"), STRADDLE_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_w4guard001", Map.of("v", "r0"));
            SlotLocation loc2 = fx.advanceDisk(planted, Map.of("v", "r2"));   // 磁盘 R2

            // 模拟 StatePersistence.persistStateChange:已 incrementRevision 后调 sink
            Intent staleCopy = planted.copy();
            long baseRev = staleCopy.getRevision();
            staleCopy.setTags(Map.of("v", "hot-stale"));
            staleCopy.incrementRevision();   // R2——与磁盘平局
            fx.svc().persistStateChangePutOnly(staleCopy);
            assertEquals("r2", fx.store().readSlot(fx.idx().get("intent_w4guard001")).getTags().get("v"),
                "stale 副本被跳过,磁盘保持冷写者权威内容");

            // fresh 副本(磁盘 R2 之后 increment 到 R3)→ 正常落盘
            Intent freshCopy = fx.store().readSlot(loc2);
            freshCopy.setTags(Map.of("v", "hot-fresh"));
            freshCopy.incrementRevision();   // R3
            fx.svc().persistStateChangePutOnly(freshCopy);
            SlotLocation latest = fx.idx().get("intent_w4guard001");
            assertEquals("hot-fresh", fx.store().readSlot(latest).getTags().get("v"),
                "fresh 副本照常落盘并改指索引");

            // F3(round 15 终审)第三段:skip 同时回滚 increment——StatePersistence 已把活对象
            // increment 到与 diskMax 平局,skip 后平局副本若不回退,revision 被洗白:下次重试/
            // 热更新以 stale 内容确定性覆盖冷写者更新。
            assertEquals(baseRev, staleCopy.getRevision(),
                "skip 后活副本 revision 回退到 base(杜绝 revision 洗白)");
            staleCopy.incrementRevision();
            fx.svc().persistStateChangePutOnly(staleCopy);
            assertEquals(baseRev, staleCopy.getRevision(), "同副本再 increment 再调守卫仍 skip 且再次回退");
            assertEquals("hot-fresh", fx.store().readSlot(fx.idx().get("intent_w4guard001")).getTags().get("v"),
                "仍 skip:磁盘保持 fresh 段内容不变");
        }
    }

    @Test
    @Tag("slow")
    @org.junit.jupiter.api.Timeout(120)
    @org.junit.jupiter.api.DisplayName("F2 串行化竞态:热/冷写者并发——败者经 stale 守卫委托重放,revision 线性且两更新复合生效")
    void hotAndColdWritersSerializeOnColdLock() throws Exception {
        int rounds = 50;
        for (int r = 0; r < rounds; r++) {
            String id = String.format("intent_f2seri%04d", r);
            try (CommandStackFx fx = new CommandStackFx(tmp.resolve("seri" + r), STRADDLE_FX_OPTS)) {
                Intent planted = fx.plantColdWheel(id, Map.of("v", "r0"));
                // planted 是与写者共享的活对象(findByIdInternal 返回内部引用),竞态会推进其
                // revision——基线必须在起跑前捕获,断言用快照值(R0+2)
                long baseRev = planted.getRevision();
                fx.plantStaleHot(planted);   // 制造 stale 热副本(在途 promote 的等价制造源)
                CountDownLatch start = new CountDownLatch(1);
                ExecutorService pool = Executors.newFixedThreadPool(2);
                Future<Optional<Intent>> hotF = pool.submit(() -> {
                    start.await();
                    return fx.svc().updateIntent(id, putTag("hot", "1"), null);
                });
                Future<Optional<Intent>> coldF = pool.submit(() -> {
                    start.await();
                    return fx.svc().updateIntent(id, putTag("cold", "1"), null);
                });
                start.countDown();
                Optional<Intent> hotR = hotF.get(30, TimeUnit.SECONDS);
                Optional<Intent> coldR = coldF.get(30, TimeUnit.SECONDS);
                pool.shutdownNow();

                assertTrue(hotR.isPresent() && coldR.isPresent(),
                    "round " + r + ": 两更新皆成功(败者经守卫委托 fresh 重放)");
                Intent disk = fx.store().readSlot(fx.idx().get(id));
                assertEquals(baseRev + 2, disk.getRevision(),
                    "round " + r + ": revision 严格线性(无同 revision 平局)");
                assertEquals("1", disk.getTags().get("hot"), "round " + r + ": hot 更新复合生效");
                assertEquals("1", disk.getTags().get("cold"), "round " + r + ": cold 更新复合生效");
            }
        }
    }

    @Test
    @Tag("slow")
    @org.junit.jupiter.api.Timeout(120)
    @org.junit.jupiter.api.DisplayName("F1 冷锁注册表:同一 id 8 线程×200 轮 acquire/临界区/release——在区计数器互斥不变量零重叠")
    void coldLockRegistrySerializesWaitersOnSameId() throws Exception {
        // 本测试只锤锁注册表:acquire/release 不触达 wheel/tail/barrier/索引,构造参数置 null。
        WheelPersistence persistence = new WheelPersistence(null, null, null, null);
        int threads = 8;
        int rounds = 200;
        String id = "intent_f1same0001";
        AtomicInteger inRegion = new AtomicInteger();
        AtomicLong overlaps = new AtomicLong();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            workers.add(pool.submit(() -> {
                start.await();
                for (int r = 0; r < rounds; r++) {
                    Object lock = persistence.acquireColdLock(id);
                    synchronized (lock) {
                        // F1 互斥不变量:进区时在区计数必须为 0。旧行为(release 以 identity
                        // 校验移除锁对象)下,等待者 B 持 L1 阻塞在监视器、A release 移除注册表项,
                        // 新到者 C computeIfAbsent 得 L2 → B/C 同临界区 → 计数 ≥ 1。
                        if (inRegion.getAndIncrement() != 0) {
                            overlaps.incrementAndGet();
                        }
                        Thread.yield();   // 放大交错窗口
                        inRegion.decrementAndGet();
                    }
                    persistence.releaseColdLock(id, lock);
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> w : workers) {
            w.get(60, TimeUnit.SECONDS);
        }
        pool.shutdownNow();
        assertEquals(0, overlaps.get(),
            "同一 id 的冷锁临界区不得重叠(等待者持旧锁对象 vs 新到者新锁对象,F1 竞态)");
    }

    @Test
    @Tag("slow")
    @org.junit.jupiter.api.Timeout(120)
    @org.junit.jupiter.api.DisplayName("F1 冷锁注册表:多 id 混合并发——各 id 独立互斥零重叠,跨 id 无串扰")
    void coldLockRegistrySerializesMixedIds() throws Exception {
        WheelPersistence persistence = new WheelPersistence(null, null, null, null);
        int threads = 8;
        int rounds = 200;
        List<String> ids = List.of("intent_f1mix0001", "intent_f1mix0002", "intent_f1mix0003", "intent_f1mix0004");
        Map<String, AtomicInteger> inRegion = new HashMap<>();
        for (String id : ids) {
            inRegion.put(id, new AtomicInteger());
        }
        AtomicLong overlaps = new AtomicLong();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers.add(pool.submit(() -> {
                start.await();
                for (int r = 0; r < rounds; r++) {
                    String id = ids.get((tid + r) % ids.size());
                    Object lock = persistence.acquireColdLock(id);
                    synchronized (lock) {
                        if (inRegion.get(id).getAndIncrement() != 0) {
                            overlaps.incrementAndGet();
                        }
                        Thread.yield();
                        inRegion.get(id).decrementAndGet();
                    }
                    persistence.releaseColdLock(id, lock);
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> w : workers) {
            w.get(60, TimeUnit.SECONDS);
        }
        pool.shutdownNow();
        assertEquals(0, overlaps.get(), "每个 id 的冷锁临界区各自互斥,不得重叠(多 id 混合并发)");
    }

    @Test
    @Tag("slow")
    @org.junit.jupiter.api.Timeout(120)
    @org.junit.jupiter.api.DisplayName("F4:投递终态原地覆写持 per-id 冷锁——覆写与冷命令临界区互斥,关闭 terminal/SCHEDULED 同 revision 平局窗")
    void persistTerminalInPlaceSerializesWithColdWriterOnColdLock() throws Exception {
        Path dir = tmp.resolve("f4tie");
        WheelConfig cfg = new WheelConfig(dir.toString(), 30, 16, 1, 10_000L, HOT_BOUNDARY_MS, PROMOTION_LEAD_MS);
        WheelStore store = new WheelStore(cfg, System::currentTimeMillis);
        TailIndex tail = new TailIndex(dir, System::currentTimeMillis);
        GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 1, 10_000);
        barrier.start();
        try {
            IntentLocationIndex idx = new IntentLocationIndex();
            WheelPersistence persistence = new WheelPersistence(store, tail, barrier, idx);
            String id = "intent_f4tie00001";

            // 植入 SCHEDULED R1 槽(镜像在途投递 Intent 的磁盘态)+ 索引
            Intent planted = new Intent(id);
            planted.setExecuteAt(Instant.now().plus(2, ChronoUnit.HOURS));
            planted.setPrecisionTier(PrecisionTier.STANDARD);
            planted.transitionTo(IntentStatus.SCHEDULED);
            planted.incrementRevision();
            SlotLocation locX = store.put(planted);
            idx.put(id, locX);

            // 冷命令镜像:持冷锁临界区——锁内重读索引 + 解码后暂停(把 T 顶进"索引已读、
            // 覆写未落"窗口);go 后直写新槽 R2 + 改指索引(镜像 updateCold 的磁盘效果)
            CountDownLatch mDecoded = new CountDownLatch(1);
            CountDownLatch go = new CountDownLatch(1);
            Thread coldWriter = new Thread(() -> {
                Object lock = persistence.acquireColdLock(id);
                synchronized (lock) {
                    SlotLocation latest = idx.get(id);
                    Intent cur = store.readSlot(latest);
                    mDecoded.countDown();
                    try {
                        go.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    cur.setTags(Map.of("v", "cold-r2"));
                    cur.incrementRevision();               // R2
                    persistence.persistToWheel(cur, false);
                }
                persistence.releaseColdLock(id, lock);
            });
            coldWriter.start();
            assertTrue(mDecoded.await(30, TimeUnit.SECONDS));

            // T:投递终态原地覆写(R2)。修复后阻塞在冷锁上直到冷写者临界区退出
            Thread terminal = new Thread(() -> {
                Intent terminalCopy = planted.copy();
                terminalCopy.transitionTo(IntentStatus.CANCELED);
                terminalCopy.incrementRevision();          // R2——与冷写者同 revision 源
                persistence.persistTerminalInPlace(terminalCopy);
            });
            terminal.start();
            // 红态(无互斥):T 无锁阻塞,立即完成 get→覆写,join 确定性地先于 go;
            // 绿态:T 阻塞在冷锁上,2s 超时属预期,join 后由 go 放行冷写者
            terminal.join(2_000);
            go.countDown();
            terminal.join(30_000);
            coldWriter.join(30_000);
            assertFalse(terminal.isAlive(), "persistTerminalInPlace 不得死锁");
            assertFalse(coldWriter.isAlive(), "冷命令临界区不得死锁");

            // 平局判定:该 id 的 max-revision 槽必须唯一——红态 X(terminal R2)与
            // Y(SCHEDULED R2)同 revision 平票,恢复按扫描序仲裁;绿态唯一
            long maxRev = -1;
            int countAtMax = 0;
            Intent maxSlot = null;
            var slots = store.scanSlotsFrom(Instant.now().minusSeconds(60));
            while (slots.hasNext()) {
                SlotEntry e = slots.next();
                if (!e.intent().getIntentId().equals(id)) {
                    continue;
                }
                long r = e.intent().getRevision();
                if (r > maxRev) {
                    maxRev = r;
                    countAtMax = 1;
                    maxSlot = e.intent();
                } else if (r == maxRev) {
                    countAtMax++;
                }
            }
            assertEquals(1, countAtMax,
                "同 revision 平局必须被冷锁互斥关闭(terminal R2 vs SCHEDULED R2)");
            assertEquals(2, maxRev, "终态/冷写总有一方以更高 revision 胜出");
            assertEquals(IntentStatus.CANCELED, maxSlot.getStatus(),
                "投递已发生,终态覆写胜出(语义归属残留,非仲裁不确定)");
        } finally {
            barrier.close();
            tail.close();
            store.close();
        }
    }

    /** schedule 注入抛错:使 createIntent 在 persistToWheel(R1)成功后的热载调度段失败,确定性进入补偿路径。 */
    private static final class ThrowingScheduleScheduler extends PrecisionScheduler {
        ThrowingScheduleScheduler(ConcurrentIntentStore store, DeliveryHandler handler) {
            super(store, handler, null);
        }

        @Override public void schedule(Intent intent) {
            throw new IllegalStateException("injected schedule failure (F5)");
        }
    }

    /** persistToWheel 第 2 次调用(= 补偿段内)暂停:把补偿线程固定在冷锁临界区内供互斥探针观测。 */
    private static final class CompPausePersistence extends WheelPersistence {
        final CountDownLatch compPaused = new CountDownLatch(1);
        final CountDownLatch compProceed = new CountDownLatch(1);
        private final AtomicInteger persistCalls = new AtomicInteger();

        CompPausePersistence(WheelStore ws, TailIndex ti, GroupCommitBarrier b, IntentLocationIndex idx) {
            super(ws, ti, b, idx);
        }

        @Override SlotLocation persistToWheel(Intent intent, boolean durable) {
            if (persistCalls.incrementAndGet() == 2) {   // #1 = create 主写入;#2 = 补偿段内
                compPaused.countDown();
                try {
                    compProceed.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.persistToWheel(intent, durable);
        }
    }

    @Test
    @Tag("slow")
    @org.junit.jupiter.api.Timeout(120)
    @org.junit.jupiter.api.DisplayName("F5:create 失败补偿段(index.remove→persistToWheel)持 per-id 冷锁——补偿临界区与冷命令互斥,冷命令探针不得穿越")
    void compensateCancelSerializesOnColdLock() throws Exception {
        Path dir = tmp.resolve("f5comp");
        WheelConfig cfg = new WheelConfig(dir.toString(), 30, 16, 1, 10_000L, HOT_BOUNDARY_MS, PROMOTION_LEAD_MS);
        WheelStore store = new WheelStore(cfg, System::currentTimeMillis);
        TailIndex tail = new TailIndex(dir, System::currentTimeMillis);
        GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 1, 10_000);
        barrier.start();
        try {
            IntentLocationIndex idx = new IntentLocationIndex();
            CompPausePersistence persistence = new CompPausePersistence(store, tail, barrier, idx);
            ConcurrentIntentStore memStore = new ConcurrentIntentStore();
            PrecisionScheduler scheduler = new ThrowingScheduleScheduler(memStore,
                intent -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER));
            PromotionDaemon daemon = new PromotionDaemon(store, tail, idx, System::currentTimeMillis,
                (i, l) -> { }, PROMOTION_LEAD_MS);
            IntentCreator creator = new IntentCreator(memStore, scheduler, daemon, new MetricsCollector(),
                new IntentTraceStore(), new AtomicLong(), null, 1L, HOT_BOUNDARY_MS,
                PrecisionTierCatalog.defaultCatalog(), store, tail, idx, persistence);

            String id = "intent_f5comp0001";
            Intent created = new Intent(id);
            created.setExecuteAt(Instant.now().plus(1, ChronoUnit.MINUTES));
            created.setPrecisionTier(PrecisionTier.STANDARD);

            ExecutorService es = Executors.newSingleThreadExecutor();
            Future<?> createF = es.submit(() -> {
                // 补偿后按"创建失败"重抛 RuntimeException——顺带锁定补偿路径被确定性触发
                assertThrows(RuntimeException.class, () -> creator.createIntent(created, AckMode.ASYNC));
                return null;
            });

            // 补偿线程已进入补偿段并停在 persistToWheel(第 2 次调用)——修复后此处持冷锁
            assertTrue(persistence.compPaused.await(30, TimeUnit.SECONDS),
                "补偿线程未到达 persistToWheel 暂停点");

            // 冷命令互斥探针:acquire→synchronized——修复后必须被补偿临界区阻塞
            CountDownLatch probeEntered = new CountDownLatch(1);
            Thread probe = new Thread(() -> {
                Object lock = persistence.acquireColdLock(id);
                synchronized (lock) {
                    probeEntered.countDown();
                }
            });
            probe.start();
            boolean crossedDuringSegment = probeEntered.await(200, TimeUnit.MILLISECONDS);
            assertFalse(crossedDuringSegment,
                "补偿冷锁临界区期间冷命令探针不得穿越(F5:补偿段须持 per-id 冷锁)");

            persistence.compProceed.countDown();
            createF.get(30, TimeUnit.SECONDS);
            probe.join(30_000);
            es.shutdownNow();

            // 补偿语义保持(不加复核):CANCELED 终态落新槽并指向索引,revision = 创建 R1 + 补偿 increment = R2
            SlotLocation compLoc = idx.get(id);
            assertNotNull(compLoc, "补偿后索引指向 CANCELED 槽");
            Intent disk = store.readSlot(compLoc);
            assertEquals(IntentStatus.CANCELED, disk.getStatus(), "补偿 CANCELED 终态落盘");
            assertEquals(2, disk.getRevision(), "补偿 revision:创建 R1 + 补偿 increment = R2");
        } finally {
            barrier.close();
            tail.close();
            store.close();
        }
    }
}
