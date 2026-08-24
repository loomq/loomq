package com.loomq.application.scheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import com.loomq.testutil.TestIntents;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * R15b: 批量窗口(batchWindowMs)此前为死配置——runBatchDrainConsumer 未引用它。修复:未满批次
 * 等待至多 batchWindowMs 累计更多 intent。本测试直接向 dispatch 队列投递(绕过 scanner,消除
 * 扫描时序的批次数不确定),确定性锁定:(1) 满批立即派发不等待窗口;(2) 未满批窗口到期仍派发。
 */
class BatchWindowAggregationTest {

    private IntentStore store;
    private PrecisionScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.stop();
    }

    /** 单消费者 STANDARD 批量目录,batchSize=4、batchWindowMs=100ms,便于观测批次聚合。 */
    private static PrecisionTierCatalog batchCatalog() {
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.ULTRA, new PrecisionTierProfile(10, 200, 1, 5, 1, 200 * 16,
            WalMode.DURABLE, 10, false, true, 200_000));
        profiles.put(PrecisionTier.FAST, new PrecisionTierProfile(50, 150, 1, 10, 1, 150 * 16,
            WalMode.DURABLE, 50, false, true, 200_000));
        profiles.put(PrecisionTier.STANDARD, new PrecisionTierProfile(500, 50, 4, 100, 1, 50 * 16,
            WalMode.DURABLE, 500, false, true, 200_000));
        profiles.put(PrecisionTier.MILLI, new PrecisionTierProfile(1, 100, 1, 1, 1, 100 * 16,
            WalMode.DURABLE, 1, true, true, 200_000));
        return PrecisionTierCatalog.of(profiles, PrecisionTier.STANDARD);
    }

    /** 经反射直达 STANDARD dispatch 队列,绕过 scanner(消除扫描时序的不确定性)。
     *  队列已随 Task 4 迁入 DispatchPipeline,经 scheduler.pipeline 反射取得。 */
    private void offerToDispatchQueue(List<Intent> intents) throws Exception {
        Field pf = PrecisionScheduler.class.getDeclaredField("pipeline");
        pf.setAccessible(true);
        Object pipeline = pf.get(scheduler);
        Field qf = DispatchPipeline.class.getDeclaredField("tierDispatchQueues");
        qf.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<PrecisionTier, BlockingQueue<Intent>> queues =
            (Map<PrecisionTier, BlockingQueue<Intent>>) qf.get(pipeline);
        BlockingQueue<Intent> queue = queues.get(PrecisionTier.STANDARD);
        for (Intent intent : intents) {
            store.save(intent);
            queue.offer(intent);
        }
    }

    @Test
    void fullBatchDispatchesWithoutWaitingForWindow() throws Exception {
        store = new ConcurrentIntentStore();
        List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch delivered = new CountDownLatch(8);
        DeliveryHandler handler = new DeliveryHandler() {
            @Override
            public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
                delivered.countDown();
                return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
            }
            @Override
            public List<CompletableFuture<DeliveryResult>> deliverBatchAsync(List<Intent> intents) {
                batchSizes.add(intents.size());
                List<CompletableFuture<DeliveryResult>> out = new ArrayList<>(intents.size());
                for (Intent i : intents) {
                    delivered.countDown();
                    out.add(CompletableFuture.completedFuture(DeliveryResult.SUCCESS));
                }
                return out;
            }
        };
        scheduler = new PrecisionScheduler(store, handler, null, batchCatalog());
        scheduler.start();

        List<Intent> intents = new ArrayList<>(8);
        for (int i = 0; i < 8; i++) intents.add(TestIntents.due("batch-full-" + i, PrecisionTier.STANDARD, Instant.now().minusMillis(5)));
        offerToDispatchQueue(intents);

        assertTrue(delivered.await(5, TimeUnit.SECONDS), "all 8 intents delivered");
        // 8 个 intent 全部在队列中,满批路径应立即聚成 2 个满批(4+4),不等待 100ms 窗口。
        assertEquals(List.of(4, 4), batchSizes,
            "burst of 8 with batchSize=4 must form exactly two full batches (no window wait): " + batchSizes);
    }

    @Test
    void partialBatchStillDeliversAfterWindow() throws Exception {
        store = new ConcurrentIntentStore();
        List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch delivered = new CountDownLatch(3);
        DeliveryHandler handler = new DeliveryHandler() {
            @Override
            public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
                delivered.countDown();
                return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
            }
            @Override
            public List<CompletableFuture<DeliveryResult>> deliverBatchAsync(List<Intent> intents) {
                batchSizes.add(intents.size());
                List<CompletableFuture<DeliveryResult>> out = new ArrayList<>(intents.size());
                for (Intent i : intents) {
                    delivered.countDown();
                    out.add(CompletableFuture.completedFuture(DeliveryResult.SUCCESS));
                }
                return out;
            }
        };
        scheduler = new PrecisionScheduler(store, handler, null, batchCatalog());
        scheduler.start();

        List<Intent> intents = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) intents.add(TestIntents.due("batch-partial-" + i, PrecisionTier.STANDARD, Instant.now().minusMillis(5)));
        offerToDispatchQueue(intents);

        // 未满批(3 < 4)须在窗口到期后仍派发,而非因等待窗口而卡死。
        assertTrue(delivered.await(5, TimeUnit.SECONDS), "partial batch must deliver after window");
        assertEquals(List.of(3), batchSizes,
            "3 intents with batchSize=4 must deliver as one partial batch: " + batchSizes);
    }
}
