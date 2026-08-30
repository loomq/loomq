package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.spi.DeliveryHandler;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * C18-2(r18): W4 stale-skip 后结算链必须中止重排与内存镜像。旧实现 skip 仅 warn+回滚,
 * 之后 updateStoreBestEffort 无条件 upsert(ConcurrentIntentStore.update 对已删 id 直接重插)
 * 复活 demote 副本、rescheduler.schedule 幽灵重投 stale 副本——幽灵投递端到端成立。
 * 骨架确定性(无真实竞态窗口):真实组件栈,DeliveryHandler 首投阻塞在 latch,阻塞期主线程
 * advanceDisk(冷写者权威前进)+ demote 等价簿记,放行 handler 返回 RETRY 走完整结算链。
 */
class BugW4StaleSkipAbortsGhostRescheduleTest {

    /** 短 backoff(300ms fixed)使幽灵重投窗口落在 2s 断言窗内。 */
    private static final RedeliveryPolicy FAST_RETRY =
        new RedeliveryPolicy(5, "fixed", 300L, 300L, 1.0, false);

    @TempDir Path tmp;

    @Test
    @org.junit.jupiter.api.Timeout(60)
    @org.junit.jupiter.api.DisplayName("C18-2: stale skip 后不得复活 demote 副本/幽灵重投——invocations 恒 1")
    void staleSkipMustNotResurrectOrGhostReschedule() throws Exception {
        CountDownLatch inDelivery = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        CommandStackFx.Options opts = new CommandStackFx.Options(System::currentTimeMillis, null, true, true,
            intent -> {
                invocations.incrementAndGet();
                inDelivery.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.RETRY);
            });
        String id = "intent_w4ghost0001";
        try (CommandStackFx fx = new CommandStackFx(tmp.resolve("w4ghost"), opts)) {
            // 冷权威 R1(+2s 近到期);seedMaxRevisions=true 已按 recovery 语义种子
            Intent cold = fx.plantColdWheel(id, Map.of("v", "r1"), System.currentTimeMillis() + 2_000L);
            cold.setRedelivery(FAST_RETRY);
            fx.plantHotSync(cold);   // 手工模拟 promote 热载:upsert + schedule(promote 等价簿记)

            assertTrue(inDelivery.await(10, TimeUnit.SECONDS), "首次投递未发生");
            // 阻塞窗口:冷写者权威前进 + demote 语义等价簿记(摘热副本)
            fx.advanceDisk(cold, Map.of("v", "r2"));   // 磁盘 R2@新槽,idx 改指,种子推进
            fx.memStore().delete(id);                   // demote:冷写者收口热副本
            fx.scheduler().removeFromSchedule(cold);    // 在途投递已被 scanDue CAS 认领,分离失败属预期
            release.countDown();

            // 修复后:结算 stale 中止——不镜像(无 upsert)、不重排(无 schedule)→ 恒 1 次投递;
            // 修复前(红):updateStoreBestEffort 复活 demote 副本 + 幽灵重投 → invocations ≥2
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline && invocations.get() < 2) {
                Thread.sleep(20);
            }
            assertEquals(1, invocations.get(),
                "stale skip 后不得幽灵重投(投递义务由冷写者权威副本接管)");
            assertNull(fx.memStore().findByIdInternal(id),
                "stale 中止不得复活 demote 的热副本(ConcurrentIntentStore.update 会直接重插)");
            SlotLocation latest = fx.idx().get(id);
            Intent disk = fx.store().readSlot(latest);
            assertEquals(2, disk.getRevision(), "磁盘保持冷写者权威 revision(R2)");
            assertEquals("r2", disk.getTags().get("v"), "磁盘保持冷写者权威内容");
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(60)
    @org.junit.jupiter.api.DisplayName("C18-2 正向对照: fresh 重试路径行为不变——重排+重投+ACKED 原地落盘")
    void freshRetryStillReschedulesAndDelivers() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        CommandStackFx.Options opts = new CommandStackFx.Options(System::currentTimeMillis, null, true, true,
            intent -> {
                int n = invocations.incrementAndGet();
                return CompletableFuture.completedFuture(
                    n == 1 ? DeliveryHandler.DeliveryResult.RETRY : DeliveryHandler.DeliveryResult.SUCCESS);
            });
        String id = "intent_w4ctl00001";
        try (CommandStackFx fx = new CommandStackFx(tmp.resolve("w4ctl"), opts)) {
            Intent cold = fx.plantColdWheel(id, Map.of("v", "r1"), System.currentTimeMillis() + 2_000L);
            cold.setRedelivery(FAST_RETRY);
            fx.plantHotSync(cold);   // fresh 热副本(R1 = 磁盘 R1)

            Intent acked = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                // r18 实测偏差:ACKED 终态原地覆写后 reclaimTerminal 按 C2-1 定向移除索引
                // (终态不索引),经 idx 轮询的观察窗为微秒级不可达——改扫 wheel 槽观测磁盘终态。
                java.util.Iterator<com.loomq.infrastructure.wheel.SlotEntry> it =
                    fx.store().scanSlotsFrom(java.time.Instant.ofEpochMilli(0));
                while (it.hasNext()) {
                    com.loomq.infrastructure.wheel.SlotEntry e = it.next();
                    if (e.intent().getIntentId().equals(id)
                            && e.intent().getStatus() == IntentStatus.ACKED) {
                        acked = e.intent();
                    }
                }
                if (acked != null) {
                    break;
                }
                Thread.sleep(20);
            }
            assertEquals(2, invocations.get(), "fresh 重试必须重排并第二次投递(正向对照,防改序误伤)");
            assertTrue(acked != null, "ACKED 终态须原地落盘");
            assertEquals(3, acked.getRevision(), "ACKED revision = 重试落盘 R2 + 终态递增 R3");
        }
    }
}
