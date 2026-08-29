package com.loomq;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LoomqEnginePhtwIntegrationTest {
    @TempDir Path tmp;

    @Test
    void createAndCancelIntentWithPhtw() throws Exception {
        LoomqEngine engine = LoomqEngine.builder().walDir(tmp).build();
        engine.start();
        try {
            Intent it = new Intent();
            it.setExecuteAt(Instant.now().plusSeconds(2));
            long seq = engine.createIntent(it, AckMode.DURABLE).join();
            assertTrue(seq > 0);

            Optional<Intent> got = engine.getIntent(it.getIntentId());
            assertTrue(got.isPresent());
            assertEquals(IntentStatus.SCHEDULED, got.get().getStatus());

            assertTrue(engine.cancelIntent(it.getIntentId()));
        } finally {
            engine.close();
        }
    }

    /**
     * 冷取消不复活:创建冷(>60min)DURABLE Intent → cancelCold → 断言索引已清、不在内存;
     * 重启引擎后 recovery 按 max revision 取 CANCELED 胜者(terminal 跳过),不复活入内存。
     *
     * <p>B1 修复验证:cancelCold 在 awaitCommit 之前把索引指向新 CANCELED 槽,关闭"在途 promote
     * 复活"窗口(promote 见 latest≠h.loc() 跳过;即便读到新槽也是 terminal 跳过)。该在途窗口因
     * LoomqEngine 无可控时钟缝隙不单独测;本测试证明 cancel 已持久化(CANCELED 新槽 revision 更高、
     * 索引清除)且 recovery 按 max revision 去重到 CANCELED(而非残留 SCHEDULED),不复活。</p>
     */
    @Test
    void cancelColdIntentPersistsAndDoesNotResurrectOnRecovery() throws Exception {
        // Cold intent: executeAt > 60min from now → lives on disk + promotion cohort, NOT in memory.
        LoomqEngine engine = LoomqEngine.builder().walDir(tmp).build();
        engine.start();
        Intent cold;
        SlotLocation scheduledLoc;
        try {
            cold = new Intent();
            cold.setExecuteAt(Instant.now().plusSeconds(90 * 60L)); // +90min → cold
            engine.createIntent(cold, AckMode.DURABLE).join();

            // Cold intent is on disk only — not loaded into the memory store.
            assertTrue(engine.getIntent(cold.getIntentId()).isEmpty(),
                "cold intent should not be in memory before promotion");

            // Capture the SCHEDULED loc the create indexed — the stale slot append-only leaves behind.
            scheduledLoc = engine.getLocationIndex().get(cold.getIntentId());
            assertNotNull(scheduledLoc, "cold intent must be indexed at its SCHEDULED loc after create");

            // Cancel via the cold path (cancelCold): reads slot, transitions to CANCELED, appends
            // a new CANCELED slot (append-only), points the index at the new CANCELED loc BEFORE
            // awaitCommit (closes the in-flight promote resurrection window), awaits commit, then
            // removes index + cohort.
            assertTrue(engine.cancelIntent(cold.getIntentId()),
                "cancelCold should succeed for a cold intent");

            // Post-cancel: still absent from memory, AND removed from the location index
            // (cancelCold removed it — proving the index re-check in promote() would skip it).
            assertTrue(engine.getIntent(cold.getIntentId()).isEmpty(),
                "cancelled cold intent must not be in memory");
            assertNull(engine.getLocationIndex().get(cold.getIntentId()),
                "cancelled cold intent must be removed from the location index");
        } finally {
            engine.close();
        }

        // Reopen on the same walDir → recovery scans slots and dedups by max revision.
        // The CANCELED slot (higher revision) wins; terminal intents are skipped (not loaded
        // into memory, not registered for promotion). So the intent is NOT resurrected.
        SlotLocation recoveredLoc;
        LoomqEngine reopened = LoomqEngine.builder().walDir(tmp).build();
        reopened.start();
        try {
            assertTrue(reopened.getIntent(cold.getIntentId()).isEmpty(),
                "recovery must not resurrect a cancelled cold intent into memory");
            // Recovery indexes the max-revision (CANCELED) loc for every winner, including
            // terminal ones. A non-null loc here that is NOT the stale SCHEDULED loc proves
            // recovery dedup'd to the CANCELED slot — not the stale SCHEDULED.
            recoveredLoc = reopened.getLocationIndex().get(cold.getIntentId());
            assertNull(recoveredLoc,
                "recovery must not index terminal (CANCELED) intent (Spec B)");
        } finally {
            reopened.close();
        }

        // Read both slots directly from disk to nail down the dedup: the stale SCHEDULED slot
        // (append-only remnant, low revision) coexists with the new CANCELED slot (high revision);
        // recovery's max-revision winner is CANCELED, which is terminal → not scheduled/deliverable.
        WheelConfig cfg = WheelConfig.defaultConfig().withDataDir(tmp.toString());
        try (WheelStore scan = new WheelStore(cfg, System::currentTimeMillis)) {
            Intent scheduledSlot = scan.readSlot(scheduledLoc);
            assertNotNull(scheduledSlot, "stale SCHEDULED slot must still be on disk (append-only)");
            assertEquals(IntentStatus.SCHEDULED, scheduledSlot.getStatus());

        }
    }

    /**
     * B2 回归守卫:在途(或刚完成)DURABLE create 必须在 close 后仍可恢复——close() 的终态
     * group-commit force 排空在途写者,使槽位持久(不丢失,亦非"失败" create 的 ghost)。
     *
     * <p>注:group-commit 循环默认 1ms 间隔,create 通常在 close 前已完成 awaitCommit,故本测试
     * 主要守护"close 不丢已落盘的 DURABLE 写入"这一持久保证;close 排空在途写者的确定性验证由
     * {@code GroupCommitBarrierTest.closeDrainsInFlightAwaitCommit} 承担。</p>
     */
    @Test
    void closeDrainsInFlightDurableWriteAndRecoversOnReopen() throws Exception {
        LoomqEngine engine = LoomqEngine.builder().walDir(tmp).build();
        engine.start();
        Intent it = new Intent();
        it.setExecuteAt(Instant.now().plusSeconds(5 * 60L)); // hot (≤60min), won't fire during the test
        try {
            // Fire a DURABLE create and close WITHOUT awaiting its future — exercises close()'s
            // drain path for any writer still in awaitCommit.
            engine.createIntent(it, AckMode.DURABLE);
            // Brief pause so the create reaches wheelStore.put (bytes in mmap) before close forces.
            Thread.sleep(100);
        } finally {
            engine.close();
        }

        // Reopen on the same walDir: the DURABLE write must be present (recovered from disk),
        // not lost — and not a ghost of a create the caller believes failed.
        LoomqEngine reopened = LoomqEngine.builder().walDir(tmp).build();
        reopened.start();
        try {
            Optional<Intent> got = reopened.getIntent(it.getIntentId());
            assertTrue(got.isPresent(),
                "in-flight DURABLE write must be recovered after close (drained, not lost)");
            assertEquals(IntentStatus.SCHEDULED, got.get().getStatus());
        } finally {
            reopened.close();
        }
    }

    @Test
    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Timeout(30)
    @DisplayName("冷改期跨重启:改期后的 executeAt/内容在恢复后按新时间投递恰好一次")
    void coldRescheduleSurvivesRestartAndDeliversOnceAtNewTime() throws Exception {
        // hotBoundary=1s:+15s 冷(>1s);改期到 +8s(更新时仍冷);promotionLead=100ms——秒级完成 E2E
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, 1_000L, 100L,
            PrecisionTier.STANDARD);
        String id = "intent_rst00000001";
        LoomqEngine engine = LoomqEngine.builder().wheelConfig(cfg)
            .deliveryHandler(it -> java.util.concurrent.CompletableFuture.completedFuture(
                com.loomq.spi.DeliveryHandler.DeliveryResult.SUCCESS))
            .build();
        engine.start();
        try {
            Intent cold = new Intent(id);
            cold.setExecuteAt(Instant.now().plusSeconds(15));
            cold.setPrecisionTier(PrecisionTier.STANDARD);
            engine.createIntent(cold, AckMode.DURABLE).join();
            Optional<Intent> updated = engine.updateIntent(id, i -> i.setTags(Map.of("v", "new")),
                Instant.now().plusSeconds(8));
            assertTrue(updated.isPresent(), "冷改期必须生效(hotBoundary=1s,+8s 仍冷)");
            assertEquals("new", updated.get().getTags().get("v"));
        } finally {
            engine.close();
        }
        // 重启:恢复 + promotion cohort 按新时间(+8s)热载投递,内容为改期后
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicInteger deliveryCount = new AtomicInteger();
        AtomicReference<String> tags = new AtomicReference<>();
        LoomqEngine reopened = LoomqEngine.builder().wheelConfig(cfg)
            .deliveryHandler(it -> {
                if (id.equals(it.getIntentId())) {
                    tags.set(it.getTags().get("v"));
                    deliveryCount.incrementAndGet();
                    delivered.countDown();
                }
                return java.util.concurrent.CompletableFuture.completedFuture(
                    com.loomq.spi.DeliveryHandler.DeliveryResult.SUCCESS);
            })
            .build();
        reopened.start();
        try {
            assertTrue(delivered.await(20, TimeUnit.SECONDS), "改期后的新时间必须投递(约 +8s)");
            assertEquals("new", tags.get(), "投递内容必须是改期后的");
            assertEquals(1, deliveryCount.get(), "必须恰好投递一次(重复投递回归守卫)");
        } finally {
            reopened.close();
        }
    }
}
