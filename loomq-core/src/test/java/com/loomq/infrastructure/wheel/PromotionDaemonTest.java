package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PromotionDaemonTest {
    @TempDir Path tmp;

    @Test
    void shouldPromoteColdIntentAtExecuteAtMinusPromotionLead() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), 30, 16, 1, 10_000L, 60L * 60_000L, 30_000L);
        long leadMs = cfg.promotionLeadMs(); // 30s(非默认 60s,证明可配)
        AtomicReference<Intent> promoted = new AtomicReference<>();
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             IntentLocationIndex idx = new IntentLocationIndex();
             PromotionDaemon daemon = new PromotionDaemon(store, tail, idx, clock::get, (i, loc) -> promoted.set(i), leadMs)) {
            Intent it = new Intent("intent_promo000000001");
            it.setExecuteAt(Instant.ofEpochMilli(clock.get() + 90 * 60_000L));
            it.transitionTo(IntentStatus.SCHEDULED);
            SlotLocation loc = store.put(it);
            idx.put(it.getIntentId(), loc);
            daemon.register(it.getIntentId(), loc, it.getExecuteAt().toEpochMilli());

            // 唤醒时间 = executeAt - leadMs(30s);此刻未到
            daemon.tickOnce();
            assertNull(promoted.get(), "未到提升时间,不应载入");

            // 推进时钟到 executeAt-leadMs(唤醒时刻)
            clock.set(it.getExecuteAt().toEpochMilli() - leadMs);
            daemon.tickOnce();
            assertNotNull(promoted.get(), "到提升时刻应从磁盘载入内存");
            assertEquals(it.getIntentId(), promoted.get().getIntentId());
        }
    }

    @Test
    void shouldRemoveColdRegistrationOnCancel() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), 30, 16, 1, 10_000L, 60L * 60_000L, 30_000L);
        long leadMs = cfg.promotionLeadMs();
        AtomicReference<Intent> promoted = new AtomicReference<>();
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             IntentLocationIndex idx = new IntentLocationIndex();
             PromotionDaemon daemon = new PromotionDaemon(store, tail, idx, clock::get, (i, loc) -> promoted.set(i), leadMs)) {
            Intent it = new Intent("intent_cancel0000001");
            it.setExecuteAt(Instant.ofEpochMilli(clock.get() + 90 * 60_000L));
            it.transitionTo(IntentStatus.SCHEDULED);
            SlotLocation loc = store.put(it);
            idx.put(it.getIntentId(), loc);
            daemon.register(it.getIntentId(), loc, it.getExecuteAt().toEpochMilli());

            assertTrue(daemon.remove(it.getIntentId()));
            clock.set(it.getExecuteAt().toEpochMilli() - leadMs);
            daemon.tickOnce();
            assertNull(promoted.get(), "取消后不应再提升");
        }
    }
}
