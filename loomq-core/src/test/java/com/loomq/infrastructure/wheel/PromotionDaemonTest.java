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
    void shouldPromoteColdIntentAtExecuteAtMinusHotWindow() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, null);
        AtomicReference<Intent> promoted = new AtomicReference<>();
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             IntentLocationIndex idx = new IntentLocationIndex();
             PromotionDaemon daemon = new PromotionDaemon(store, tail, idx, clock::get, promoted::set)) {
            // +90min → hour 轮(冷)
            Intent it = new Intent("intent_promo000000001");
            it.setExecuteAt(Instant.ofEpochMilli(clock.get() + 90 * 60_000L));
            it.transitionTo(IntentStatus.SCHEDULED);
            SlotLocation loc = store.put(it);
            idx.put(it.getIntentId(), loc);                 // mirror createIntent: index + register
            daemon.register(it.getIntentId(), loc, it.getExecuteAt().toEpochMilli());

            // 唤醒时间 = executeAt - 60min;此刻未到
            daemon.tickOnce();
            assertNull(promoted.get(), "未到提升时间,不应载入");

            // 推进时钟到 executeAt-60min(唤醒时刻)
            clock.set(it.getExecuteAt().toEpochMilli() - PromotionDaemon.HOT_WINDOW_MS);
            daemon.tickOnce();
            assertNotNull(promoted.get(), "到提升时刻应从磁盘载入内存");
            assertEquals(it.getIntentId(), promoted.get().getIntentId());
        }
    }

    @Test
    void shouldRemoveColdRegistrationOnCancel() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, null);
        AtomicReference<Intent> promoted = new AtomicReference<>();
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             IntentLocationIndex idx = new IntentLocationIndex();
             PromotionDaemon daemon = new PromotionDaemon(store, tail, idx, clock::get, promoted::set)) {
            Intent it = new Intent("intent_cancel0000001");
            it.setExecuteAt(Instant.ofEpochMilli(clock.get() + 90 * 60_000L));
            it.transitionTo(IntentStatus.SCHEDULED);
            SlotLocation loc = store.put(it);
            idx.put(it.getIntentId(), loc);                 // mirror createIntent: index + register
            daemon.register(it.getIntentId(), loc, it.getExecuteAt().toEpochMilli());

            assertTrue(daemon.remove(it.getIntentId()));
            clock.set(it.getExecuteAt().toEpochMilli() - PromotionDaemon.HOT_WINDOW_MS);
            daemon.tickOnce();
            assertNull(promoted.get(), "取消后不应再提升");
        }
    }
}
