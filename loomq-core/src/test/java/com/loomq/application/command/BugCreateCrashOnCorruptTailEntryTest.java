package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R21: hasActiveDuplicate 的 tail 分支裸 decode——R4/P1-6 只给 WheelRecovery 和 promoteInto
 * 加了防御解码,hasActiveDuplicate 扫描到"结构完整但槽字节损坏"(bit rot)的 tail 记录时
 * 抛 CRC ISE,直接穿透 createIntent。批量创建同一时刻(同 tail 键)的 intent 是现实场景:
 * 同键集合内损坏记录与正常记录共存,任一创建请求都会被损坏条目炸掉。
 *
 * <p>修复:与恢复路径同款防御——解码失败跳过并告警(损坏即视为不存在,与 recovery 语义
 * 一致:恢复时损坏条目同样被跳过,索引本就不该引用它)。</p>
 */
class BugCreateCrashOnCorruptTailEntryTest {

    @TempDir
    Path tmp;

    @Test
    void createMustSkipCorruptTailEntryInsteadOfCrashing() throws Exception {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        long execMs = clock.get() + 40L * 24 * 3600 * 1000; // +40d → tail 视界

        // 1. 写入一条 tail 记录后关闭
        try (TailIndex tail = new TailIndex(tmp, clock::get)) {
            Intent intent = new Intent("r21-corrupt-0001");
            intent.setExecuteAt(Instant.ofEpochMilli(execMs));
            intent.transitionTo(IntentStatus.SCHEDULED);
            intent.incrementRevision();
            tail.put(intent);
        }

        // 2. 翻转槽 payload 内一个字节(结构完整、CRC 损坏——bit rot 模拟)
        Path runFile = tmp.resolve("tail").resolve("tail.log");
        byte[] all = Files.readAllBytes(runFile);
        int idLen = "r21-corrupt-0001".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        int slotStart = 1 + 8 + 1 + idLen; // type + execMs + idLen + id
        all[slotStart + 50] ^= 0x55; // 槽 payload 区
        Files.write(runFile, all);

        // 3. 重开:loadRun 保留结构完整的损坏槽(与恢复侧防御解码跳过的是同一形态)
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, 60L * 60_000L, 60_000L,
            PrecisionTier.STANDARD);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail2 = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail2, 1, 10_000)) {

            IntentLocationIndex idx = new IntentLocationIndex();
            ConcurrentIntentStore memStore = new ConcurrentIntentStore();
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong seq = new AtomicLong();
            MetricsCollector mc = new MetricsCollector();
            PrecisionScheduler scheduler = new PrecisionScheduler(memStore, i ->
                CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER), null);
            PromotionDaemon daemon = new PromotionDaemon(store, tail2, idx, clock::get, (i, loc) -> {}, 60_000L);
            ExecutorService cb = Executors.newVirtualThreadPerTaskExecutor();
            IntentCommandService svc = new IntentCommandService(
                memStore, scheduler, store, tail2, barrier, idx, daemon,
                mc, cb, running, seq, null, PrecisionTier.STANDARD, 1L, 60L * 60_000L);
            barrier.start();
            daemon.start();

            // 恢复侧索引状态:损坏条目被恢复跳过,索引残留指向损坏记录占据的键
            idx.put("r21-corrupt-0001", SlotLocation.tail(execMs));

            // 修复前:hasActiveDuplicate 解码损坏槽 → CRC ISE 穿透 createIntent;
            // 修复后:跳过损坏条目 → 判无活重复 → 创建成功。
            Intent fresh = new Intent("r21-corrupt-0001");
            fresh.setExecuteAt(Instant.ofEpochMilli(execMs));
            fresh.setPrecisionTier(PrecisionTier.STANDARD);
            long s = svc.createIntent(fresh, AckMode.DURABLE);
            assertTrue(s > 0,
                "create must succeed once the corrupt tail entry is skipped (was: CRC ISE crashing the create)");
            cb.shutdown();
        }
    }
}
