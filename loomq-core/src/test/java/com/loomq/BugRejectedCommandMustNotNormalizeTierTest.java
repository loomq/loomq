package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 被拒绝的 create/update 不应因为 null precisionTier 归一化而修改调用方传入的 Intent。
 */
class BugRejectedCommandMustNotNormalizeTierTest {

    @TempDir Path tmp;

    private static final DeliveryHandler SUCCESS =
        i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @Test
    void duplicateCreateRejectionMustNotMutatePrecisionTier() throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("d")).nodeId("d1")
                .deliveryHandler(SUCCESS)
                .build()) {
            engine.start();
            Intent first = new Intent("dup-tier-0001");
            first.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60_000));
            first.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(first, AckMode.DURABLE).get();

            Intent duplicate = new Intent("dup-tier-0001");
            duplicate.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60_000));
            duplicate.setPrecisionTier(null);

            assertThrows(CompletionException.class,
                () -> engine.createIntent(duplicate, AckMode.DURABLE).join(),
                "duplicate create must be rejected");
            assertNull(duplicate.getPrecisionTier(),
                "rejected duplicate create must not normalize the caller's Intent");
        }
    }

    @Test
    void failedUpdateValidationMustNotNormalizePrecisionTier() throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("u")).nodeId("u1")
                .deliveryHandler(SUCCESS)
                .build()) {
            engine.start();
            long t0 = System.currentTimeMillis();
            Intent it = new Intent("upd-tier-0001");
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 60_000));
            it.setDeadline(Instant.ofEpochMilli(t0 + 120_000));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            assertThrows(IllegalArgumentException.class,
                () -> engine.updateIntent("upd-tier-0001", i -> {
                    i.setPrecisionTier(null);
                    i.setDeadline(Instant.ofEpochMilli(t0 + 30_000));
                }),
                "invalid update must be rejected");

            // 失败更新不得把调用方传入的 null 归一化,但活对象的调度字段必须还原到
            // updater 前值——precisionTier 是调度字段(决定 cohort/桶路由),污染不得残留。
            assertEquals(PrecisionTier.ULTRA,
                engine.getIntentStoreInternal().findByIdInternal("upd-tier-0001").getPrecisionTier(),
                "failed update validation must restore the original precisionTier, not keep the updater's null");
        }
    }

    @Test
    void failedRescheduleUpdateMustRestorePrecisionTierAndSchedule() throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("r")).nodeId("r1")
                .deliveryHandler(SUCCESS)
                .build()) {
            engine.start();
            long t0 = System.currentTimeMillis();
            Intent it = new Intent("upd-tier-rs-0001");
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 60_000));
            it.setDeadline(Instant.ofEpochMilli(t0 + 120_000));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            // 改期触发 removeFromSchedule(removedForReschedule=true),随后校验失败——
            // 回滚除还原 executeAt/status 外,还必须还原 precisionTier,并按原时刻重排。
            assertThrows(IllegalArgumentException.class,
                () -> engine.updateIntent("upd-tier-rs-0001",
                    i -> {
                        i.setPrecisionTier(null);
                        i.setDeadline(Instant.ofEpochMilli(t0 + 30_000));
                    },
                    Instant.ofEpochMilli(t0 + 90_000)),
                "invalid reschedule update must be rejected");

            Intent internal = engine.getIntentStoreInternal().findByIdInternal("upd-tier-rs-0001");
            assertEquals(PrecisionTier.ULTRA, internal.getPrecisionTier(),
                "failed reschedule update must restore precisionTier");
            assertEquals(Instant.ofEpochMilli(t0 + 60_000), internal.getExecuteAt(),
                "failed reschedule update must restore the original executeAt");
            assertEquals(IntentStatus.SCHEDULED, internal.getStatus(),
                "failed reschedule update must restore the original status");
        }
    }
}
