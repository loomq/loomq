package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 冷改期(round 13)读路径:按 intentId 键读 tail 记录。 */
class TailIndexReadByIdTest {
    @TempDir Path tmp;

    private static final long BASE = Instant.parse("2026-06-30T00:00:00Z").toEpochMilli();

    private Intent plant(String id, long execMs, long revision) {
        Intent cold = new Intent(id);
        cold.setExecuteAt(Instant.ofEpochMilli(execMs));
        cold.setPrecisionTier(PrecisionTier.STANDARD);
        cold.transitionTo(IntentStatus.SCHEDULED);
        for (long i = 0; i < revision; i++) cold.incrementRevision();
        return cold;
    }

    @Test
    void readByIdReturnsDecodedIntentForExistingRecord() {
        AtomicLong clock = new AtomicLong(BASE);
        try (TailIndex tail = new TailIndex(tmp, clock::get)) {
            long execMs = BASE + 31L * 24 * 60 * 60 * 1000L;   // >30d 视界
            tail.put(plant("intent_rbi00000001", execMs, 1));

            Intent read = tail.readById("intent_rbi00000001");
            assertNotNull(read);
            assertEquals("intent_rbi00000001", read.getIntentId());
            assertEquals(execMs, read.getExecuteAt().toEpochMilli());
            assertEquals(1, read.getRevision());
        }
    }

    @Test
    void readByIdReturnsNullForMissingId() {
        AtomicLong clock = new AtomicLong(BASE);
        try (TailIndex tail = new TailIndex(tmp, clock::get)) {
            assertNull(tail.readById("intent_absent000001"));
        }
    }

    @Test
    void readByIdReflectsLatestPutAfterRetarget() {
        AtomicLong clock = new AtomicLong(BASE);
        try (TailIndex tail = new TailIndex(tmp, clock::get)) {
            long execMs = BASE + 31L * 24 * 60 * 60 * 1000L;
            Intent cold = plant("intent_rbi00000002", execMs, 1);
            tail.put(cold);
            // 模拟 tail→tail 改期:更高 revision + 新 executeAt 再 put(byId 单活换位)
            cold.setExecuteAt(Instant.ofEpochMilli(execMs + 24 * 60 * 60 * 1000L));
            cold.incrementRevision();
            tail.put(cold);

            Intent read = tail.readById("intent_rbi00000002");
            assertNotNull(read);
            assertEquals(execMs + 24 * 60 * 60 * 1000L, read.getExecuteAt().toEpochMilli());
            assertEquals(2, read.getRevision());
            assertEquals(1, tail.size(), "byId 单活:重 put 后 size 仍为 1");
        }
    }
}
