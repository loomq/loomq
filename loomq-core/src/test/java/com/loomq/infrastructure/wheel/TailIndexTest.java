package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TailIndexTest {
    @TempDir Path tmp;

    @Test
    void shouldStoreAndScanInOrder() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (TailIndex tail = new TailIndex(tmp, clock::get)) {
            tail.put(make("intent_b000000000002", clock.get() + 50L * 86_400_000L));
            tail.put(make("intent_a000000000001", clock.get() + 45L * 86_400_000L));
            List<TailEntry> all = new ArrayList<>();
            tail.scanFrom(0).forEachRemaining(all::add);
            assertEquals(2, all.size());
            assertTrue(all.get(0).executeAtMs() < all.get(1).executeAtMs());
        }
    }

    @Test
    void shouldPromoteEntriesEnteringHorizon() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            long execAt = clock.get() + 40L * 86_400_000L; // +40d, 在 tail
            tail.put(make("intent_p000000000001", execAt));
            // 推进时钟到距 executeAt 10 天(进入 day 视界)
            clock.set(execAt - 10L * 86_400_000L);
            int promoted = tail.promoteInto(store);
            assertEquals(1, promoted);
            assertEquals(0, count(tail));
        }
    }

    @Test
    void shouldSurviveRestartViaRunFile() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (TailIndex t1 = new TailIndex(tmp, clock::get)) {
            t1.put(make("intent_a000000000001", clock.get() + 45L * 86_400_000L));
            t1.put(make("intent_b000000000002", clock.get() + 50L * 86_400_000L));
            t1.flush(); // force to disk
        }
        // Reopen: loadRun rebuilds in-memory state from the run file
        try (TailIndex t2 = new TailIndex(tmp, clock::get)) {
            List<TailEntry> all = new ArrayList<>();
            t2.scanFrom(0).forEachRemaining(all::add);
            assertEquals(2, all.size(), "tail entries must survive restart via run file");
        }
    }

    @Test
    void shouldHandleSameExecuteAtMsCollisions() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        long sameMs = clock.get() + 45L * 86_400_000L;
        try (TailIndex t = new TailIndex(tmp, clock::get)) {
            t.put(make("intent_same000000001", sameMs));
            t.put(make("intent_same000000002", sameMs)); // same executeAtMs — must NOT overwrite
            List<TailEntry> all = new ArrayList<>();
            t.scanFrom(0).forEachRemaining(all::add);
            assertEquals(2, all.size(), "same-ms intents must coexist");
        }
    }

    @Test
    void shouldTruncateTornTrailingRecordOnRestart() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (TailIndex t1 = new TailIndex(tmp, clock::get)) {
            t1.put(make("intent_good00000001", clock.get() + 45L * 86_400_000L));
            t1.flush();
        }
        // Corrupt the run file: append a partial (torn) record — just the type+execMs header, no id/slot
        Path run = tmp.resolve("tail").resolve("tail.log");
        try (FileChannel ch = FileChannel.open(run, StandardOpenOption.WRITE)) {
            ch.position(ch.size());
            ByteBuffer torn = ByteBuffer.allocate(9);
            torn.put((byte) 1).putLong(clock.get() + 50L * 86_400_000L); // PUT type + execMs, no id/slot
            torn.flip();
            ch.write(torn);
        }
        // Reopen: loadRun must detect the torn record, truncate it, and the good entry must survive
        try (TailIndex t2 = new TailIndex(tmp, clock::get)) {
            List<TailEntry> all = new ArrayList<>();
            t2.scanFrom(0).forEachRemaining(all::add);
            assertEquals(1, all.size(), "torn trailing record must be truncated; good entry survives");
            // And a new put must land cleanly (not poison the log)
            t2.put(make("intent_new0000000002", clock.get() + 55L * 86_400_000L));
            t2.flush();
        }
        try (TailIndex t3 = new TailIndex(tmp, clock::get)) {
            List<TailEntry> all = new ArrayList<>();
            t3.scanFrom(0).forEachRemaining(all::add);
            assertEquals(2, all.size(), "after truncation + new put, both good entries survive");
            // Strengthen: without truncation the torn 9-byte header (execMs +50d) would be misparsed
            // as a PUT consuming the following real record's bytes as its slot → a phantom entry
            // carrying execMs +50d survives and the real +55d entry is lost. Assert exactly the two
            // good execMs values and absence of the +50d phantom.
            long good1 = clock.get() + 45L * 86_400_000L;
            long good2 = clock.get() + 55L * 86_400_000L;
            long tornPhantom = clock.get() + 50L * 86_400_000L;
            List<Long> execMs = all.stream().map(TailEntry::executeAtMs).sorted().toList();
            assertEquals(List.of(good1, good2), execMs, "exactly the two good entries survive; no phantom");
            for (TailEntry e : all) {
                assertNotEquals(tornPhantom, e.executeAtMs(), "torn-record phantom (+50d) must not survive");
            }
        }
    }

    private Intent make(String id, long execAtMs) {
        Intent it = new Intent(id);
        it.setExecuteAt(Instant.ofEpochMilli(execAtMs));
        it.transitionTo(IntentStatus.SCHEDULED);
        return it;
    }
    private int count(TailIndex t) { int[] n={0}; t.scanFrom(0).forEachRemaining(e -> n[0]++); return n[0]; }
}
