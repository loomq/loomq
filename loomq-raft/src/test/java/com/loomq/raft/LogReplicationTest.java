package com.loomq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.store.ConcurrentIntentStore;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogReplicationTest {

    private Path dataDir;
    private SimpleWalWriter wal;
    private RaftLog raftLog;
    private StandaloneElection election;

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("logrepl-test-");
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false);
        wal = new SimpleWalWriter(cfg, "logrepl-test");
        raftLog = new RaftLog(wal);
        election = new StandaloneElection();
        election.start();
    }

    @AfterEach
    void tearDown() {
        election.stop();
        if (wal != null) wal.close();
    }

    private static Intent makeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    /** Set the private commitIndex field via reflection. */
    private static void setCommitIndex(LogReplication replication, long value) throws Exception {
        Field f = LogReplication.class.getDeclaredField("commitIndex");
        f.setAccessible(true);
        ((AtomicLong) f.get(replication)).set(value);
    }

    /**
     * Store that throws on the Nth upsert call (1-based).
     * Useful for simulating transient storage failures.
     */
    static class FailingIntentStore extends ConcurrentIntentStore {
        private final int failOnCall;
        private int callCount = 0;

        FailingIntentStore(int failOnCall) {
            this.failOnCall = failOnCall;
        }

        @Override
        public void upsert(Intent intent) {
            callCount++;
            if (callCount == failOnCall) {
                throw new RuntimeException("Simulated upsert failure on call " + callCount);
            }
            super.upsert(intent);
        }
    }

    /**
     * Verifies that lastApplied does not advance past a failed upsert.
     *
     * Setup: commitIndex=2, two valid entries at index 1 and 2.
     * Entry 1 upsert succeeds, entry 2 upsert throws.
     * Expected: lastApplied==1 after applyCommitted() (not 2).
     */
    @Test
    void applyCommitted_shouldNotAdvanceLastAppliedPastFailedUpsert() throws Exception {
        // -- write two entries to the real RaftLog --
        Intent intent1 = makeIntent("intent-1");
        Intent intent2 = makeIntent("intent-2");
        byte[] payload1 = IntentBinaryCodec.encode(intent1);
        byte[] payload2 = IntentBinaryCodec.encode(intent2);

        raftLog.appendEntry(1L, payload1); // index 1
        raftLog.appendEntry(1L, payload2); // index 2

        // -- store that fails on 2nd upsert --
        FailingIntentStore store = new FailingIntentStore(2);

        LogReplication replication = new LogReplication("node-1", wal, raftLog, store, election);
        setCommitIndex(replication, 2L);

        // -- act --
        replication.applyCommitted();

        // -- assert --
        assertEquals(1L, replication.lastApplied(),
            "lastApplied must not advance past the failed upsert at index 2");
    }

    /**
     * Verifies that applyCommitted stops after the first failure —
     * the failed entry and all subsequent entries are NOT applied in the same call.
     */
    @Test
    void applyCommitted_shouldStopApplyingAfterFailure() throws Exception {
        Intent intent1 = makeIntent("intent-1");
        Intent intent2 = makeIntent("intent-2");
        Intent intent3 = makeIntent("intent-3");

        raftLog.appendEntry(1L, IntentBinaryCodec.encode(intent1)); // index 1
        raftLog.appendEntry(1L, IntentBinaryCodec.encode(intent2)); // index 2
        raftLog.appendEntry(1L, IntentBinaryCodec.encode(intent3)); // index 3

        FailingIntentStore store = new FailingIntentStore(2); // fail on 2nd upsert

        LogReplication replication = new LogReplication("node-1", wal, raftLog, store, election);
        setCommitIndex(replication, 3L);

        // -- act --
        replication.applyCommitted();

        // -- assert --
        assertEquals(1L, replication.lastApplied(),
            "lastApplied should be 1 — loop breaks at failed entry 2, entry 3 not applied");
        // intent-1 was saved, intent-2 and intent-3 were not
        assertNotNull(store.findById("intent-1"),
            "intent-1 should be in the store");
        assertNull(store.findById("intent-2"),
            "intent-2 should NOT be in the store (upsert failed)");
        assertNull(store.findById("intent-3"),
            "intent-3 should NOT be in the store (loop broke before reaching it)");
    }

    /**
     * Verifies that applyCommitted retries the failed entry on a subsequent call
     * (after the store is no longer failing).
     */
    @Test
    void applyCommitted_shouldRetryFailedEntryOnNextCall() throws Exception {
        Intent intent1 = makeIntent("intent-1");
        Intent intent2 = makeIntent("intent-2");

        raftLog.appendEntry(1L, IntentBinaryCodec.encode(intent1)); // index 1
        raftLog.appendEntry(1L, IntentBinaryCodec.encode(intent2)); // index 2

        // Fail on 2nd upsert, then allow subsequent calls to succeed
        FailingIntentStore store = new FailingIntentStore(2);

        LogReplication replication = new LogReplication("node-1", wal, raftLog, store, election);
        setCommitIndex(replication, 2L);

        // -- first call: intent-1 succeeds, intent-2 fails --
        replication.applyCommitted();
        assertEquals(1L, replication.lastApplied());

        // -- second call: intent-2 should succeed now (callCount=2 already passed) --
        replication.applyCommitted();
        assertEquals(2L, replication.lastApplied(),
            "lastApplied should advance to 2 after retry succeeds");
        assertNotNull(store.findById("intent-2"),
            "intent-2 should be in the store after successful retry");
    }
}
