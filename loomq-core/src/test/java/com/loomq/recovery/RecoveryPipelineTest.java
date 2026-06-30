package com.loomq.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.recovery.RecoveryPipeline.RecoveryReport;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.WalAccessor;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RecoveryPipelineTest {

    private Path dataDir;
    private IntentStore store;
    private PrecisionScheduler scheduler;
    private RecoveryPipeline pipeline;

    private static final DeliveryHandler NOOP_HANDLER =
        intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    private static Intent makeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    @BeforeEach
    void setUp() throws IOException {
        dataDir = Files.createTempDirectory("recovery-test-");
        store = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(store, NOOP_HANDLER, null);
        pipeline = new RecoveryPipeline(dataDir);
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
        if (store != null) {
            store.shutdown();
        }
        try {
            Files.walk(dataDir)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
        } catch (IOException ignored) {}
    }

    @Test
    void recoverShouldReturnEmptyReportWhenNoSnapshotOrWal() {
        // Given: empty data directory
        WalAccessor emptyWal = new EmptyWalAccessor();

        // When
        RecoveryReport report = pipeline.recover(store, scheduler, emptyWal);

        // Then
        assertNotNull(report);
        assertEquals(0, report.restoredTotal());
        assertEquals(0, report.restoredFromSnapshot());
        assertEquals(0, report.restoredFromWal());
    }

    @Test
    void recoverShouldRestoreFromSnapshot() {
        // Given: create a snapshot with intents
        store.save(makeIntent("snap-intent-1"));
        store.save(makeIntent("snap-intent-2"));
        pipeline.checkpoint(store, () -> 0L);

        // Clear store to simulate restart
        store.clear();

        // When
        RecoveryReport report = pipeline.recover(store, scheduler, new EmptyWalAccessor());

        // Then
        assertEquals(2, report.restoredTotal());
        assertEquals(2, report.restoredFromSnapshot());
        assertEquals(0, report.restoredFromWal());
        assertNotNull(store.findById("snap-intent-1"));
        assertNotNull(store.findById("snap-intent-2"));
    }

    @Test
    void closeShouldBeIdempotent() {
        // When: close without starting snapshots
        pipeline.close();
        pipeline.close(); // second close should not throw

        // Then: no exception
    }

    @Test
    void startSnapshotsThenCloseShouldWork() {
        // Given
        pipeline.startSnapshots(store, () -> 0L);

        // When
        pipeline.close();

        // Then: no exception
    }

    /**
     * WalAccessor 空实现，用于恢复测试。
     */
    private static class EmptyWalAccessor implements WalAccessor {
        @Override
        public long getWritePosition() { return 0; }

        @Override
        public long getFlushedPosition() { return 0; }

        @Override
        public byte[] readRecord(long position, int recordLength) throws IOException {
            throw new IOException("No WAL data");
        }

        @Override
        public List<WalSegment> listSegments() { return Collections.emptyList(); }

        @Override
        public void truncateBefore(long globalOffset) {}

        @Override
        public long getLastLogEpoch() { return 0; }

        @Override
        public void setCurrentEpoch(long epoch) {}

        @Override
        public String getVotedFor() { return null; }

        @Override
        public void setVotedFor(String nodeId) {}
    }
}
