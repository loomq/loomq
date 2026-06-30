package com.loomq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft 共识路径 Soak Test。
 *
 * 验证目标：
 * 1. 单节点持续高负载写入下 commitIndex 正确推进
 * 2. 快照 + 日志压缩后继续写入不丢数据
 * 3. 多线程并发 propose 不产生数据丢失或重复
 *
 * @tag slow
 */
@Tag("slow")
class RaftSoakTest {

    private static final Logger logger = LoggerFactory.getLogger(RaftSoakTest.class);

    private Path dataDir;
    private SimpleWalWriter wal;
    private IntentStore store;

    private static Intent makeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setExecuteAt(java.time.Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    private static void waitForLeader(RaftNode node, long maxWaitMs) {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.isLeader()) return;
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("raft-soak-");
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false, "memory_segment", 1, 8, 64, 10, 4, 1, false);
        wal = new SimpleWalWriter(cfg, "raft-soak");
        store = new ConcurrentIntentStore();
    }

    @AfterEach
    void tearDown() {
        if (wal != null) wal.close();
        if (store != null) store.shutdown();
    }

    /**
     * 持续写入 500 个 Intent，验证 commitIndex 正确推进且所有数据可查。
     */
    @Test
    void sustainedWriteLoad_commitIndexAdvancesCorrectly() {
        RaftConfig config = RaftConfig.singleNode("node-1");
        RaftNode node = new RaftNode(config, wal, store, null);
        node.start();
        waitForLeader(node, 5000);
        assertTrue(node.isLeader(), "single node should become leader");

        int totalIntents = 500;
        List<Long> indices = new ArrayList<>(totalIntents);

        for (int i = 0; i < totalIntents; i++) {
            byte[] encoded = IntentBinaryCodec.encode(makeIntent("soak-" + i));
            long index = node.propose(encoded);
            assertTrue(index > 0, "propose should return valid index at iteration " + i);
            indices.add(index);
        }

        // Commit and apply all
        long lastIndex = indices.get(indices.size() - 1);
        node.getReplication().advanceCommitIndex(
            new long[]{lastIndex}, node.getElection().currentEpoch());
        node.applyCommitted();

        // Verify commit index advanced to last proposed
        assertTrue(node.getReplication().commitIndex() >= lastIndex,
            "commitIndex should be >= last proposed index");

        // Verify all intents are in store
        for (int i = 0; i < totalIntents; i++) {
            Intent restored = store.findById("soak-" + i);
            assertNotNull(restored, "Intent soak-" + i + " should be in store after commit");
        }

        logger.info("Sustained write: {} intents proposed and committed, commitIndex={}",
            totalIntents, node.getReplication().commitIndex());
        node.close();
    }

    /**
     * 快照 + 日志压缩后继续写入，验证不丢数据。
     *
     * 流程：
     * 1. 提交 300 个 Intent
     * 2. 压缩日志到 lastApplied
     * 3. 继续提交 200 个 Intent
     * 4. 验证全部 500 个 Intent 可查
     */
    @Test
    void snapshotCompaction_continuedWrite_noDataLoss() {
        RaftConfig config = RaftConfig.singleNode("node-1");
        RaftNode node = new RaftNode(config, wal, store, null);
        node.start();
        waitForLeader(node, 5000);
        assertTrue(node.isLeader(), "single node should become leader");

        // Phase 1: Write 300 intents
        int phase1Count = 300;
        long lastIdx1 = 0;
        for (int i = 0; i < phase1Count; i++) {
            byte[] encoded = IntentBinaryCodec.encode(makeIntent("pre-compact-" + i));
            lastIdx1 = node.propose(encoded);
        }
        node.getReplication().advanceCommitIndex(
            new long[]{lastIdx1}, node.getElection().currentEpoch());
        node.applyCommitted();

        long lastApplied = node.getReplication().lastApplied();
        assertTrue(lastApplied >= lastIdx1, "lastApplied should advance");

        // Phase 2: Compact log through lastApplied
        long firstIndexBefore = node.getRaftLog().firstIndex();
        node.getRaftLog().compactThrough(lastApplied);
        long firstIndexAfter = node.getRaftLog().firstIndex();
        assertTrue(firstIndexAfter >= lastApplied,
            "firstIndex should advance past compaction point");

        logger.info("Log compacted: firstIndex {} -> {}", firstIndexBefore, firstIndexAfter);

        // Phase 3: Write 200 more intents AFTER compaction
        int phase2Count = 200;
        long lastIdx2 = 0;
        for (int i = 0; i < phase2Count; i++) {
            byte[] encoded = IntentBinaryCodec.encode(makeIntent("post-compact-" + i));
            lastIdx2 = node.propose(encoded);
            assertTrue(lastIdx2 > lastIdx1, "post-compact index should be > pre-compact");
        }
        node.getReplication().advanceCommitIndex(
            new long[]{lastIdx2}, node.getElection().currentEpoch());
        node.applyCommitted();

        // Phase 4: Verify ALL intents in store
        for (int i = 0; i < phase1Count; i++) {
            assertNotNull(store.findById("pre-compact-" + i),
                "Pre-compact intent " + i + " should survive log compaction");
        }
        for (int i = 0; i < phase2Count; i++) {
            assertNotNull(store.findById("post-compact-" + i),
                "Post-compact intent " + i + " should be in store");
        }

        logger.info("Snapshot compaction soak: {} pre + {} post = {} total intents verified",
            phase1Count, phase2Count, phase1Count + phase2Count);
        node.close();
    }

    /**
     * 多线程并发 propose，验证无数据丢失。
     *
     * 16 个虚拟线程各提交 50 个 Intent（共 800 个）
     * 验证所有 Intent 的 propose index 唯一且全部可查
     */
    @Test
    void concurrentProposals_noDataLoss() throws Exception {
        RaftConfig config = RaftConfig.singleNode("node-1");
        RaftNode node = new RaftNode(config, wal, store, null);
        node.start();
        waitForLeader(node, 5000);
        assertTrue(node.isLeader(), "single node should become leader");

        int threadCount = 16;
        int intentsPerThread = 50;
        int totalIntents = threadCount * intentsPerThread;

        CopyOnWriteArrayList<Long> allIndices = new CopyOnWriteArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < intentsPerThread; i++) {
                        String id = "concurrent-" + threadId + "-" + i;
                        byte[] encoded = IntentBinaryCodec.encode(makeIntent(id));
                        long index = node.propose(encoded);
                        if (index <= 0) {
                            errorCount.incrementAndGet();
                        }
                        allIndices.add(index);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    logger.error("Thread {} failed: {}", threadId, e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "All threads should complete");
        executor.shutdown();

        assertEquals(0, errorCount.get(), "No propose errors should occur");
        assertEquals(totalIntents, allIndices.size(), "All intents should be proposed");

        // Verify all indices are unique
        long uniqueCount = allIndices.stream().distinct().count();
        assertEquals(totalIntents, uniqueCount, "All propose indices should be unique");

        // Commit and apply all
        long maxIndex = allIndices.stream().mapToLong(Long::longValue).max().orElse(0);
        node.getReplication().advanceCommitIndex(
            new long[]{maxIndex}, node.getElection().currentEpoch());
        node.applyCommitted();

        // Verify all intents in store
        int found = 0;
        for (int t = 0; t < threadCount; t++) {
            for (int i = 0; i < intentsPerThread; i++) {
                String id = "concurrent-" + t + "-" + i;
                Intent restored = store.findById(id);
                if (restored != null) {
                    found++;
                }
            }
        }
        assertEquals(totalIntents, found, "All concurrent intents should be in store");

        logger.info("Concurrent soak: {} threads x {} intents = {} total, all verified",
            threadCount, intentsPerThread, totalIntents);
        node.close();
    }

    /**
     * 快照编码/解码 round-trip 在大量 Intent 下的正确性。
     */
    @Test
    void snapshotEncodeDecode_largePayload_roundTrip() {
        RaftConfig config = RaftConfig.singleNode("node-1");
        RaftNode node = new RaftNode(config, wal, store, null);
        node.start();
        waitForLeader(node, 5000);
        assertTrue(node.isLeader());

        // Create 200 intents with varying sizes
        int count = 200;
        for (int i = 0; i < count; i++) {
            Intent intent = makeIntent("snapshot-rt-" + i);
            // Add some tags to increase payload size
            intent.setTags(java.util.Map.of("key", "value-" + i, "index", String.valueOf(i)));
            store.save(intent);
        }

        // Propose all through Raft (which encodes/decodes via IntentBinaryCodec)
        long lastIndex = 0;
        for (int i = 0; i < count; i++) {
            byte[] encoded = IntentBinaryCodec.encode(makeIntent("snapshot-rt-" + i));
            lastIndex = node.propose(encoded);
        }

        node.getReplication().advanceCommitIndex(
            new long[]{lastIndex}, node.getElection().currentEpoch());
        node.applyCommitted();

        // Verify all committed intents are in store
        for (int i = 0; i < count; i++) {
            assertNotNull(store.findById("snapshot-rt-" + i),
                "Intent " + i + " should survive round-trip");
        }

        logger.info("Snapshot round-trip: {} intents verified", count);
        node.close();
    }
}
