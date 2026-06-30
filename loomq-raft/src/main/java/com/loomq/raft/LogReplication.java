package com.loomq.raft;

import com.loomq.common.RaftRole;
import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.spi.WalAccessor;
import com.loomq.store.IntentStore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Raft Log Replication (§5.3). Leader 复制 entries 到 followers，多数确认后 commit。 */
public class LogReplication {
    private static final Logger log = LoggerFactory.getLogger(LogReplication.class);
    private static final LoomQMetrics metrics = LoomQMetrics.getInstance();
    private final String nodeId;
    private final WalAccessor wal;
    private final RaftLog raftLog;
    private final IntentStore store;
    private final LeaderElection election; // interface — supports both Raft and K8s election
    private final RaftRuntimeListener runtimeListener;
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);
    private final ConcurrentMap<Long, CompletableFuture<Void>> appliedWaiters = new ConcurrentHashMap<>();

    public LogReplication(String nodeId, WalAccessor wal, RaftLog raftLog, IntentStore store, LeaderElection election) {
        this(nodeId, wal, raftLog, store, election, null);
    }

    public LogReplication(String nodeId, WalAccessor wal, RaftLog raftLog, IntentStore store,
                          LeaderElection election, RaftRuntimeListener runtimeListener) {
        this.nodeId = nodeId; this.wal = wal; this.raftLog = raftLog;
        this.store = store; this.election = election;
        this.runtimeListener = runtimeListener;
    }

    public long commitIndex() { return commitIndex.get(); }
    public long lastApplied() { return lastApplied.get(); }

    /**
     * 重置 commitIndex 和 lastApplied（InstallSnapshot 使用）。
     * Leader 通过 majority 推进，follower 通过此方法直接跳转到快照 index。
     */
    public synchronized void resetToSnapshot(long index) {
        commitIndex.set(index);
        lastApplied.set(index);
        metrics.updateRaftCommitIndex(index);
        metrics.updateRaftLastApplied(index);
    }

    /**
     * Wait until a given log index has been applied to the local state machine.
     */
    public boolean awaitApplied(long index, long timeoutMs) {
        if (lastApplied.get() >= index) {
            return true;
        }

        CompletableFuture<Void> waiter = new CompletableFuture<>();
        CompletableFuture<Void> existing = appliedWaiters.putIfAbsent(index, waiter);
        if (existing != null) {
            waiter = existing;
        }

        if (lastApplied.get() >= index) {
            appliedWaiters.remove(index, waiter);
            return true;
        }

        try {
            waiter.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            appliedWaiters.remove(index, waiter);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            appliedWaiters.remove(index, waiter);
            return false;
        } catch (ExecutionException e) {
            throw new RuntimeException("Raft applied wait failed", e.getCause());
        }
    }

    /**
     * Fail all pending application waiters, used when leadership is lost.
     */
    public void failPendingWaiters(Throwable cause) {
        if (cause == null) {
            cause = new IllegalStateException("Raft leadership lost");
        }
        // Use iterator.remove() to atomically drain — avoids race with
        // awaitApplied() adding a new waiter between loop completion and clear().
        var it = appliedWaiters.entrySet().iterator();
        while (it.hasNext()) {
            it.next().getValue().completeExceptionally(cause);
            it.remove();
        }
    }

    /**
     * Leader: 推进 commitIndex（多数 matchIndex >= idx 且 entry 来自当前 epoch）。
     *
     * 算法：对 matchIndex 排序，取中位数作为 candidate（多数阈值）。
     * 若 candidate > commitIndex 且 candidate entry 的 epoch == currentEpoch，则提交。
     * 复杂度 O(p log p)，支持数十节点集群。
     */
    public void advanceCommitIndex(long[] matchIndices, long currentEpoch) {
        long[] sorted = matchIndices.clone();
        java.util.Arrays.sort(sorted);
        long candidate = sorted[sorted.length / 2]; // majority threshold
        if (candidate > commitIndex.get()
            && raftLog.readEntryEpoch(candidate) == currentEpoch) {
            commitIndex.set(candidate);
            metrics.updateRaftEpoch(currentEpoch);
            metrics.updateRaftCommitIndex(candidate);
            log.debug("commitIndex -> {}", candidate);
        }
    }

    /** Follower: 处理 AppendEntries */
    public AppendEntriesResult handleAppendEntries(long epoch, String leaderId,
            long prevLogIndex, long prevLogEpoch, byte[][] entries, long leaderCommit) {
        if (epoch < election.currentEpoch()) return AppendEntriesResult.fail(election.currentEpoch());
        election.onAppendEntries(epoch, leaderId);
        metrics.updateRaftEpoch(epoch);

        // §5.3: 验证 prevLogIndex 处的 entry epoch 与 prevLogEpoch 一致
        if (prevLogIndex > 0) {
            long lastIdx = raftLog.lastIndex();
            if (lastIdx < prevLogIndex) return AppendEntriesResult.fail(epoch, lastIdx);
            long existingEpoch = raftLog.readEntryEpoch(prevLogIndex);
            if (existingEpoch < 0) {
                // Entry not found or read error — treat as log inconsistency
                log.warn("Cannot read epoch at prevLogIndex={}, rejecting AppendEntries", prevLogIndex);
                return AppendEntriesResult.fail(epoch, lastIdx);
            }
            if (existingEpoch != prevLogEpoch) {
                // Epoch mismatch — conflicting entry (§5.3).
                // Report prevLogIndex as conflict point for leader to skip to.
                return AppendEntriesResult.fail(epoch, prevLogIndex);
            }
        }

        // Truncate conflicting entries before appending (§5.3).
        // If the follower has entries past prevLogIndex (e.g., from a previous leader),
        // they must be deleted so the new leader's entries replace them contiguously.
        if (prevLogIndex < raftLog.lastIndex()) {
            raftLog.truncateAfter(prevLogIndex);
        }

        // 写入新 entries（每个 entry 前 8 字节为原始 epoch，由 leader 在 readEntryRaw 中保留）
        for (byte[] entry : entries) {
            if (entry == null || entry.length < 8) continue;
            java.nio.ByteBuffer entryBuf = java.nio.ByteBuffer.wrap(entry);
            long entryEpoch = entryBuf.getLong();
            byte[] payload = new byte[entry.length - 8];
            entryBuf.get(payload);
            raftLog.appendEntry(entryEpoch, payload);
        }
        long lastIdx = raftLog.lastIndex();
        boolean commitAdvanced = false;
        if (leaderCommit > commitIndex.get()) {
            long newCommit = Math.min(leaderCommit, lastIdx);
            long prev = commitIndex.getAndUpdate(cur -> Math.max(cur, newCommit));
            commitAdvanced = newCommit > prev;
        }
        if (commitAdvanced) {
            applyCommitted();
        }
        metrics.updateRaftCommitIndex(commitIndex.get());
        metrics.updateRaftLastApplied(lastApplied.get());
        return AppendEntriesResult.success(epoch, lastIdx);
    }

    /** Apply committed entries to state machine */
    public synchronized void applyCommitted() {
        long committed = commitIndex.get();
        long applied = lastApplied.get();
        while (applied < committed) {
            applied++;
            byte[] entryPayload = raftLog.readEntry(applied);
            if (entryPayload == null || entryPayload.length == 0) {
                lastApplied.set(applied); // no-op entry, safe to advance
                continue;
            }
            try {
                Intent intent = IntentBinaryCodec.decode(entryPayload);
                boolean isNew = store.findById(intent.getIntentId()) == null;
                store.upsert(intent);
                lastApplied.set(applied); // upsert 成功后才推进
                if (runtimeListener != null) {
                    try {
                        runtimeListener.onCommittedIntent(intent, isNew, election.role() == RaftRole.LEADER);
                    } catch (Exception listenerEx) {
                        log.error("Runtime listener failed while applying committed entry at index {}",
                            applied, listenerEx);
                    }
                }
                completeAppliedWaiter(applied);
                log.debug("Applied committed entry at index {}: intentId={}", applied, intent.getIntentId());
            } catch (Exception e) {
                log.error("Failed to apply committed entry at index {}", applied, e);
                // Fail this entry's waiter
                failAppliedWaiter(applied, e);
                // Fail all subsequent entries' waiters — Raft requires sequential application,
                // so entries after a failed entry cannot be safely applied.
                for (long i = applied + 1; i <= committed; i++) {
                    failAppliedWaiter(i,
                        new IllegalStateException("Skipped: prior entry at index " + applied + " failed", e));
                }
                break;
            }
        }
        metrics.updateRaftCommitIndex(commitIndex.get());
        metrics.updateRaftLastApplied(lastApplied.get());
    }

    private void completeAppliedWaiter(long appliedIndex) {
        CompletableFuture<Void> waiter = appliedWaiters.remove(appliedIndex);
        if (waiter != null) {
            waiter.complete(null);
        }
    }

    private void failAppliedWaiter(long appliedIndex, Throwable cause) {
        CompletableFuture<Void> waiter = appliedWaiters.remove(appliedIndex);
        if (waiter != null) {
            waiter.completeExceptionally(cause != null ? cause : new IllegalStateException("Raft apply failed"));
        }
    }
}
