package com.loomq.metrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Raft 运行指标注册表。
 *
 * 负责记录当前角色、epoch、commit index、lastApplied 和提交滞后。
 */
final class RaftMetricsRegistry {

    private final AtomicReference<String> raftRole = new AtomicReference<>("OFFLINE");
    private final AtomicReference<String> raftLeaderId = new AtomicReference<>(null);
    private final AtomicLong raftEpoch = new AtomicLong(0);
    private final AtomicLong raftCommitIndex = new AtomicLong(0);
    private final AtomicLong raftLastApplied = new AtomicLong(0);
    private final AtomicLong raftReplicationLag = new AtomicLong(0);
    private final AtomicInteger raftConnectedPeers = new AtomicInteger(0);
    private final AtomicInteger raftTotalPeers = new AtomicInteger(0);
    private final AtomicLong raftPendingWrites = new AtomicLong(0);
    private final LongAdder raftWriteProposalLatencyTotalMs = new LongAdder();
    private final LongAdder raftWriteProposalCount = new LongAdder();
    private final AtomicLong raftWriteProposalLatencyMaxMs = new AtomicLong(0);
    private final LongAdder raftWriteTimeouts = new LongAdder();
    private final LongAdder raftWriteStepDownAborts = new LongAdder();
    private final LongAdder raftWriteBackpressureRejects = new LongAdder();
    private final LongAdder raftWriteRevisionConflicts = new LongAdder();

    void updateRaftRole(String role) {
        raftRole.set(role == null || role.isBlank() ? "OFFLINE" : role);
    }

    void updateRaftLeaderId(String leaderId) {
        raftLeaderId.set(leaderId == null || leaderId.isBlank() ? null : leaderId);
    }

    void updateRaftEpoch(long epoch) {
        raftEpoch.set(Math.max(0, epoch));
    }

    void updateRaftCommitIndex(long commitIndex) {
        raftCommitIndex.set(Math.max(0, commitIndex));
    }

    void updateRaftLastApplied(long lastApplied) {
        raftLastApplied.set(Math.max(0, lastApplied));
    }

    void updateRaftReplicationLag(long replicationLag) {
        raftReplicationLag.set(Math.max(0, replicationLag));
    }

    void updateRaftConnectedPeers(int connectedPeers) {
        raftConnectedPeers.set(Math.max(0, connectedPeers));
    }

    void updateRaftTotalPeers(int totalPeers) {
        raftTotalPeers.set(Math.max(0, totalPeers));
    }

    void updateRaftPendingWrites(long pendingWrites) {
        raftPendingWrites.set(Math.max(0, pendingWrites));
    }

    void recordRaftWriteProposalLatency(long latencyMs) {
        long safeLatency = Math.max(0, latencyMs);
        raftWriteProposalLatencyTotalMs.add(safeLatency);
        raftWriteProposalCount.increment();
        raftWriteProposalLatencyMaxMs.updateAndGet(max -> Math.max(max, safeLatency));
    }

    void incrementRaftWriteTimeouts() {
        raftWriteTimeouts.increment();
    }

    void incrementRaftWriteStepDownAborts() {
        raftWriteStepDownAborts.increment();
    }

    void incrementRaftWriteBackpressureRejects() {
        raftWriteBackpressureRejects.increment();
    }

    void incrementRaftWriteRevisionConflicts() {
        raftWriteRevisionConflicts.increment();
    }

    String getRaftRole() {
        return raftRole.get();
    }

    String getRaftLeaderId() {
        return raftLeaderId.get();
    }

    long getRaftEpoch() {
        return raftEpoch.get();
    }

    long getRaftCommitIndex() {
        return raftCommitIndex.get();
    }

    long getRaftLastApplied() {
        return raftLastApplied.get();
    }

    long getRaftCommitLag() {
        return Math.max(0, raftCommitIndex.get() - raftLastApplied.get());
    }

    long getRaftReplicationLag() {
        return raftReplicationLag.get();
    }

    int getRaftConnectedPeers() {
        return raftConnectedPeers.get();
    }

    int getRaftTotalPeers() {
        return raftTotalPeers.get();
    }

    long getRaftPendingWrites() {
        return raftPendingWrites.get();
    }

    double getRaftAverageWriteProposalLatencyMs() {
        long count = raftWriteProposalCount.sum();
        if (count == 0) {
            return 0.0;
        }
        return (double) raftWriteProposalLatencyTotalMs.sum() / count;
    }

    long getRaftWriteProposalLatencyMaxMs() {
        return raftWriteProposalLatencyMaxMs.get();
    }

    long getRaftWriteTimeouts() {
        return raftWriteTimeouts.sum();
    }

    long getRaftWriteStepDownAborts() {
        return raftWriteStepDownAborts.sum();
    }

    long getRaftWriteBackpressureRejects() {
        return raftWriteBackpressureRejects.sum();
    }

    long getRaftWriteRevisionConflicts() {
        return raftWriteRevisionConflicts.sum();
    }

    void reset() {
        raftRole.set("OFFLINE");
        raftLeaderId.set(null);
        raftEpoch.set(0);
        raftCommitIndex.set(0);
        raftLastApplied.set(0);
        raftReplicationLag.set(0);
        raftConnectedPeers.set(0);
        raftTotalPeers.set(0);
        raftPendingWrites.set(0);
        raftWriteProposalLatencyTotalMs.reset();
        raftWriteProposalCount.reset();
        raftWriteProposalLatencyMaxMs.set(0);
        raftWriteTimeouts.reset();
        raftWriteStepDownAborts.reset();
        raftWriteBackpressureRejects.reset();
        raftWriteRevisionConflicts.reset();
    }
}
