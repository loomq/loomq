package com.loomq.raft;

import com.loomq.common.RaftRole;
import com.loomq.common.RaftStatusSnapshot;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.spi.RaftStatusProvider;
import com.loomq.spi.WalAccessor;
import com.loomq.store.IntentStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft 节点 — 组合选举、日志复制、状态机应用。
 *
 * 通过内核 SPI（WalAccessor + IntentStore）与 LoomqEngine 交互。
 */
public class RaftNode implements AutoCloseable, RaftStatusProvider {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    private static final LoomQMetrics metrics = LoomQMetrics.getInstance();
    private static final int MAX_ENTRIES_PER_APPEND = 1000;
    private final String nodeId;
    private final WalAccessor wal;
    private final IntentStore store;
    private final LeaderElection election;
    private final LogReplication replication;
    private final RaftLog raftLog;
    private final RaftTransport transport;
    private final RaftRuntimeListener runtimeListener;
    private final List<String> peers;
    private final ScheduledExecutorService heartbeatTimer;
    private final long heartbeatMs;
    private final long readLeaseMs;
    private final long shutdownTimeoutMs;
    private final ConcurrentMap<String, PeerReplicationState> peerStates;
    private ScheduledFuture<?> heartbeatTask;
    private volatile long readLeaseUntilMs = 0L;
    /**
     * Leader generation counter — incremented each time this node becomes leader.
     * Heartbeat callbacks check this against their captured generation to discard
     * stale responses after stepDown mid-loop (§5.1 safety).
     */
    private volatile long leaderGeneration = 0;

    public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport) {
        this(config, wal, store, transport, null, new StandaloneElection(wal, config.nodeId()));
    }

    public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport,
                    RaftRuntimeListener runtimeListener) {
        this(config, wal, store, transport, runtimeListener, new StandaloneElection(wal, config.nodeId()));
    }

    /**
     * 创建 RaftNode，使用外部提供的 LeaderElection 实现。
     *
     * <p>externalElection 不能为 null，必须显式传入（K8sLeaseElection 或 StandaloneElection）。
     */
    public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport,
                    RaftRuntimeListener runtimeListener, LeaderElection externalElection) {
        this.nodeId = config.nodeId();
        this.wal = wal;
        this.store = store;
        this.transport = transport;
        this.runtimeListener = runtimeListener;
        this.peers = config.peers();
        this.heartbeatMs = config.heartbeatMs();
        this.readLeaseMs = computeReadLeaseMs(config.electionMinMs(), config.heartbeatMs());
        this.shutdownTimeoutMs = config.shutdownTimeoutMs();
        this.peerStates = new ConcurrentHashMap<>();
        for (String peerId : peers) {
            if (!peerId.equals(nodeId)) {
                peerStates.put(peerId, new PeerReplicationState(peerId));
            }
        }
        this.raftLog = new RaftLog(wal);
        if (externalElection == null) {
            throw new IllegalArgumentException("externalElection is required (K8sLeaseElection or StandaloneElection)");
        }
        this.election = externalElection;
        this.replication = new LogReplication(nodeId, wal, raftLog, store, election, runtimeListener);
        this.heartbeatTimer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-heartbeat-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        // Server-side RPC handlers (transport may be null for single-node/no-network tests)
        if (transport != null) {
            transport.setOnAppendEntries(request -> {
                AppendEntriesResult result = replication.handleAppendEntries(
                    request.epoch(), request.leaderId(),
                    request.prevLogIndex(), request.prevLogEpoch(),
                    request.entries().toArray(new byte[0][]),
                    request.leaderCommit());
                syncRaftMetrics();
                return new RaftTransport.AppendEntriesResponse(
                    result.epoch, result.success, result.matchIndex, result.conflictIndex);
            });

            transport.setOnInstallSnapshot(request -> {
                // Transport layer handles chunk reassembly — only complete snapshots arrive here
                Long appliedIndex = handleInstallSnapshot(request);
                syncRaftMetrics();
                return new RaftTransport.InstallSnapshotResponse(
                    request.epoch(), appliedIndex != null ? appliedIndex : -1);
            });
            transport.setCurrentEpochSupplier(election::currentEpoch);
        }

        // Election lifecycle callbacks
        election.addBecomeLeaderListener(this::onBecomeLeader);
        election.addBecomeFollowerListener(this::onBecomeFollower);
    }

    public void start() {
        election.start();
        syncRaftMetrics();
        if (runtimeListener != null && role() == RaftRole.FOLLOWER) {
            notifyRuntimeRole();
        }
        log.info("RaftNode started: node={}, epoch={}, logIndex={}",
            nodeId, election.currentEpoch(), raftLog.lastIndex());
    }

    @Override
    public void close() {
        try {
            stopHeartbeat();
            if (isLeader() && !peerStates.isEmpty() && shutdownTimeoutMs > 0) {
                gracefulShutdown();
            }
        } catch (Exception e) {
            log.error("Error during graceful shutdown", e);
        } finally {
            heartbeatTimer.shutdown();
            election.stop();
            replication.failPendingWaiters(new IllegalStateException("Raft node closed"));
            notifyRuntimeRole(RaftRole.FOLLOWER);
            metrics.updateRaftRole("OFFLINE");
            metrics.updateRaftLeaderId(null);
            metrics.updateRaftReplicationLag(0);
            metrics.updateRaftConnectedPeers(0);
            metrics.updateRaftTotalPeers(0);
            if (transport != null) {
                try {
                    transport.close();
                } catch (Exception e) {
                    log.error("Error closing transport", e);
                }
            }
            log.info("RaftNode closed: node={}", nodeId);
        }
    }

    /**
     * 优雅关机：等待异步复制完成。
     *
     * <p>收到 SIGTERM 后，Leader 执行以下步骤：
     * <ol>
     *   <li>停止接收新的写入请求（election.stop() 后 isLeader() 返回 false）</li>
     *   <li>持续复制积压日志到 Followers</li>
     *   <li>等待 commitIndex 追上 lastLogIndex（最长 shutdownTimeoutMs）</li>
     *   <li>超时后强制关闭</li>
     * </ol>
     */
    private void gracefulShutdown() {
        log.info("Graceful shutdown: waiting for async replication (max {}ms)", shutdownTimeoutMs);
        long deadline = System.currentTimeMillis() + shutdownTimeoutMs;
        long lastLogIdx = raftLog.lastIndex();
        long commitIdx = replication.commitIndex();

        // 先做一次心跳，触发复制
        sendHeartbeats();

        while (System.currentTimeMillis() < deadline && commitIdx < lastLogIdx && isLeader()) {
            try {
                Thread.sleep(100);
                sendHeartbeats();
                commitIdx = replication.commitIndex();
                lastLogIdx = raftLog.lastIndex();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        if (commitIdx >= lastLogIdx) {
            log.info("Graceful shutdown: all entries replicated (commitIndex={})", commitIdx);
        } else {
            log.warn("Graceful shutdown: timeout, {} entries may not be replicated (commitIndex={}, lastLogIndex={})",
                lastLogIdx - commitIdx, commitIdx, lastLogIdx);
        }
    }

    public RaftRole role() { return election.role(); }
    public boolean isLeader() { return election.isLeader(); }
    public LeaderElection getElection() { return election; }
    public LogReplication getReplication() { return replication; }
    public WalAccessor getWal() { return wal; }
    public RaftLog getRaftLog() { return raftLog; }

    @Override
    public boolean isRaftEnabled() {
        return true;
    }

    @Override
    public boolean canServeLinearizableRead() {
        if (!isLeader()) {
            return false;
        }
        if (peerStates.isEmpty()) {
            return true;
        }
        return System.currentTimeMillis() <= readLeaseUntilMs;
    }

    @Override
    public String currentLeaderId() {
        return election.currentLeader();
    }

    /** Leader 提交一条 entry 到本地 WAL 并返回 index */
    public long propose(byte[] entry) {
        if (!isLeader()) {
            throw new IllegalStateException("Raft proposals must be issued by the leader");
        }
        long epoch = election.currentEpoch(); // capture once
        long index = raftLog.appendEntry(epoch, entry);
        if (peerStates.isEmpty()) {
            // Single-node: commit and apply immediately
            replication.advanceCommitIndex(new long[]{index}, epoch);
            replication.applyCommitted();
        } else {
            // Multi-node: trigger immediate replication instead of waiting for next heartbeat tick
            triggerImmediateReplication();
        }
        log.debug("Proposed entry at index {}", index);
        return index;
    }

    /**
     * 异步模式：Leader 提交 entry 到本地 WAL 并立即提交应用。
     *
     * <p>不等待多数节点确认，直接将 commitIndex 推进到此 index。
     * 用于 ASYNC 复制模式，写入延迟最低，但 Leader 崩溃可能丢失未复制的数据。
     */
    public long proposeAsync(byte[] entry) {
        if (!isLeader()) {
            throw new IllegalStateException("Raft proposals must be issued by the leader");
        }
        long epoch = election.currentEpoch(); // capture once
        long index = raftLog.appendEntry(epoch, entry);
        // 立即推进 commitIndex 并应用（不等待 quorum）
        replication.advanceCommitIndex(new long[]{index}, epoch);
        replication.applyCommitted();
        // 后台异步复制到 Followers
        if (!peerStates.isEmpty()) {
            triggerImmediateReplication();
        }
        log.debug("Async proposed entry at index {}", index);
        return index;
    }

    /**
     * Trigger an immediate replication round by scheduling a heartbeat on the executor.
     * This reduces replication latency from avg(heartbeatMs/2) to near-zero.
     */
    private void triggerImmediateReplication() {
        try {
            heartbeatTimer.execute(this::sendHeartbeats);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Timer shut down — will be caught by next heartbeat cycle
            log.debug("Immediate replication trigger rejected (timer shut down)");
        }
    }

    /** Apply committed entries to state machine */
    public void applyCommitted() {
        replication.applyCommitted();
    }

    // ========== InstallSnapshot (§5.5 / §7) ==========

    /**
     * Leader: 发送快照给落后过多的 follower。
     * 当 follower 的 nextIndex 低于 WAL 最早可用 index 时（段文件已截断），
     * 必须用快照替代 AppendEntries 来同步。
     */
    public void sendInstallSnapshot(String peerId) {
        PeerReplicationState ps = peerStates.get(peerId);
        if (ps == null) {
            return;
        }

        long epoch = election.currentEpoch();
        long snapshotIndex = replication.lastApplied();
        long snapshotEpoch = snapshotIndex > 0 ? raftLog.readEntryEpoch(snapshotIndex) : 0;
        long requestGeneration = ++ps.requestGeneration;

        byte[] snapshotData = encodeStoreSnapshot();
        if (snapshotData.length == 0) {
            log.error("Failed to encode snapshot for peer {}, skipping", peerId);
            return;
        }
        log.info("Sending snapshot to {}: {} bytes (index={})", peerId, snapshotData.length, snapshotIndex);

        // Transport handles chunking — RaftNode passes full data
        transport.sendInstallSnapshot(peerId,
            new RaftTransport.InstallSnapshotRequest(
                epoch, nodeId, snapshotIndex, snapshotEpoch,
                0, 1, snapshotData))
            .thenAccept(response -> {
                if (requestGeneration != ps.requestGeneration) return;
                if (response.bytesReceived() >= 0) {
                    // Compact log now that snapshot is confirmed by follower
                    raftLog.compactThrough(snapshotIndex, snapshotEpoch);
                    ps.nextIndex = snapshotIndex + 1;
                    ps.matchIndex = snapshotIndex;
                    log.info("InstallSnapshot accepted by {} (index={})", peerId, snapshotIndex);
                } else {
                    log.warn("InstallSnapshot rejected by {}", peerId);
                }
            })
            .exceptionally(ex -> {
                log.error("InstallSnapshot failed for {}: {} (compactThrough may have failed, disk space not reclaimed)",
                    peerId, ex.getMessage(), ex);
                return null;
            });
    }

    /**
     * Follower: 处理 InstallSnapshot RPC。
     * 清空当前 IntentStore，解码并应用快照数据。
     *
     * @return 新的 lastApplied index（失败返回 -1）
     */
    private Long handleInstallSnapshot(RaftTransport.InstallSnapshotRequest request) {
        if (request.epoch() < election.currentEpoch()) {
            log.debug("Rejecting InstallSnapshot: stale epoch {} < {}", request.epoch(), election.currentEpoch());
            return -1L;
        }
        election.onAppendEntries(request.epoch(), request.leaderId());

        try {
            // Decode snapshot first — if this fails, store is untouched and
            // we reject the snapshot (leader will retry).
            java.util.List<com.loomq.domain.intent.Intent> decoded = decodeStoreSnapshot(request.data());

            // Phase 1: upsert-then-prune (requires replication lock to block applyCommitted)
            synchronized (replication) {
                java.util.Set<String> snapshotIds = new java.util.HashSet<>();
                for (com.loomq.domain.intent.Intent intent : decoded) {
                    store.upsert(intent);
                    snapshotIds.add(intent.getIntentId());
                }

                // Remove stale intents not in snapshot
                java.util.Set<String> toRemove = new java.util.HashSet<>(store.getAllIntents().keySet());
                toRemove.removeAll(snapshotIds);
                for (String id : toRemove) {
                    store.delete(id);
                }

                replication.resetToSnapshot(request.lastIncludedIndex());
                // Compact WAL inside the replication lock so the snapshot is applied
                // atomically with log truncation. This keeps log state consistent with
                // the in-memory store: any concurrent applyCommitted is blocked until
                // both the reset and the compaction have completed.
                raftLog.compactThrough(request.lastIncludedIndex(), request.lastIncludedEpoch());
            }

            log.info("InstallSnapshot applied: {} intents, index={}, epoch={}",
                decoded.size(), request.lastIncludedIndex(), request.lastIncludedEpoch());
            return request.lastIncludedIndex();
        } catch (Exception e) {
            log.error("Failed to apply InstallSnapshot from {}", request.leaderId(), e);
            return -1L;
        }
    }

    /** Encode all intents in store as a binary snapshot blob */
    private byte[] encodeStoreSnapshot() {
        try {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(bos);
            var allIntents = store.getAllIntents();
            dos.writeInt(allIntents.size());
            for (var intent : allIntents.values()) {
                byte[] encoded = com.loomq.infrastructure.wal.IntentBinaryCodec.encode(intent);
                dos.writeInt(encoded.length);
                dos.write(encoded);
            }
            dos.flush();
            return bos.toByteArray();
        } catch (java.io.IOException e) {
            log.error("Failed to encode store snapshot", e);
            return new byte[0];
        }
    }

    /** Decode a binary snapshot blob into a list of intents (no side effects). */
    private java.util.List<com.loomq.domain.intent.Intent> decodeStoreSnapshot(byte[] data) throws java.io.IOException {
        java.io.DataInputStream dis = new java.io.DataInputStream(
            new java.io.ByteArrayInputStream(data));
        int count = dis.readInt();
        if (count < 0 || count > 1_000_000) {
            throw new java.io.IOException("Invalid snapshot count: " + count);
        }
        java.util.List<com.loomq.domain.intent.Intent> result = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = dis.readInt();
            if (len < 0 || len > 10_000_000) {
                throw new java.io.IOException("Invalid entry length: " + len);
            }
            byte[] encoded = new byte[len];
            dis.readFully(encoded);
            var intent = com.loomq.infrastructure.wal.IntentBinaryCodec.decode(encoded);
            if (intent == null) {
                throw new java.io.IOException("Decoded intent is null at index " + i);
            }
            result.add(intent);
        }
        return result;
    }

    // ========== Heartbeat ==========

    private void onBecomeLeader(long epoch) {
        leaderGeneration++;
        renewReadLease();
        syncRaftMetrics();
        notifyRuntimeRole();
        log.info("Node {} became LEADER at epoch {} (gen {})", nodeId, epoch, leaderGeneration);

        // 追加 no-op entry（Raft 安全性保证）
        // 新 Leader 无法直接提交之前 term 的 entry，
        // 通过追加当前 term 的 no-op entry，间接提交所有之前的 entry。
        if (!peerStates.isEmpty()) {
            appendNoOpEntry();
        }

        startHeartbeat(epoch);
    }

    /**
     * 追加 no-op entry 到当前 term。
     * 用于新 Leader 间接提交之前 term 的 entry（Raft §5.4.2）。
     */
    private void appendNoOpEntry() {
        try {
            long index = raftLog.appendEntry(election.currentEpoch(), new byte[0]);
            log.debug("Appended no-op entry at index {} for epoch {}", index, election.currentEpoch());
            // 触发复制以尽快推进 commitIndex
            triggerImmediateReplication();
        } catch (Exception e) {
            log.error("Failed to append no-op entry", e);
        }
    }

    private void onBecomeFollower(long epoch) {
        readLeaseUntilMs = 0L;
        syncRaftMetrics();
        replication.failPendingWaiters(new IllegalStateException("Leadership lost"));
        notifyRuntimeRole();
        log.info("Node {} became FOLLOWER at epoch {}", nodeId, epoch);
        leaderGeneration++;
        stopHeartbeat();
    }

    private void startHeartbeat(long epoch) {
        stopHeartbeat();
        heartbeatTask = heartbeatTimer.scheduleAtFixedRate(this::sendHeartbeats,
            heartbeatMs, heartbeatMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Send AppendEntries to all peers. Called both by the periodic heartbeat timer
     * and by {@link #triggerImmediateReplication()} after a new entry is proposed.
     */
    private void sendHeartbeats() {
        try {
            if (!isLeader() || transport == null) {
                stopHeartbeat();
                return;
            }
            syncRaftMetrics();
            final long myGeneration = leaderGeneration;
            long currentEpoch = election.currentEpoch();
            long commitIdx = replication.commitIndex();
            long lastIdx = raftLog.lastIndex();
            int majority = ((peerStates.size() + 1) / 2) + 1;
            java.util.concurrent.atomic.AtomicInteger quorumAcks = new java.util.concurrent.atomic.AtomicInteger(1);
            java.util.concurrent.atomic.AtomicBoolean leaseRenewed = new java.util.concurrent.atomic.AtomicBoolean(false);

            for (PeerReplicationState ps : peerStates.values()) {
                // Stop sending if we lost leadership mid-iteration (stepDown via callback)
                if (!isLeader()) break;

                try {
                    // If follower is too far behind (log already truncated), send snapshot
                    if (ps.nextIndex < raftLog.firstIndex()) {
                        sendInstallSnapshot(ps.peerId);
                        continue;
                    }

                    // Pipeline: skip if all missing entries are already covered by an in-flight RPC
                    if (ps.inflightMaxIndex > 0 && ps.nextIndex <= ps.inflightMaxIndex) {
                        continue;
                    }

                    long requestGeneration = ++ps.requestGeneration;

                    // Read entries the peer is missing (batch-limited)
                    long nextIdx = ps.nextIndex;
                    long prevLogIndex = nextIdx - 1;
                    long prevLogEpoch = prevLogIndex > 0 ? raftLog.readEntryEpoch(prevLogIndex) : 0;
                    if (prevLogEpoch < 0) prevLogEpoch = 0; // entry not yet readable

                    int count = (int) Math.min(lastIdx - nextIdx + 1, MAX_ENTRIES_PER_APPEND);
                    if (count <= 0) continue;

                    // Read entries, stopping at first null (truncated or I/O error)
                    java.util.List<byte[]> entryList = new java.util.ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        byte[] entry = raftLog.readEntryRaw(nextIdx + i);
                        if (entry == null) break;
                        entryList.add(entry);
                    }
                    if (entryList.isEmpty()) continue;
                    // Update inflightMaxIndex to actual last entry sent
                    ps.inflightMaxIndex = nextIdx + entryList.size() - 1;
                    transport.sendAppendEntries(ps.peerId,
                            new RaftTransport.AppendEntriesRequest(
                                currentEpoch, nodeId, prevLogIndex, prevLogEpoch, entryList, commitIdx))
                        .whenComplete((response, throwable) -> {
                            if (throwable != null) {
                                log.error("AppendEntries failed for peer {}: {}", ps.peerId,
                                    throwable.getMessage(), throwable);
                                ps.inflightMaxIndex = 0; // Reset so next heartbeat can retry
                                return;
                            }
                            if (requestGeneration != ps.requestGeneration) return;
                            // Bail out if this response belongs to a past leadership epoch
                            if (myGeneration != leaderGeneration) return;

                            if (response.success()) {
                                ps.matchIndex = response.matchIndex();
                                ps.nextIndex = response.matchIndex() + 1;
                                if (quorumAcks.incrementAndGet() >= majority && leaseRenewed.compareAndSet(false, true)) {
                                    renewReadLease();
                                }
                                advanceCommitIndexFromAllPeers();
                            } else if (response.epoch() > currentEpoch) {
                                log.warn("AppendEntries rejected: follower {} has higher epoch {} > {}, stepping down",
                                    ps.peerId, response.epoch(), currentEpoch);
                                election.stepDown(response.epoch());
                            } else {
                                // Log inconsistency: use conflictIndex for fast backtrack
                                if (response.conflictIndex() > 0) {
                                    ps.nextIndex = Math.max(1, response.conflictIndex());
                                } else {
                                    ps.nextIndex = Math.max(1, ps.nextIndex - 1);
                                }
                                // Reset inflightMaxIndex so next heartbeat can retry
                                ps.inflightMaxIndex = 0;
                                log.debug("AppendEntries rejected by {} (conflictIdx={}), nextIndex={}",
                                    ps.peerId, response.conflictIndex(), ps.nextIndex);
                            }
                        });
                } catch (Exception peerEx) {
                    log.error("Heartbeat failed for peer {}: {}", ps.peerId,
                        peerEx.getMessage(), peerEx);
                    // Continue to next peer — don't let one peer failure stop others
                }
            }
            syncRaftMetrics();
        } catch (Throwable t) {
            log.error("Heartbeat task terminated unexpectedly — this is a bug", t);
            // Do NOT rethrow — ScheduledExecutorService would silently cancel the task
        }
    }

    private void advanceCommitIndexFromAllPeers() {
        long[] matchIndices = new long[peerStates.size() + 1];
        matchIndices[0] = raftLog.lastIndex(); // self always matches
        int i = 1;
        for (PeerReplicationState ps : peerStates.values()) {
            matchIndices[i++] = ps.matchIndex;
        }
        long before = replication.commitIndex();
        replication.advanceCommitIndex(matchIndices, election.currentEpoch());
        if (replication.commitIndex() > before) {
            replication.applyCommitted();
        }
        syncRaftMetrics();
    }

    private void stopHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
    }

    private void syncRaftMetrics() {
        RaftStatusSnapshot status = snapshotStatus();
        metrics.updateRaftRole(status.role().name());
        metrics.updateRaftLeaderId(status.leaderId());
        metrics.updateRaftEpoch(status.epoch());
        metrics.updateRaftCommitIndex(status.commitIndex());
        metrics.updateRaftLastApplied(status.lastApplied());
        metrics.updateRaftReplicationLag(status.replicationLag());
        metrics.updateRaftConnectedPeers(status.connectedPeers());
        metrics.updateRaftTotalPeers(status.totalPeers());
    }

    private void notifyRuntimeRole() {
        notifyRuntimeRole(role());
    }

    private void notifyRuntimeRole(RaftRole role) {
        if (runtimeListener != null) {
            runtimeListener.onRoleChanged(role, election.currentEpoch());
        }
    }

    @Override
    public RaftStatusSnapshot snapshotStatus() {
        long epoch = election.currentEpoch();
        long commitIndex = replication.commitIndex();
        long lastApplied = replication.lastApplied();
        long commitLag = Math.max(0, commitIndex - lastApplied);
        long replicationLag = 0;
        Map<String, Boolean> reachability = new LinkedHashMap<>();
        int connectedPeers = 0;
        int totalPeers = 0;

        for (String peerId : peers) {
            if (peerId.equals(nodeId)) {
                continue;
            }

            totalPeers++;
            boolean connected = transport != null && transport.isPeerConnected(peerId);
            reachability.put(peerId, connected);
            if (connected) {
                connectedPeers++;
            }

            if (isLeader()) {
                PeerReplicationState ps = peerStates.get(peerId);
                if (ps != null) {
                    replicationLag = Math.max(replicationLag, Math.max(0, raftLog.lastIndex() - ps.matchIndex));
                }
            }
        }

        return new RaftStatusSnapshot(
            nodeId,
            election.role(),
            election.currentLeader(),
            epoch,
            commitIndex,
            lastApplied,
            commitLag,
            isLeader() ? replicationLag : 0,
            connectedPeers,
            totalPeers,
            reachability
        );
    }

    // ========== Peer replication state ==========

    /** Tracks replication progress for a single follower peer. */
    static class PeerReplicationState {
        final String peerId;
        volatile long nextIndex = 1;
        volatile long matchIndex;
        volatile long requestGeneration = 0;
        /**
         * Highest index included in an in-flight AppendEntries RPC.
         * Used for pipelining: a new AE is sent only if nextIndex > inflightMaxIndex,
         * meaning there are entries not yet covered by any in-flight RPC.
         */
        volatile long inflightMaxIndex = 0;

        PeerReplicationState(String peerId) {
            this.peerId = peerId;
        }
    }

    private void renewReadLease() {
        readLeaseUntilMs = System.currentTimeMillis() + readLeaseMs;
    }

    private static long computeReadLeaseMs(long electionMinMs, long heartbeatMs) {
        long heartbeatLease = Math.max(heartbeatMs * 2L, 100L);
        long maxSafeLease = Math.max(1L, electionMinMs - 1L);
        return Math.max(1L, Math.min(maxSafeLease, heartbeatLease));
    }
}
