package com.loomq.raft;

import com.loomq.common.RaftRole;
import com.loomq.spi.WalAccessor;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单机模式选主实现（no-op）。
 *
 * <p>非 K8s 环境下使用，固定返回 Leader。
 * 无选举、无心跳、无 term 管理。
 */
public class StandaloneElection implements LeaderElection {
    private static final Logger log = LoggerFactory.getLogger(StandaloneElection.class);

    private final WalAccessor wal;
    private final String nodeId;
    private volatile boolean started = false;
    private volatile long currentEpoch = 1L;
    private volatile Consumer<Long> onBecomeLeader;
    private volatile Consumer<Long> onBecomeFollower;

    public StandaloneElection() {
        this(null, "standalone");
    }

    public StandaloneElection(WalAccessor wal) {
        this(wal, "standalone");
    }

    public StandaloneElection(WalAccessor wal, String nodeId) {
        this.wal = wal;
        this.nodeId = (nodeId != null && !nodeId.isBlank()) ? nodeId : "standalone";
        if (wal != null) {
            long persisted = wal.getLastLogEpoch();
            if (persisted > 0) this.currentEpoch = persisted;
        }
    }

    @Override
    public RaftRole role() {
        return started ? RaftRole.LEADER : RaftRole.FOLLOWER;
    }

    @Override
    public boolean isLeader() {
        return started;
    }

    @Override
    public long currentEpoch() {
        return currentEpoch;
    }

    @Override
    public String currentLeader() {
        return nodeId;
    }

    @Override
    public void start() {
        started = true;
        log.info("StandaloneElection started: fixed LEADER mode, epoch={}", currentEpoch);
        if (onBecomeLeader != null) {
            onBecomeLeader.accept(currentEpoch);
        }
    }

    @Override
    public void stop() {
        started = false;
        log.info("StandaloneElection stopped");
    }

    @Override
    public void onAppendEntries(long leaderEpoch, String leaderId) {
        // No-op: single-node mode has no peers
    }

    @Override
    public void addBecomeLeaderListener(Consumer<Long> listener) {
        this.onBecomeLeader = listener;
    }

    @Override
    public void addBecomeFollowerListener(Consumer<Long> listener) {
        this.onBecomeFollower = listener;
    }

    @Override
    public void stepDown(long newEpoch) {
        // 单机模式永不退位
    }
}
