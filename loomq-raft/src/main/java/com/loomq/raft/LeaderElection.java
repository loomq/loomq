package com.loomq.raft;

import com.loomq.common.RaftRole;
import java.util.function.Consumer;

/**
 * 选主抽象接口。
 *
 * <p>不同环境使用不同实现：
 * <ul>
 *   <li>{@link StandaloneElection} — 单机模式 no-op 实现</li>
 *   <li>{@link K8sLeaseElection} — K8s Lease 选主</li>
 * </ul>
 */
public interface LeaderElection {

    /** 当前角色 */
    RaftRole role();

    /** 是否为 Leader */
    boolean isLeader();

    /** 当前 epoch（K8s 模式对应 leaseTransitions） */
    long currentEpoch();

    /** 当前 Leader 节点 ID */
    String currentLeader();

    /** 启动选举模块 */
    void start();

    /** 停止选举模块 */
    void stop();

    /**
     * 收到新 leader 的 AppendEntries 时调用。
     * 用于重置选举时钟和更新 leader 信息。
     */
    void onAppendEntries(long leaderEpoch, String leaderId);

    /** 注册成为 Leader 的回调 */
    void addBecomeLeaderListener(Consumer<Long> listener);

    /** 注册成为 Follower 的回调 */
    void addBecomeFollowerListener(Consumer<Long> listener);

    /** 退位为 Follower，更新 epoch */
    void stepDown(long newEpoch);
}
