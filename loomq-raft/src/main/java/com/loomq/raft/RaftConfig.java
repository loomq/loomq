package com.loomq.raft;

import java.util.List;

/**
 * Raft 共识配置。
 *
 * @param nodeId                节点 ID
 * @param peers                 集群节点列表（包含自身）
 * @param dataDir               数据目录
 * @param electionMinMs         选举超时最小值（毫秒）
 * @param electionMaxMs         选举超时最大值（毫秒）
 * @param heartbeatMs           心跳间隔（毫秒）
 * @param replicationMode       日志复制模式（ASYNC/SYNC）
 * @param shutdownTimeoutMs     优雅关机等待异步复制完成的最大时间（毫秒）
 */
public record RaftConfig(String nodeId, List<String> peers, String dataDir,
        long electionMinMs, long electionMaxMs, long heartbeatMs,
        ReplicationMode replicationMode, long shutdownTimeoutMs) {

    public RaftConfig(String nodeId, List<String> peers, String dataDir,
                      long electionMinMs, long electionMaxMs, long heartbeatMs) {
        this(nodeId, peers, dataDir, electionMinMs, electionMaxMs, heartbeatMs,
            ReplicationMode.ASYNC, 25_000L);
    }

    public static RaftConfig singleNode(String nodeId) {
        return new RaftConfig(nodeId, List.of(), "./raft-data", 150, 300, 50);
    }

    public static RaftConfig cluster(String nodeId, List<String> peers) {
        return new RaftConfig(nodeId, peers, "./raft-data", 150, 300, 50);
    }
}
