package com.loomq.common;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only Raft control-plane snapshot for health and observability.
 */
public record RaftStatusSnapshot(
    String nodeId,
    RaftRole role,
    String leaderId,
    long epoch,
    long commitIndex,
    long lastApplied,
    long commitLag,
    long replicationLag,
    int connectedPeers,
    int totalPeers,
    Map<String, Boolean> peerReachability
) {

    public RaftStatusSnapshot {
        peerReachability = peerReachability == null
            ? Map.of()
            : Collections.unmodifiableMap(new LinkedHashMap<>(peerReachability));
    }

    public boolean isLeader() {
        return role == RaftRole.LEADER;
    }
}
