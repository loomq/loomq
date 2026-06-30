package com.loomq.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.common.RaftRole;
import com.loomq.common.RaftStatusSnapshot;
import com.loomq.config.ServerConfig;
import com.loomq.http.netty.HttpErrorResponse;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.spi.RaftStatusProvider;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class LoomqServerApplicationTest {

    @Test
    void validateSupportedStartupModesShouldAllowRaftOnlyMode() {
        String previous = System.getProperty("loomq.cluster.enabled");
        try {
            System.clearProperty("loomq.cluster.enabled");

            assertDoesNotThrow(LoomqServerApplication::validateSupportedStartupModes);
        } finally {
            restoreProperty("loomq.cluster.enabled", previous);
        }
    }

    @Test
    void validateSupportedStartupModesShouldRejectLegacyClusterMode() {
        String previous = System.getProperty("loomq.cluster.enabled");
        try {
            System.setProperty("loomq.cluster.enabled", "true");

            assertThrows(IllegalStateException.class,
                LoomqServerApplication::validateSupportedStartupModes);
        } finally {
            restoreProperty("loomq.cluster.enabled", previous);
        }
    }

    @Test
    void parseRaftPeerIdsShouldRejectDuplicateIds() {
        assertThrows(IllegalStateException.class,
            () -> LoomqServerApplication.parseRaftPeerIds(
                "node-1@127.0.0.1:7931,node-1@127.0.0.1:7932", "node-1"));
    }

    @Test
    void parseRaftPeerTargetsShouldRejectMissingEndpointForRemotePeer() {
        assertThrows(IllegalStateException.class,
            () -> LoomqServerApplication.parseRaftPeerTargets("node-1,node-2", "node-1"));
    }

    @Test
    void buildReadinessResponseShouldAllowEmbeddedModeWhenWalIsHealthy() {
        Object response = LoomqServerApplication.buildReadinessResponse(null, metrics(true, 0));

        Map<?, ?> body = assertInstanceOf(Map.class, response);
        assertEquals("UP", body.get("status"));
        assertEquals("embedded", body.get("mode"));
        assertEquals(Boolean.TRUE, body.get("ready"));
        assertEquals(Boolean.TRUE, body.get("acceptingReads"));
        assertEquals(Boolean.TRUE, body.get("acceptingWrites"));
    }

    @Test
    void buildReadinessResponseShouldRejectUnhealthyWal() {
        Object response = LoomqServerApplication.buildReadinessResponse(null, metrics(false, 0));

        HttpErrorResponse error = assertInstanceOf(HttpErrorResponse.class, response);
        assertEquals(503, error.status());
        assertEquals("50304", error.error().code());
        assertEquals("WAL_UNHEALTHY", error.error().details().get("reason"));
    }

    @Test
    void buildReadinessResponseShouldRejectRaftFollowerForClientTraffic() {
        Object response = LoomqServerApplication.buildReadinessResponse(
            raftStatus(RaftRole.FOLLOWER, "node-2", 0, 2, 2, false),
            metrics(true, 0));

        HttpErrorResponse error = assertInstanceOf(HttpErrorResponse.class, response);
        assertEquals(503, error.status());
        assertEquals("RAFT_NOT_LEADER", error.error().details().get("reason"));
        assertEquals(Boolean.FALSE, error.error().details().get("acceptingReads"));
        assertEquals(Boolean.FALSE, error.error().details().get("acceptingWrites"));
    }

    @Test
    void buildReadinessResponseShouldAllowRaftLeaderWithFreshQuorum() {
        Object response = LoomqServerApplication.buildReadinessResponse(
            raftStatus(RaftRole.LEADER, "node-1", 0, 2, 2, true),
            metrics(true, 0));

        Map<?, ?> body = assertInstanceOf(Map.class, response);
        assertEquals(Boolean.TRUE, body.get("ready"));
        assertEquals("raft", body.get("mode"));
        assertEquals("READY", body.get("reason"));
        assertEquals(Boolean.TRUE, body.get("quorumReachable"));
        assertEquals(Boolean.TRUE, body.get("acceptingReads"));
        assertEquals(Boolean.TRUE, body.get("acceptingWrites"));
    }

    @Test
    void buildReadinessResponseShouldRejectLeaderWithoutQuorum() {
        Object response = LoomqServerApplication.buildReadinessResponse(
            raftStatus(RaftRole.LEADER, "node-1", 0, 0, 2, true),
            metrics(true, 0));

        HttpErrorResponse error = assertInstanceOf(HttpErrorResponse.class, response);
        assertEquals(503, error.status());
        assertEquals("RAFT_QUORUM_UNREACHABLE", error.error().details().get("reason"));
        assertEquals(Boolean.FALSE, error.error().details().get("quorumReachable"));
    }

    private static void restoreProperty(String key, String previousValue) {
        if (previousValue == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previousValue);
        }
    }

    private static LoomQMetrics.MetricsSnapshot metrics(boolean walHealthy, long raftPendingWrites) {
        LoomQMetrics metrics = LoomQMetrics.getInstance();
        metrics.reset();
        metrics.updateWalHealth(walHealthy);
        metrics.updateRaftPendingWrites(raftPendingWrites);
        return metrics.snapshot();
    }

    private static RaftStatusProvider raftStatus(RaftRole role,
                                                 String leaderId,
                                                 long commitLag,
                                                 int connectedPeers,
                                                 int totalPeers,
                                                 boolean canServeLinearizableRead) {
        return new RaftStatusProvider() {
            @Override
            public boolean isRaftEnabled() {
                return true;
            }

            @Override
            public RaftRole role() {
                return role;
            }

            @Override
            public String currentLeaderId() {
                return leaderId;
            }

            @Override
            public boolean canServeLinearizableRead() {
                return canServeLinearizableRead;
            }

            @Override
            public RaftStatusSnapshot snapshotStatus() {
                return new RaftStatusSnapshot(
                    "node-1",
                    role,
                    leaderId,
                    7,
                    11,
                    11 - commitLag,
                    commitLag,
                    0,
                    connectedPeers,
                    totalPeers,
                    Map.of());
            }
        };
    }

    private static ServerConfig serverConfig(int port, int nettyPort) {
        Properties props = new Properties();
        props.setProperty("host", "127.0.0.1");
        props.setProperty("port", String.valueOf(port));
        props.setProperty("backlog", "1024");
        props.setProperty("virtualThreads", "true");
        props.setProperty("maxRequestSize", String.valueOf(10 * 1024 * 1024));
        props.setProperty("threadPoolSize", "200");
        props.setProperty("nettyHost", "127.0.0.1");
        props.setProperty("nettyPort", String.valueOf(nettyPort));
        props.setProperty("bossThreads", "1");
        props.setProperty("workerThreads", "0");
        props.setProperty("maxContentLength", String.valueOf(10 * 1024 * 1024));
        props.setProperty("useEpoll", "false");
        props.setProperty("pooledAllocator", "true");
        props.setProperty("soBacklog", "1024");
        props.setProperty("tcpNoDelay", "true");
        props.setProperty("connectionTimeoutMs", "30000");
        props.setProperty("idleTimeoutSeconds", "60");
        props.setProperty("maxConnections", "1000");
        props.setProperty("writeBufferHighWaterMark", String.valueOf(1024 * 1024));
        props.setProperty("writeBufferLowWaterMark", String.valueOf(512 * 1024));
        props.setProperty("maxConcurrentBusinessRequests", "100");
        props.setProperty("httpSemaphoreTimeoutMs", "500");
        props.setProperty("gracefulShutdownTimeoutMs", "5000");
        return ServerConfig.fromProperties(props);
    }
}
