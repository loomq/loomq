package com.loomq.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.LoomqEngine;
import com.loomq.config.ServerConfig;
import com.loomq.http.netty.IntentHandler;
import com.loomq.http.netty.NettyHttpServer;
import com.loomq.http.netty.RadixRouter;
import com.loomq.raft.GrpcRaftTransport;
import com.loomq.raft.RaftConfig;
import com.loomq.raft.RaftNode;
import com.loomq.raft.RaftWriteCoordinator;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.RaftStatusProvider;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag("integration")
class RaftServerSmokeTest {

    @Test
    @Timeout(45)
    @DisplayName("Raft standalone server should expose health, metrics, and intent APIs")
    void raftSmokeTest() throws Exception {
        Path walDir = Files.createTempDirectory("loomq-raft-smoke");
        LoomqEngine engine = LoomqEngine.builder()
            .nodeId("raft-smoke-node")
            .walDir(walDir)
            .deliveryHandler(intent -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
            .build();

        int raftPort = allocatePort();
        GrpcRaftTransport transport = new GrpcRaftTransport("raft-smoke-node");
        transport.setListenAddress("127.0.0.1", raftPort);
        transport.start();
        RaftNode raftNode = new RaftNode(
            new RaftConfig("raft-smoke-node", List.of(), walDir.toString(), 50, 100, 50),
            engine.getWalAccessor(),
            engine.getIntentStoreInternal(),
            transport);
        RaftWriteCoordinator writeCoordinator = new RaftWriteCoordinator(raftNode, engine.getIntentStoreInternal());

        NettyHttpServer server = null;
        try {
            engine.start();
            raftNode.start();
            waitForLeader(raftNode, 5_000);
            assertTrue(raftNode.isLeader(), "single-node Raft should become leader");

            RadixRouter router = new RadixRouter();
            new IntentHandler(engine, writeCoordinator, raftNode).register(router);
            registerSystemRoutes(router, engine, raftNode);

            server = new NettyHttpServer(testConfig(), router);
            server.start();

            HttpClient client = HttpClient.newHttpClient();

            HttpResponse<String> health = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/health"))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"status\":\"UP\""));
            assertTrue(health.body().contains("\"narrative\""));
            assertTrue(health.body().contains("\"vitals\""));
            assertTrue(health.body().contains("\"durability\""));
            // raft details are served via /health/ready and /health/deep

            HttpResponse<String> readiness = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/health/ready"))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, readiness.statusCode());
            assertTrue(readiness.body().contains("\"ready\":true"));
            assertTrue(readiness.body().contains("\"mode\":\"raft\""));
            assertTrue(readiness.body().contains("\"reason\":\"READY\""));

            HttpResponse<String> deepHealth = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/health/deep"))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, deepHealth.statusCode());
            assertTrue(deepHealth.body().contains("\"tiers\""));

            HttpResponse<String> metrics = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/metrics"))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, metrics.statusCode());
            assertTrue(metrics.body().contains("\"raftRole\":\"LEADER\""));
            assertTrue(metrics.body().contains("\"raftLeaderId\":\"raft-smoke-node\""));
            assertTrue(metrics.body().contains("\"raftPendingWrites\""));
            assertTrue(metrics.body().contains("\"raftWriteProposalLatencyMs\""));

            String intentId = "raft-smoke-" + System.nanoTime();
            Instant executeAt = Instant.now().plusSeconds(5);
            String payload = """
                {
                  "intentId":"%s",
                  "executeAt":"%s",
                  "deadline":"%s",
                  "precisionTier":"STANDARD",
                  "shardKey":"orders"
                }
                """.formatted(intentId, executeAt, executeAt.plusSeconds(60));

            HttpResponse<String> created = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/intents"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(201, created.statusCode());
            assertTrue(created.body().contains(intentId));
            assertTrue(created.body().contains("\"revision\":1"));

            HttpResponse<String> patched = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/intents/" + intentId))
                    .header("Content-Type", "application/json")
                    .header("X-LoomQ-Expected-Revision", "1")
                    .method("PATCH", HttpRequest.BodyPublishers.ofString("""
                        {
                          "deadline":"%s",
                          "tags":{"scenario":"smoke-patch"}
                        }
                        """.formatted(executeAt.plusSeconds(60))))
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, patched.statusCode());
            assertTrue(patched.body().contains(intentId));
            assertTrue(patched.body().contains("\"revision\":2"));

            HttpResponse<String> cancelled = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/intents/" + intentId + "/cancel"))
                    .header("Content-Type", "application/json")
                    .header("X-LoomQ-Expected-Revision", "2")
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, cancelled.statusCode());
            assertTrue(cancelled.body().contains("\"status\":\"CANCELED\""));
            assertTrue(cancelled.body().contains("\"revision\":3"));

            String fireNowId = "raft-smoke-fire-" + System.nanoTime();
            Instant fireExecuteAt = Instant.now().plusSeconds(30);
            String firePayload = """
                {
                  "intentId":"%s",
                  "executeAt":"%s",
                  "deadline":"%s",
                  "precisionTier":"STANDARD",
                  "shardKey":"orders"
                }
                """.formatted(fireNowId, fireExecuteAt, fireExecuteAt.plusSeconds(60));

            HttpResponse<String> fireCreated = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/intents"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(firePayload))
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(201, fireCreated.statusCode());
            assertTrue(fireCreated.body().contains("\"revision\":1"));

            HttpResponse<String> fired = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/intents/" + fireNowId + "/fire-now"))
                    .header("Content-Type", "application/json")
                    .header("X-LoomQ-Expected-Revision", "1")
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, fired.statusCode());
            assertTrue(fired.body().contains("\"status\":\"DISPATCHING\""));

            HttpResponse<String> fetched = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/intents/" + intentId))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, fetched.statusCode());
            assertTrue(fetched.body().contains(intentId));
        } finally {
            if (server != null) {
                server.stop();
            }
            try {
                raftNode.close();
            } catch (Exception ignored) {
            }
            try {
                engine.close();
            } catch (Exception ignored) {
            }
            try {
                Files.walk(walDir)
                    .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ignored) {
                        }
                    });
            } catch (Exception ignored) {
            }
        }
    }

    private static void waitForLeader(RaftNode node, long maxWaitMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.isLeader()) {
                return;
            }
            Thread.sleep(50);
        }
    }

    private static void registerSystemRoutes(RadixRouter router, LoomqEngine engine, RaftStatusProvider raftStatus) throws Exception {
        Method method = LoomqServerApplication.class.getDeclaredMethod(
            "registerSystemRoutes", RadixRouter.class, LoomqEngine.class, RaftStatusProvider.class);
        method.setAccessible(true);
        method.invoke(null, router, engine, raftStatus);
    }

    private ServerConfig testConfig() {
        Properties props = new Properties();
        props.setProperty("host", "127.0.0.1");
        props.setProperty("port", "0");
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
        props.setProperty("gracefulShutdownTimeoutMs", "5000");
        props.setProperty("server.host", "127.0.0.1");
        props.setProperty("server.port", "0");
        props.setProperty("server.backlog", "1024");
        props.setProperty("server.virtual_threads", "true");
        props.setProperty("server.max_request_size", String.valueOf(10 * 1024 * 1024));
        props.setProperty("server.thread_pool_size", "200");
        return ServerConfig.fromProperties(props);
    }

    private static int allocatePort() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        }
    }
}
