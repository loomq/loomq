package com.loomq.server;

import com.loomq.LoomqEngine;
import com.loomq.api.ErrorResponse;
import com.loomq.channel.grpc.config.GrpcConfig;
import com.loomq.channel.grpc.server.GlobalIntentObserver;
import com.loomq.channel.grpc.server.LoomqGrpcServer;
import com.loomq.channel.http.HttpCallbackHandler;
import com.loomq.channel.http.NettyHttpDeliveryHandler;
import com.loomq.channel.http.batch.BatchDeliveryConfig;
import com.loomq.channel.http.batch.BatchedHttpDeliveryHandler;
import com.loomq.common.HealthNarrator;
import com.loomq.common.MetricsCollector;
import com.loomq.common.RaftRole;
import com.loomq.common.RaftStatusSnapshot;
import com.loomq.common.TimelineService;
import com.loomq.common.WalReplayService;
import com.loomq.config.LoomqConfig;
import com.loomq.config.ServerConfig;
import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.grpc.gen.DeliveryAckMode;
import com.loomq.http.netty.HttpErrorResponse;
import com.loomq.http.netty.IntentHandler;
import com.loomq.http.netty.NettyHttpServer;
import com.loomq.http.netty.RadixRouter;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.raft.RaftRuntimeListener;
import com.loomq.raft.RaftWriteCoordinator;
import com.loomq.spi.DirectWriteCoordinator;
import com.loomq.spi.RaftStatusProvider;
import com.loomq.spi.WriteCoordinator;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone Netty service bootstrap.
 *
 * The embedded core remains reusable through LoomqEngine, while the standalone
 * service only wires the Netty transport and the HTTP adapters.
 */
public class LoomqServerApplication {

    private static final Logger logger = LoggerFactory.getLogger(LoomqServerApplication.class);
    private static final byte[] HEALTH_LIVE_RESPONSE = "{\"status\":\"ALIVE\"}".getBytes(StandardCharsets.UTF_8);
    private static final String READINESS_ERROR_CODE = "50304";

    public static void main(String[] args) {
        printBanner();

        LoomqConfig config = LoomqConfig.getInstance();
        ServerConfig serverConfig = config.getServerConfig();

        String nodeId = resolveSetting("LOOMQ_NODE_ID", "loomq.node.id", "node-1");
        String dataDir = resolveSetting("LOOMQ_DATA_DIR", "loomq.data.dir", config.getWalConfig().dataDir());
        WalConfig walConfig = config.getWalConfig().withDataDir(dataDir);

        MetricsCollector.getInstance().setWalDataDir(dataDir);
        MetricsCollector.getInstance().setSchedulerMaxPendingIntents(config.getSchedulerConfig().maxPendingIntents());
        logRuntimeConfiguration(config, nodeId, dataDir, walConfig);
        validateSupportedStartupModes();

        HttpCallbackHandler callbackHandler = new HttpCallbackHandler();
        var deliveryHandler = createDeliveryHandler();

        LoomqEngine engine = LoomqEngine.builder()
            .nodeId(nodeId)
            .walDir(Path.of(dataDir))
            .walConfig(walConfig)
            .callbackHandler(callbackHandler)
            .deliveryHandler(deliveryHandler)
            .build();

        // ---- Global observer for gRPC streaming (registered only when gRPC is enabled) ----
        GrpcConfig grpcConfig = config.getGrpcConfig();
        GlobalIntentObserver globalObserver = grpcConfig.enabled() ? new GlobalIntentObserver() : null;
        if (globalObserver != null) {
            engine.registerObserver(globalObserver);
        }

        // ---- 共识模式 ----
        final com.loomq.raft.RaftNode raftNode;
        final RaftRuntimeListener raftRuntimeListener;
        final WriteCoordinator writeCoordinator;
        String mode = resolveSetting("LOOMQ_MODE", "loomq.mode", "standalone");

        if ("distributed".equalsIgnoreCase(mode)) {
            // ---- K8s Lease 选主 + 日志复制 ----
            engine.getScheduler().pause();
            raftRuntimeListener = new RaftRuntimeBridge(engine);

            String podName = resolveSetting("POD_NAME", "loomq.kubernetes.podName", nodeId);
            String namespace = resolveSetting("LOOMQ_K8S_NAMESPACE", "loomq.kubernetes.lease.namespace", "default");
            String leaseName = resolveSetting("LOOMQ_K8S_LEASE_NAME", "loomq.kubernetes.lease.leaseName", "loomq-leader");
            int leaseDuration = Integer.parseInt(
                resolveSetting("LOOMQ_K8S_LEASE_DURATION", "loomq.kubernetes.lease.durationSeconds", "10"));
            int renewInterval = Integer.parseInt(
                resolveSetting("LOOMQ_K8S_LEASE_RENEW_INTERVAL", "loomq.kubernetes.lease.renewIntervalSeconds", "3"));

            com.loomq.raft.K8sLeaseConfig k8sConfig = new com.loomq.raft.K8sLeaseConfig(
                leaseDuration, renewInterval, namespace, leaseName, podName);
            com.loomq.raft.K8sLeaseElection k8sElection = new com.loomq.raft.K8sLeaseElection(
                k8sConfig, engine.getWalAccessor());

            // Raft node for log replication (no Raft election — K8s handles that)
            String peersStr = resolveSetting("LOOMQ_PEERS", "loomq.peers", podName);
            List<String> peers = parseRaftPeerIds(peersStr, podName);
            int raftPort = Integer.parseInt(
                resolveSetting("LOOMQ_RAFT_PORT", "loomq.raft.port", "9928"));

            com.loomq.raft.RaftConfig raftConfig = new com.loomq.raft.RaftConfig(
                podName, peers, dataDir, 150, 300, 50);
            com.loomq.raft.GrpcRaftTransport raftTransport = new com.loomq.raft.GrpcRaftTransport(podName);
            raftTransport.setListenAddress("0.0.0.0", raftPort);
            try {
                raftTransport.start();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to start Raft gRPC transport", e);
            }

            raftNode = new com.loomq.raft.RaftNode(raftConfig,
                engine.getWalAccessor(), engine.getIntentStoreInternal(),
                raftTransport, raftRuntimeListener, k8sElection);

            List<RaftPeerTarget> peerTargets = parseRaftPeerTargets(peersStr, podName);
            connectRaftPeers(raftTransport, peerTargets, podName);

            String replicationModeStr = resolveSetting("LOOMQ_REPLICATION_MODE", "loomq.replication.mode", "ASYNC");
            com.loomq.raft.ReplicationMode replicationMode = "SYNC".equalsIgnoreCase(replicationModeStr)
                ? com.loomq.raft.ReplicationMode.SYNC : com.loomq.raft.ReplicationMode.ASYNC;

            writeCoordinator = new RaftWriteCoordinator(
                raftNode,
                engine.getIntentStoreInternal(),
                serverConfig.maxConcurrentBusinessRequests(),
                serverConfig.httpSemaphoreTimeoutMs(),
                5_000L,
                replicationMode
            );
            logger.info("Distributed mode (K8s Lease): pod={}, namespace={}, lease={}, peers={}, replicationMode={}",
                podName, namespace, leaseName, peers, replicationMode);

        } else if (Boolean.parseBoolean(
                resolveSetting("LOOMQ_RAFT_ENABLED", "loomq.raft.enabled", "false"))) {
            throw new IllegalStateException(
                "loomq.raft.enabled=true is no longer supported. "
                + "Use loomq.mode=distributed with K8s Lease for multi-node deployment. "
                + "See migration guide: docs/superpowers/specs/2026-06-03-raft-code-review-fixes-design.md");
        } else {
            // ---- 单机模式 ----
            raftNode = null;
            raftRuntimeListener = null;
            writeCoordinator = new DirectWriteCoordinator(
                engine.getCommandService(),
                engine.getIntentStoreInternal(),
                engine.getRunning()
            );
            logger.info("Single-node mode: DirectWriteCoordinator enabled");
        }

        RadixRouter router = new RadixRouter();
        RaftStatusProvider raftStatus = raftNode;
        new IntentHandler(engine, writeCoordinator, raftStatus).register(router);
        registerSystemRoutes(router, engine, raftStatus);

        NettyHttpServer server = new NettyHttpServer(serverConfig, router, config.getSecurityConfig());

        // ---- Optional gRPC server ----
        final LoomqGrpcServer grpcServer;
        if (grpcConfig.enabled()) {
            // 如果使用 gRPC 流投递，传递给 gRPC 服务器
            com.loomq.channel.grpc.server.GrpcStreamDeliveryHandler grpcDeliveryHandler =
                deliveryHandler instanceof com.loomq.channel.grpc.server.GrpcStreamDeliveryHandler gdh
                    ? gdh : null;

            grpcServer = new LoomqGrpcServer(grpcConfig, engine, raftStatus, writeCoordinator,
                globalObserver, grpcDeliveryHandler);
            logger.info("gRPC server will be started on {}:{}", grpcConfig.host(), grpcConfig.port());
        } else {
            grpcServer = null;
        }

        // 批量投递处理器的关闭钩子
        if (deliveryHandler instanceof BatchedHttpDeliveryHandler batchedHandler) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down BatchedHttpDeliveryHandler...");
                batchedHandler.shutdown();
            }, "loomq-batch-delivery-shutdown"));
        }

        // gRPC 流投递处理器的关闭钩子
        if (deliveryHandler instanceof com.loomq.channel.grpc.server.GrpcStreamDeliveryHandler grpcHandler) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down GrpcStreamDeliveryHandler...");
                grpcHandler.close();
            }, "loomq-grpc-delivery-shutdown"));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received, stopping servers and engine...");
            try {
                server.stop();
            } catch (Exception e) {
                logger.warn("Error while stopping Netty server", e);
            }

            if (grpcServer != null) {
                try {
                    grpcServer.stop();
                } catch (Exception e) {
                    logger.warn("Error while stopping gRPC server", e);
                }
            }

            if (globalObserver != null) {
                globalObserver.close();
            }

            try {
                callbackHandler.close();
            } catch (Exception e) {
                logger.warn("Error while closing callback handler", e);
            }

            if (raftNode != null) {
                raftNode.close();
            }

            try {
                engine.close();
            } catch (Exception e) {
                logger.error("Error during engine shutdown", e);
            }
        }, "loomq-shutdown"));

        try {
            engine.start();
            if (raftNode != null) {
                raftNode.start();
            }
            server.start();
            if (grpcServer != null) {
                grpcServer.start();
                logger.info("LoomQ gRPC server started on {}:{}", grpcConfig.host(), grpcServer.getPort());
            }

            logger.info("LoomQ Netty server started on http://{}:{}", serverConfig.host(), server.getPort());

            while (engine.isRunning()) {
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Main thread interrupted");
        } catch (Exception e) {
            logger.error("Server failed to start", e);
            try {
                server.stop();
            } catch (Exception stopError) {
                logger.warn("Error while stopping Netty server after startup failure", stopError);
            }
            if (grpcServer != null) {
                try {
                    grpcServer.stop();
                } catch (Exception stopError) {
                    logger.warn("Error while stopping gRPC server after startup failure", stopError);
                }
            }
            try {
                callbackHandler.close();
            } catch (Exception closeError) {
                logger.warn("Error while closing callback handler after startup failure", closeError);
            }
            try {
                engine.close();
            } catch (Exception closeError) {
                logger.warn("Error while closing engine after startup failure", closeError);
            }
            System.exit(1);
        }

        logger.info("Application exited");
    }

    private static void logRuntimeConfiguration(LoomqConfig config, String nodeId, String dataDir, WalConfig walConfig) {
        ServerConfig serverConfig = config.getServerConfig();
        logger.info(
            "Runtime config: nodeId={}, dataDir={}, server={}:{} backlog={} virtualThreads={} maxRequestSize={} threadPoolSize={}, netty={}:{} epoll={} pooledAllocator={} maxConnections={} maxConcurrentRequests={} httpSemaphoreTimeoutMs={}, walDir={}, walEngine={}, walFlushStrategy={}, walFlushThresholdKb={}, walStripeCount={}, schedulerMaxPendingIntents={}, recoveryBatchSize={}, retryInitialDelayMs={}, retryMaxDelayMs={}",
            nodeId,
            dataDir,
            serverConfig.host(),
            serverConfig.port(),
            serverConfig.backlog(),
            serverConfig.virtualThreads(),
            serverConfig.maxRequestSize(),
            serverConfig.threadPoolSize(),
            serverConfig.nettyHost(),
            serverConfig.nettyPort(),
            serverConfig.useEpoll(),
            serverConfig.pooledAllocator(),
            serverConfig.maxConnections(),
            serverConfig.maxConcurrentBusinessRequests(),
            serverConfig.httpSemaphoreTimeoutMs(),
            walConfig.dataDir(),
            walConfig.engine(),
            walConfig.flushStrategy(),
            walConfig.memorySegmentFlushThresholdKb(),
            walConfig.memorySegmentStripeCount(),
            config.getSchedulerConfig().maxPendingIntents(),
            config.getRecoveryConfig().batchSize(),
            config.getRetryConfig().initialDelayMs(),
            config.getRetryConfig().maxDelayMs()
        );
    }

    private static String resolveSetting(String envKey, String propertyKey, String fallback) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }

        String propertyValue = System.getProperty(propertyKey);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }

        return fallback;
    }

    /**
     * 根据配置创建投递处理器。
     *
     * <p>配置方式：
     * <ul>
     *   <li>环境变量：LOOMQ_DELIVERY_HANDLER=single|batched</li>
     *   <li>系统属性：-Dloomq.delivery.handler=single|batched</li>
     *   <li>默认：single（单请求模式）</li>
     * </ul>
     *
     * <p>批量模式可选配置：
     * <ul>
     *   <li>LOOMQ_DELIVERY_BATCH_SIZE / -Dloomq.delivery.batch.size=200</li>
     *   <li>LOOMQ_DELIVERY_FLUSH_INTERVAL_MS / -Dloomq.delivery.flush.interval.ms=10</li>
     *   <li>LOOMQ_DELIVERY_MAX_CONNECTIONS / -Dloomq.delivery.max.connections=500</li>
     * </ul>
     */
    private static com.loomq.spi.DeliveryHandler createDeliveryHandler() {
        String handlerType = resolveSetting("LOOMQ_DELIVERY_HANDLER", "loomq.delivery.handler", "single");

        if ("batched".equalsIgnoreCase(handlerType)) {
            int batchSize = Integer.parseInt(
                resolveSetting("LOOMQ_DELIVERY_BATCH_SIZE", "loomq.delivery.batch.size", "200"));
            long flushIntervalMs = Long.parseLong(
                resolveSetting("LOOMQ_DELIVERY_FLUSH_INTERVAL_MS", "loomq.delivery.flush.interval.ms", "10"));
            int maxConnections = Integer.parseInt(
                resolveSetting("LOOMQ_DELIVERY_MAX_CONNECTIONS", "loomq.delivery.max.connections", "500"));

            BatchDeliveryConfig config = new BatchDeliveryConfig(
                batchSize, flushIntervalMs, maxConnections, true, 30000);

            logger.info("Using BatchedHttpDeliveryHandler: batchSize={}, flushIntervalMs={}, maxConnections={}",
                batchSize, flushIntervalMs, maxConnections);

            return new BatchedHttpDeliveryHandler(config);
        }

        if ("grpc-stream".equalsIgnoreCase(handlerType)) {
            // gRPC 流投递模式
            String ackModeStr = resolveSetting("LOOMQ_DELIVERY_ACK_MODE", "loomq.delivery.ack.mode", "auto");
            DeliveryAckMode ackMode = "manual".equalsIgnoreCase(ackModeStr)
                ? DeliveryAckMode.MANUAL_ACK
                : DeliveryAckMode.AUTO_ACK;

            logger.info("Using GrpcStreamDeliveryHandler: ackMode={}", ackMode);
            return new com.loomq.channel.grpc.server.GrpcStreamDeliveryHandler(ackMode);
        }

        logger.info("Using NettyHttpDeliveryHandler (single-request mode)");
        return new NettyHttpDeliveryHandler();
    }

    static void validateSupportedStartupModes() {
        boolean legacyReplicationEnabled = Boolean.parseBoolean(
            resolveSetting("LOOMQ_CLUSTER_ENABLED", "loomq.cluster.enabled", "false"));
        if (legacyReplicationEnabled) {
            throw new IllegalStateException(
                "Legacy cluster/replication mode is no longer supported. " +
                    "Enable Raft with LOOMQ_RAFT_ENABLED and LOOMQ_RAFT_PEERS instead.");
        }
    }

    static List<String> parseRaftPeerIds(String peersSpec, String selfNodeId) {
        if (peersSpec == null || peersSpec.isBlank()) {
            return List.of(selfNodeId);
        }

        List<String> peers = new ArrayList<>();
        HashSet<String> seen = new HashSet<>();
        for (String raw : peersSpec.split(",")) {
            String token = raw.trim();
            if (token.isEmpty()) {
                continue;
            }
            String nodeId = extractPeerNodeId(token);
            if (nodeId.isEmpty()) {
                throw new IllegalStateException("Raft peer entry is blank");
            }
            if (!seen.add(nodeId)) {
                throw new IllegalStateException("Duplicate Raft peer id: " + nodeId);
            }
            peers.add(nodeId);
        }

        if (peers.isEmpty()) {
            return List.of(selfNodeId);
        }
        return List.copyOf(peers);
    }

    static List<RaftPeerTarget> parseRaftPeerTargets(String peersSpec, String selfNodeId) {
        if (peersSpec == null || peersSpec.isBlank()) {
            return List.of();
        }

        List<RaftPeerTarget> targets = new ArrayList<>();
        HashSet<String> seen = new HashSet<>();
        for (String raw : peersSpec.split(",")) {
            String token = raw.trim();
            if (token.isEmpty()) {
                continue;
            }

            RaftPeerTarget target = parsePeerTarget(token, selfNodeId);
            if (target != null) {
                if (!seen.add(target.nodeId())) {
                    throw new IllegalStateException("Duplicate Raft peer endpoint: " + target.nodeId());
                }
                targets.add(target);
            }
        }
        return List.copyOf(targets);
    }

    private static String extractPeerNodeId(String token) {
        int at = token.indexOf('@');
        int eq = token.indexOf('=');
        int sep = at >= 0 ? at : eq;
        if (sep > 0) {
            return token.substring(0, sep).trim();
        }
        return token.trim();
    }

    private static RaftPeerTarget parsePeerTarget(String token, String selfNodeId) {
        int at = token.indexOf('@');
        int eq = token.indexOf('=');
        int sep = at >= 0 ? at : eq;
        if (sep <= 0) {
            String peerId = token.trim();
            if (peerId.isEmpty()) {
                throw new IllegalStateException("Raft peer entry is blank");
            }
            if (peerId.equals(selfNodeId)) {
                return null;
            }
            throw new IllegalStateException(
                "Raft peer '" + peerId + "' is missing host:port endpoint; use peerId@host:port or peerId=host:port");
        }

        String peerId = token.substring(0, sep).trim();
        if (peerId.isEmpty()) {
            throw new IllegalStateException("Raft peer id is blank in '" + token + "'");
        }
        if (peerId.equals(selfNodeId)) {
            return null;
        }

        String endpoint = token.substring(sep + 1).trim();
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            throw new IllegalStateException(
                "Invalid Raft peer endpoint '" + token + "', expected peerId@host:port or peerId=host:port");
        }

        String host = endpoint.substring(0, colon).trim();
        String portStr = endpoint.substring(colon + 1).trim();
        if (host.isEmpty()) {
            throw new IllegalStateException("Raft peer host is blank in '" + token + "'");
        }
        try {
            int port = Integer.parseInt(portStr);
            if (port <= 0 || port > 65535) {
                throw new IllegalStateException("Raft peer port must be in range [1, 65535]: " + token);
            }
            return new RaftPeerTarget(peerId, host, port);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid Raft peer port in '" + token + "': " + portStr, e);
        }
    }


    private static void connectRaftPeers(com.loomq.raft.RaftTransport transport,
                                         List<RaftPeerTarget> targets,
                                         String selfNodeId) {
        for (RaftPeerTarget target : targets) {
            if (target.nodeId().equals(selfNodeId)) {
                continue;
            }

            try {
                transport.connect(target.nodeId(), target.host(), target.port()).join();
                logger.info("Raft peer connected: {} -> {}:{}", target.nodeId(), target.host(), target.port());
            } catch (Exception e) {
                logger.warn("Raft peer connect failed: {} -> {}:{}",
                    target.nodeId(), target.host(), target.port(), e);
            }
        }
    }

    private static void printBanner() {
        logger.info("");
        logger.info("██╗      ██████╗  ██████╗ ███╗   ███╗ ██████╗ ");
        logger.info("██║     ██╔═══██╗██╔═══██╗████╗ ████║██╔═══██╗");
        logger.info("██║     ██║   ██║██║   ██║██╔████╔██║██║   ██║");
        logger.info("██║     ██║   ██║██║   ██║██║╚██╔╝██║██║▄▄ ██║");
        logger.info("███████╗╚██████╔╝╚██████╔╝██║ ╚═╝ ██║╚██████╔╝");
        logger.info("╚══════╝ ╚═════╝  ╚═════╝ ╚═╝     ╚═╝ ╚══▀▀═╝ ");
        logger.info("");
        logger.info(" Event Infrastructure for Delayed Execution");
        logger.info("              Version 0.9.2");
        logger.info("              Mode: Server (Netty)");
        logger.info("");
    }

    private static void registerSystemRoutes(RadixRouter router, LoomqEngine engine,
                                             RaftStatusProvider raftStatus) {
        router.add(HttpMethod.GET, "/health", (method, uri, body, headers, pathParams) -> {
            String accept = headers != null ? headers.getOrDefault("Accept", "") : "";
            if (accept.contains("text/plain")) {
                var health = buildHealthResponse(engine, raftStatus);
                return health.get("status") + " — " + health.get("timestamp");
            }
            return HealthNarrator.narrate(engine);
        });
        router.add(HttpMethod.GET, "/health/live", (method, uri, body, headers, pathParams) ->
            HEALTH_LIVE_RESPONSE);
        router.add(HttpMethod.GET, "/health/ready", (method, uri, body, headers, pathParams) ->
            buildReadinessResponse(raftStatus, LoomQMetrics.getInstance().snapshot()));
        router.add(HttpMethod.GET, "/health/deep", (method, uri, body, headers, pathParams) ->
            buildDeepHealthResponse(engine, raftStatus));
        router.add(HttpMethod.GET, "/metrics", (method, uri, body, headers, pathParams) -> {
            String accept = headers.getOrDefault("Accept", "application/json");
            if (accept.contains("text/plain")) {
                return new com.loomq.http.netty.NettyRequestHandler.TextResponse(
                    MetricsCollector.getInstance().exportPrometheusMetrics());
            }
            return LoomQMetrics.getInstance().snapshot();
        });
        router.add(HttpMethod.GET, "/v1/system/chronoscope", (method, uri, body, headers, pathParams) ->
            com.loomq.application.scheduler.ChronoscopeSnapshot.from(
                engine.getScheduler(),
                engine.getBucketGroupManager(),
                engine.getScheduler().getCohortManager()));
        router.add(HttpMethod.GET, "/v1/system/timeline", (method, uri, body, headers, pathParams) -> {
            java.time.Instant now = java.time.Instant.now();
            java.time.Instant from = now;
            java.time.Instant to = now.plusSeconds(1800); // default 30 min window
            int queryStart = uri.indexOf('?');
            if (queryStart >= 0 && queryStart < uri.length() - 1) {
                String query = uri.substring(queryStart + 1);
                for (String pair : query.split("&")) {
                    int eq = pair.indexOf('=');
                    if (eq <= 0) continue;
                    String key = pair.substring(0, eq);
                    String value = pair.substring(eq + 1);
                    if ("from".equals(key) && !value.isBlank()) {
                        from = java.time.Instant.parse(value);
                    } else if ("to".equals(key) && !value.isBlank()) {
                        to = java.time.Instant.parse(value);
                    }
                }
            }
            if (!from.isBefore(to)) {
                return new HttpErrorResponse(400,
                    ErrorResponse.of("40010", "Query parameter 'from' must be before 'to'"));
            }
            if (java.time.Duration.between(from, to).toHours() > 24) {
                return new HttpErrorResponse(400,
                    ErrorResponse.of("40011", "Timeline window exceeds maximum of 24 hours"));
            }
            return TimelineService.build(
                engine.getScheduler().getCohortManager(), engine.getScheduler(), from, to);
        });
        router.add(HttpMethod.GET, "/v1/system/wal/segments", (method, uri, body, headers, pathParams) ->
            WalReplayService.listSegments(engine.getWalAccessor()));
        router.add(HttpMethod.GET, "/v1/system/wal/replay", (method, uri, body, headers, pathParams) -> {
            String intentId = null;
            int queryStart = uri.indexOf('?');
            if (queryStart >= 0 && queryStart < uri.length() - 1) {
                String query = uri.substring(queryStart + 1);
                for (String pair : query.split("&")) {
                    int eq = pair.indexOf('=');
                    if (eq <= 0) continue;
                    if ("intentId".equals(pair.substring(0, eq))) {
                        intentId = pair.substring(eq + 1);
                    }
                }
            }
            if (intentId == null || intentId.isBlank()) {
                return new HttpErrorResponse(400,
                    com.loomq.api.ErrorResponse.of("40004", "Query parameter 'intentId' is required"));
            }
            return WalReplayService.replayByIntentId(engine.getWalAccessor(), intentId);
        });
    }

    private static Map<String, Object> buildHealthResponse(LoomqEngine engine, RaftStatusProvider raftStatus) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("status", engine.getWalHealth().status());
        root.put("timestamp", java.time.Instant.now().toString());
        root.put("wal", buildWalHealthSection(engine));
        root.put("raft", buildRaftHealthSection(raftStatus, LoomQMetrics.getInstance().snapshot()));
        return root;
    }

    private static Map<String, Object> buildDeepHealthResponse(LoomqEngine engine, RaftStatusProvider raftStatus) {
        Map<String, Object> root = buildHealthResponse(engine, raftStatus);
        Map<String, Object> tiers = new LinkedHashMap<>();
        var backpressure = engine.getScheduler().getBackpressureStatus();
        for (var entry : backpressure.entrySet()) {
            var info = entry.getValue();
            Map<String, Object> infoMap = new LinkedHashMap<>();
            infoMap.put("maxConcurrency", info.maxConcurrency());
            infoMap.put("activeDispatches", info.activeDispatches());
            infoMap.put("availablePermits", info.availablePermits());
            infoMap.put("queueSize", info.queueSize());
            infoMap.put("utilizationPct", String.format("%.1f", info.utilizationPct()));
            infoMap.put("underBackpressure", info.underBackpressure());
            tiers.put(entry.getKey().name(), infoMap);
        }
        root.put("tiers", tiers);
        return root;
    }

    static Object buildReadinessResponse(RaftStatusProvider raftStatus, LoomQMetrics.MetricsSnapshot metrics) {
        Map<String, Object> readiness = buildReadinessDetails(raftStatus, metrics);
        if (Boolean.TRUE.equals(readiness.get("ready"))) {
            return readiness;
        }

        return new HttpErrorResponse(
            503,
            ErrorResponse.of(READINESS_ERROR_CODE, "Node is not ready to serve client traffic", readiness)
        );
    }

    static Map<String, Object> buildReadinessDetails(RaftStatusProvider raftStatus,
                                                     LoomQMetrics.MetricsSnapshot metrics) {
        Map<String, Object> ready = new LinkedHashMap<>();
        boolean walHealthy = metrics.walHealthy();
        ready.put("status", walHealthy ? "UP" : "DOWN");
        ready.put("walHealthy", walHealthy);

        if (raftStatus == null || !raftStatus.isRaftEnabled()) {
            ready.put("ready", walHealthy);
            ready.put("mode", "embedded");
            ready.put("raftEnabled", false);
            ready.put("acceptingReads", walHealthy);
            ready.put("acceptingWrites", walHealthy);
            ready.put("reason", walHealthy ? "READY" : "WAL_UNHEALTHY");
            return ready;
        }

        RaftStatusSnapshot status = raftStatus.snapshotStatus();
        boolean quorumReachable = hasRaftQuorum(status);
        boolean acceptingReads = raftStatus.canServeLinearizableRead();
        boolean acceptingWrites = isAcceptingRaftWrites(status, metrics, quorumReachable);
        boolean raftReady = walHealthy && acceptingReads && acceptingWrites;

        ready.put("ready", raftReady);
        ready.put("mode", "raft");
        ready.put("raftEnabled", true);
        ready.put("nodeId", status.nodeId());
        ready.put("role", status.role().name());
        ready.put("leaderId", status.leaderId());
        ready.put("epoch", status.epoch());
        ready.put("commitIndex", status.commitIndex());
        ready.put("lastApplied", status.lastApplied());
        ready.put("commitLag", status.commitLag());
        ready.put("connectedPeers", status.connectedPeers());
        ready.put("totalPeers", status.totalPeers());
        ready.put("quorumReachable", quorumReachable);
        ready.put("acceptingReads", acceptingReads);
        ready.put("acceptingWrites", acceptingWrites);
        ready.put("writePending", metrics.raftPendingWrites());
        ready.put("reason", readinessReason(walHealthy, status, acceptingReads, quorumReachable, metrics));
        return ready;
    }

    private static Map<String, Object> buildWalHealthSection(LoomqEngine engine) {
        Map<String, Object> wal = new LinkedHashMap<>();
        var status = engine.getWalHealth();
        wal.put("status", status.status());
        wal.put("unflushedBytes", status.unflushedBytes());
        wal.put("lastFsyncMsAgo", status.lastFsyncMsAgo());
        wal.put("writePosition", status.writePosition());
        wal.put("flushedPosition", status.flushedPosition());
        return wal;
    }

    private static Map<String, Object> buildRaftHealthSection(RaftStatusProvider raftStatus,
                                                              LoomQMetrics.MetricsSnapshot metrics) {
        Map<String, Object> raft = new LinkedHashMap<>();
        if (raftStatus == null || !raftStatus.isRaftEnabled()) {
            raft.put("enabled", false);
            raft.put("acceptingReads", false);
            raft.put("acceptingWrites", false);
            return raft;
        }

        RaftStatusSnapshot status = raftStatus.snapshotStatus();
        raft.put("enabled", true);
        raft.put("nodeId", status.nodeId());
        raft.put("role", status.role().name());
        raft.put("leaderId", status.leaderId());
        raft.put("epoch", status.epoch());
        raft.put("commitIndex", status.commitIndex());
        raft.put("lastApplied", status.lastApplied());
        raft.put("commitLag", status.commitLag());
        raft.put("replicationLag", status.replicationLag());
        raft.put("connectedPeers", status.connectedPeers());
        raft.put("totalPeers", status.totalPeers());
        raft.put("peerReachability", status.peerReachability());
        raft.put("quorumReachable", hasRaftQuorum(status));
        raft.put("writePending", metrics.raftPendingWrites());
        raft.put("writeProposalLatencyMs", metrics.raftWriteProposalLatencyMs());
        raft.put("writeProposalLatencyMaxMs", metrics.raftWriteProposalLatencyMaxMs());
        raft.put("writeTimeouts", metrics.raftWriteTimeouts());
        raft.put("writeStepDownAborts", metrics.raftWriteStepDownAborts());
        raft.put("acceptingReads", raftStatus.canServeLinearizableRead());
        raft.put("acceptingWrites", isAcceptingRaftWrites(status, metrics, hasRaftQuorum(status)));
        return raft;
    }

    private static boolean isAcceptingRaftWrites(RaftStatusSnapshot status,
                                                 LoomQMetrics.MetricsSnapshot metrics,
                                                 boolean quorumReachable) {
        return status.isLeader()
            && quorumReachable
            && status.commitLag() == 0
            && metrics.raftPendingWrites() == 0;
    }

    private static boolean hasRaftQuorum(RaftStatusSnapshot status) {
        int clusterSize = status.totalPeers() + 1;
        int reachableNodes = status.connectedPeers() + 1;
        return reachableNodes >= (clusterSize / 2) + 1;
    }

    private static String readinessReason(boolean walHealthy,
                                          RaftStatusSnapshot status,
                                          boolean acceptingReads,
                                          boolean quorumReachable,
                                          LoomQMetrics.MetricsSnapshot metrics) {
        if (!walHealthy) {
            return "WAL_UNHEALTHY";
        }
        if (!status.isLeader()) {
            return "RAFT_NOT_LEADER";
        }
        if (!quorumReachable) {
            return "RAFT_QUORUM_UNREACHABLE";
        }
        if (!acceptingReads) {
            return "RAFT_READ_LEASE_UNAVAILABLE";
        }
        if (status.commitLag() > 0) {
            return "RAFT_COMMIT_LAG";
        }
        if (metrics.raftPendingWrites() > 0) {
            return "RAFT_PENDING_WRITES";
        }
        return "READY";
    }

    private static final class RaftRuntimeBridge implements RaftRuntimeListener {
        private final LoomqEngine engine;

        private RaftRuntimeBridge(LoomqEngine engine) {
            this.engine = engine;
        }

        @Override
        public void onRoleChanged(RaftRole role, long term) {
            if (role == RaftRole.LEADER) {
                engine.getScheduler().rebuildFromCommittedState(engine.getIntentStore().getAllIntents().values());
                engine.getScheduler().resume();
            } else {
                engine.getScheduler().pause();
            }
        }

        @Override
        public void onCommittedIntent(Intent intent, boolean isNew, boolean leader) {
            if (!leader || intent == null) {
                return;
            }

            if (intent.getStatus().isTerminal()) {
                engine.getScheduler().unschedule(intent);
                if (intent.getStatus() == IntentStatus.CANCELED) {
                    MetricsCollector.getInstance().incrementIntentsCancelled();
                }
                return;
            }

            if (isNew) {
                engine.getScheduler().schedule(intent);
                MetricsCollector.getInstance().incrementIntentsCreated();
            } else {
                engine.getScheduler().restore(intent);
            }
        }
    }

    record RaftPeerTarget(String nodeId, String host, int port) {}
}
