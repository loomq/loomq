package com.loomq.raft;

import com.google.protobuf.ByteString;
import com.loomq.raft.grpc.RaftServiceGrpc;
import com.loomq.raft.grpc.SnapshotChunk;
import com.loomq.raft.grpc.SnapshotResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC-based Raft RPC Transport。
 *
 * <p>实现 {@link RaftTransport} 接口，使用 gRPC HTTP/2 通信。
 * 保留分块 InstallSnapshot 逻辑。
 *
 * <p>注意：protobuf 生成的 {@code AppendEntriesRequest} / {@code AppendEntriesResponse}
 * 与 {@link RaftTransport} 接口中的 DTO record 同名。
 * 由于本类实现 RaftTransport，内部类型会遮蔽 import，
 * 因此 protobuf 类型使用完全限定名。
 */
public class GrpcRaftTransport implements RaftTransport {
    private static final Logger log = LoggerFactory.getLogger(GrpcRaftTransport.class);
    private static final int SNAPSHOT_CHUNK_SIZE = 256 * 1024; // 256KB per chunk
    private static final long MAX_SNAPSHOT_BYTES = 256L * 1024 * 1024; // 256MB max snapshot
    private final String nodeId;
    private final Object connectionLock = new Object();
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> blockingStubs = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceStub> asyncStubs = new ConcurrentHashMap<>();
    private final ExecutorService rpcExecutor = Executors.newFixedThreadPool(4, r -> {
        Thread t = new Thread(r, "grpc-raft-rpc");
        t.setDaemon(true);
        return t;
    });
    private final ExecutorService snapshotExecutor = Executors.newFixedThreadPool(2, r -> {
        Thread t = new Thread(r, "grpc-raft-snapshot");
        t.setDaemon(true);
        return t;
    });
    private Server grpcServer;

    // Server-side handlers (DTO-based)
    private volatile Function<RaftTransport.AppendEntriesRequest, RaftTransport.AppendEntriesResponse> appendEntriesHandler;
    private volatile Function<RaftTransport.InstallSnapshotRequest, RaftTransport.InstallSnapshotResponse> installSnapshotHandler;
    private volatile java.util.function.Supplier<Long> currentEpochSupplier = () -> 0L;

    private String listenHost;
    private int listenPort;

    public GrpcRaftTransport(String nodeId) {
        this.nodeId = nodeId;
    }

    // ========== RaftTransport interface implementation ==========

    @Override
    public void setOnAppendEntries(
            Function<RaftTransport.AppendEntriesRequest, RaftTransport.AppendEntriesResponse> handler) {
        this.appendEntriesHandler = handler;
    }

    @Override
    public void setOnInstallSnapshot(
            Function<RaftTransport.InstallSnapshotRequest, RaftTransport.InstallSnapshotResponse> handler) {
        this.installSnapshotHandler = handler;
    }

    @Override
    public void setCurrentEpochSupplier(java.util.function.Supplier<Long> supplier) {
        this.currentEpochSupplier = supplier;
    }

    /** Start gRPC server to listen for incoming Raft RPCs */
    @Override
    public void start() throws Exception {
        if (listenPort <= 0) {
            throw new IllegalStateException("listenHost and listenPort must be set via setListenAddress() before start()");
        }
        if (listenHost != null && !listenHost.isEmpty()) {
            grpcServer = io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
                .forAddress(new java.net.InetSocketAddress(listenHost, listenPort))
                .addService(new RaftServiceImpl())
                .build()
                .start();
        } else {
            grpcServer = ServerBuilder.forPort(listenPort)
                .addService(new RaftServiceImpl())
                .build()
                .start();
        }
        log.info("GrpcRaftTransport listening on {}:{}", listenHost, listenPort);
    }

    /**
     * 设置监听地址。必须在 {@link #start()} 之前调用。
     */
    public void setListenAddress(String host, int port) {
        this.listenHost = host;
        this.listenPort = port;
    }

    /** Connect to a peer via gRPC */
    @Override
    public CompletableFuture<Void> connect(String peerId, String host, int port) {
        return CompletableFuture.runAsync(() -> {
            synchronized (connectionLock) {
                ManagedChannel newChannel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
                ManagedChannel existing = channels.put(peerId, newChannel);
                if (existing != null) {
                    existing.shutdown();
                }
                blockingStubs.put(peerId, RaftServiceGrpc.newBlockingStub(newChannel));
                asyncStubs.put(peerId, RaftServiceGrpc.newStub(newChannel));
            }
            log.info("GrpcRaftTransport connected to {} at {}:{}", peerId, host, port);
        }, rpcExecutor);
    }

    @Override
    public void disconnect(String peerId) {
        ManagedChannel channel = channels.remove(peerId);
        if (channel != null) {
            channel.shutdown();
            blockingStubs.remove(peerId);
            asyncStubs.remove(peerId);
        }
    }

    /** Whether the outbound connection to a peer is currently active */
    @Override
    public boolean isPeerConnected(String peerId) {
        ManagedChannel channel = channels.get(peerId);
        return channel != null && !channel.isShutdown();
    }

    /** Number of currently connected peers */
    @Override
    public int connectedPeerCount() {
        int connected = 0;
        for (ManagedChannel channel : channels.values()) {
            if (channel != null && !channel.isShutdown()) {
                connected++;
            }
        }
        return connected;
    }

    // ========== Client-side RPCs ==========

    /** Send AppendEntries to a peer */
    @Override
    public CompletableFuture<RaftTransport.AppendEntriesResponse> sendAppendEntries(
            String peerId, RaftTransport.AppendEntriesRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            RaftServiceGrpc.RaftServiceBlockingStub stub = blockingStubs.get(peerId);
            if (stub == null) {
                return new RaftTransport.AppendEntriesResponse(request.epoch(), false, -1, -1);
            }
            try {
                // DTO -> protobuf (use fully-qualified name to avoid collision with RaftTransport inner types)
                com.loomq.raft.grpc.AppendEntriesRequest.Builder builder =
                    com.loomq.raft.grpc.AppendEntriesRequest.newBuilder()
                        .setEpoch(request.epoch())
                        .setLeaderId(request.leaderId())
                        .setPrevLogIndex(request.prevLogIndex())
                        .setPrevLogEpoch(request.prevLogEpoch())
                        .setLeaderCommit(request.leaderCommit());
                if (request.entries() != null) {
                    for (byte[] entry : request.entries()) {
                        if (entry != null) {
                            builder.addEntries(ByteString.copyFrom(entry));
                        }
                    }
                }
                com.loomq.raft.grpc.AppendEntriesResponse protoResp =
                    stub.withDeadlineAfter(5, TimeUnit.SECONDS).appendEntries(builder.build());
                // protobuf -> DTO
                return new RaftTransport.AppendEntriesResponse(
                    protoResp.getEpoch(),
                    protoResp.getSuccess(),
                    protoResp.getMatchIndex(),
                    protoResp.getConflictIndex());
            } catch (Exception e) {
                log.warn("AppendEntries to {} failed: {}", peerId, e.getMessage());
                return new RaftTransport.AppendEntriesResponse(request.epoch(), false, -1, -1);
            }
        }, rpcExecutor);
    }

    /** Send InstallSnapshot to a lagging follower — single stream, internally chunked */
    @Override
    public CompletableFuture<RaftTransport.InstallSnapshotResponse> sendInstallSnapshot(
            String peerId, RaftTransport.InstallSnapshotRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            RaftServiceGrpc.RaftServiceStub asyncStub = asyncStubs.get(peerId);
            if (asyncStub == null) {
                return new RaftTransport.InstallSnapshotResponse(request.epoch(), -1);
            }
            try {
                byte[] data = request.data();
                int totalChunks = Math.max(1, (data.length + SNAPSHOT_CHUNK_SIZE - 1) / SNAPSHOT_CHUNK_SIZE);

                CountDownLatch latch = new CountDownLatch(1);
                final long[] result = {-1L};

                StreamObserver<SnapshotResponse> responseObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(SnapshotResponse response) {
                        result[0] = response.getLastIncludedIndex();
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.warn("InstallSnapshot to {} failed: {}", peerId, t.getMessage());
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                };

                // One stream for all chunks
                long deadlineSeconds = Math.max(60, totalChunks * 5L);
                StreamObserver<SnapshotChunk> requestObserver = asyncStub
                    .withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS)
                    .installSnapshot(responseObserver);

                // Use isReady() + setOnReadyHandler for gRPC backpressure
                java.util.concurrent.atomic.AtomicInteger chunkIdx =
                    new java.util.concurrent.atomic.AtomicInteger(0);

                // Cast to CallStreamObserver to access isReady() and setOnReadyHandler
                // Note: The StreamObserver returned by stub.installSnapshot() is a CallStreamObserver
                io.grpc.stub.CallStreamObserver<SnapshotChunk> callStream =
                    (io.grpc.stub.CallStreamObserver<SnapshotChunk>) requestObserver;

                java.lang.Runnable onReadyHandler = () -> {
                    while (callStream.isReady()) {
                        int i = chunkIdx.getAndIncrement();
                        if (i >= totalChunks) {
                            requestObserver.onCompleted();
                            return;
                        }
                        int start = i * SNAPSHOT_CHUNK_SIZE;
                        int end = Math.min(start + SNAPSHOT_CHUNK_SIZE, data.length);
                        byte[] chunk = java.util.Arrays.copyOfRange(data, start, end);
                        requestObserver.onNext(SnapshotChunk.newBuilder()
                            .setEpoch(request.epoch())
                            .setLeaderId(request.leaderId())
                            .setLastIncludedIndex(request.lastIncludedIndex())
                            .setLastIncludedEpoch(request.lastIncludedEpoch())
                            .setChunkIndex(i)
                            .setTotalChunks(totalChunks)
                            .setData(ByteString.copyFrom(chunk))
                            .build());
                    }
                };
                callStream.setOnReadyHandler(onReadyHandler);

                // Trigger first send (gRPC will also call it later if isReady() flips)
                onReadyHandler.run();

                latch.await(deadlineSeconds, TimeUnit.SECONDS);
                return new RaftTransport.InstallSnapshotResponse(request.epoch(), result[0]);
            } catch (Exception e) {
                log.warn("InstallSnapshot to {} failed: {}", peerId, e.getMessage());
                return new RaftTransport.InstallSnapshotResponse(request.epoch(), -1);
            }
        }, snapshotExecutor);
    }

    @Override
    public void close() {
        if (grpcServer != null) {
            gracefulShutdown(grpcServer);
        }
        channels.values().forEach(this::gracefulShutdown);
        gracefulShutdown(rpcExecutor, "rpcExecutor");
        gracefulShutdown(snapshotExecutor, "snapshotExecutor");
    }

    private void gracefulShutdown(java.util.concurrent.ExecutorService svc, String name) {
        svc.shutdown();
        try {
            if (!svc.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("{} did not terminate gracefully, forcing shutdown", name);
                svc.shutdownNow();
            }
        } catch (InterruptedException e) {
            svc.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void gracefulShutdown(io.grpc.Server server) {
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void gracefulShutdown(ManagedChannel channel) {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // ========== gRPC Service Implementation ==========

    /**
     * gRPC 服务实现。所有 protobuf 类型使用完全限定名，
     * 因为 RaftTransport 接口的 DTO record 同名会遮蔽 import。
     */
    private class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {

        @Override
        public void appendEntries(
                com.loomq.raft.grpc.AppendEntriesRequest protoReq,
                StreamObserver<com.loomq.raft.grpc.AppendEntriesResponse> responseObserver) {
            try {
                if (appendEntriesHandler == null) {
                    responseObserver.onNext(com.loomq.raft.grpc.AppendEntriesResponse.newBuilder().build());
                    responseObserver.onCompleted();
                    return;
                }
                // protobuf -> DTO
                List<byte[]> entries = protoReq.getEntriesList().stream()
                    .map(ByteString::toByteArray)
                    .toList();
                RaftTransport.AppendEntriesRequest dtoReq = new RaftTransport.AppendEntriesRequest(
                    protoReq.getEpoch(), protoReq.getLeaderId(),
                    protoReq.getPrevLogIndex(), protoReq.getPrevLogEpoch(),
                    entries, protoReq.getLeaderCommit());

                // 调用 handler
                RaftTransport.AppendEntriesResponse dtoResp = appendEntriesHandler.apply(dtoReq);

                // DTO -> protobuf
                responseObserver.onNext(com.loomq.raft.grpc.AppendEntriesResponse.newBuilder()
                    .setSuccess(dtoResp.success())
                    .setMatchIndex(dtoResp.matchIndex())
                    .setConflictIndex(dtoResp.conflictIndex())
                    .setEpoch(dtoResp.epoch())
                    .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("AppendEntries handler error", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public StreamObserver<SnapshotChunk> installSnapshot(StreamObserver<SnapshotResponse> responseObserver) {
            return new StreamObserver<>() {
                private final java.util.concurrent.atomic.AtomicBoolean errored = new java.util.concurrent.atomic.AtomicBoolean(false);
                private long epoch;
                private String leaderId;
                private long lastIncludedIndex;
                private long lastIncludedEpoch;
                private int totalChunks = -1;
                private byte[][] chunks;
                private int receivedCount = 0;

                @Override
                public void onNext(SnapshotChunk chunk) {
                    try {
                        if (errored.get()) return;
                        if (installSnapshotHandler == null) {
                            log.warn("InstallSnapshot received but no handler registered");
                            errored.set(true);
                            responseObserver.onError(new IllegalStateException("No installSnapshotHandler"));
                            return;
                        }
                        if (totalChunks == -1) {
                            int remoteTotalChunks = chunk.getTotalChunks();
                            if (remoteTotalChunks <= 0 || remoteTotalChunks > 10_000) {
                                errored.set(true);
                                responseObserver.onError(new IllegalArgumentException(
                                    "Invalid totalChunks: " + remoteTotalChunks));
                                return;
                            }
                            long estimatedSize = (long) remoteTotalChunks * SNAPSHOT_CHUNK_SIZE;
                            if (estimatedSize > MAX_SNAPSHOT_BYTES) {
                                log.warn("Rejecting InstallSnapshot: estimated size {} > max {}", estimatedSize, MAX_SNAPSHOT_BYTES);
                                errored.set(true);
                                responseObserver.onError(new IllegalArgumentException(
                                    "Snapshot too large: " + estimatedSize + " > " + MAX_SNAPSHOT_BYTES));
                                return;
                            }
                            epoch = chunk.getEpoch();
                            leaderId = chunk.getLeaderId();
                            lastIncludedIndex = chunk.getLastIncludedIndex();
                            lastIncludedEpoch = chunk.getLastIncludedEpoch();
                            totalChunks = remoteTotalChunks;
                            // Early epoch check — reject stale snapshot before buffering all chunks
                            if (currentEpochSupplier.get() > chunk.getEpoch()) {
                                log.warn("Rejecting stale InstallSnapshot: epoch {} < current epoch {}",
                                    chunk.getEpoch(), currentEpochSupplier.get());
                                errored.set(true);
                                responseObserver.onError(new io.grpc.StatusRuntimeException(
                                    io.grpc.Status.FAILED_PRECONDITION.withDescription("Stale snapshot epoch")));
                                return;
                            }
                            chunks = new byte[totalChunks][];
                        }

                        int idx = chunk.getChunkIndex();
                        if (idx >= 0 && idx < totalChunks && chunks[idx] == null) {
                            chunks[idx] = chunk.getData().toByteArray();
                            receivedCount++;
                        }
                    } catch (Exception e) {
                        log.error("InstallSnapshot chunk processing error", e);
                        errored.set(true);
                        responseObserver.onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.warn("InstallSnapshot stream error from {}: {}", leaderId, t.getMessage());
                    if (errored.compareAndSet(false, true)) {
                        responseObserver.onError(t);
                    }
                }

                @Override
                public void onCompleted() {
                    if (errored.get()) return;
                    try {
                        if (receivedCount != totalChunks) {
                            log.warn("InstallSnapshot incomplete: received {}/{} chunks", receivedCount, totalChunks);
                            responseObserver.onNext(SnapshotResponse.newBuilder()
                                .setLastIncludedIndex(-1)
                                .build());
                            responseObserver.onCompleted();
                            return;
                        }

                        // Reassemble all chunks
                        int totalSize = 0;
                        for (byte[] c : chunks) {
                            totalSize += c != null ? c.length : 0;
                        }
                        byte[] fullData = new byte[totalSize];
                        int offset = 0;
                        for (byte[] c : chunks) {
                            if (c != null) {
                                System.arraycopy(c, 0, fullData, offset, c.length);
                                offset += c.length;
                            }
                        }

                        // Construct DTO (complete snapshot, totalChunks=1)
                        RaftTransport.InstallSnapshotRequest dtoReq = new RaftTransport.InstallSnapshotRequest(
                            epoch, leaderId, lastIncludedIndex, lastIncludedEpoch,
                            0, 1, fullData);

                        RaftTransport.InstallSnapshotResponse dtoResp =
                            installSnapshotHandler.apply(dtoReq);

                        responseObserver.onNext(SnapshotResponse.newBuilder()
                            .setLastIncludedIndex(dtoResp.bytesReceived())
                            .build());
                        responseObserver.onCompleted();
                    } catch (Exception e) {
                        log.error("InstallSnapshot completion error", e);
                        responseObserver.onError(e);
                    }
                }
            };
        }
    }
}
