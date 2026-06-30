package com.loomq.raft;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Raft 节点间传输抽象。
 * DTO 为纯 POJO record，不依赖 protobuf 或 gRPC。
 */
public interface RaftTransport extends AutoCloseable {

    // --- DTO 定义 ---

    record AppendEntriesRequest(
        long epoch, String leaderId, long prevLogIndex, long prevLogEpoch,
        List<byte[]> entries, long leaderCommit
    ) {}

    record AppendEntriesResponse(
        long epoch, boolean success, long matchIndex, long conflictIndex
    ) {}

    record InstallSnapshotRequest(
        long epoch, String leaderId, long lastIncludedIndex,
        long lastIncludedEpoch, int chunkIndex, int totalChunks, byte[] data
    ) {}

    record InstallSnapshotResponse(long epoch, long bytesReceived) {}

    // --- 生命周期 ---

    void start() throws Exception;
    CompletableFuture<Void> connect(String peerId, String host, int port);
    void disconnect(String peerId);

    // --- RPC 方法 ---

    CompletableFuture<AppendEntriesResponse> sendAppendEntries(
        String peerId, AppendEntriesRequest request);
    CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(
        String peerId, InstallSnapshotRequest request);

    // --- 回调注册 ---

    void setOnAppendEntries(
        Function<AppendEntriesRequest, AppendEntriesResponse> handler);
    void setOnInstallSnapshot(
        Function<InstallSnapshotRequest, InstallSnapshotResponse> handler);

    /**
     * Set a supplier for the current epoch, used to reject stale InstallSnapshot
     * requests early (before buffering all chunks).
     */
    void setCurrentEpochSupplier(java.util.function.Supplier<Long> supplier);

    // --- 连接状态查询 ---

    boolean isPeerConnected(String peerId);
    int connectedPeerCount();

    // --- 关闭 ---

    @Override
    void close();
}
