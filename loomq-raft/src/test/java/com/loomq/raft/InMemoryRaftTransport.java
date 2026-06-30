package com.loomq.raft;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 单元测试用 Transport 实现。直接传递 DTO，无网络。
 */
public class InMemoryRaftTransport implements RaftTransport {

    private Function<AppendEntriesRequest, AppendEntriesResponse> appendEntriesHandler;
    private Function<InstallSnapshotRequest, InstallSnapshotResponse> installSnapshotHandler;
    private final Set<String> connectedPeers = ConcurrentHashMap.newKeySet();

    @Override
    public void start() {
        // no-op
    }

    @Override
    public CompletableFuture<Void> connect(String peerId, String host, int port) {
        connectedPeers.add(peerId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void disconnect(String peerId) {
        connectedPeers.remove(peerId);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(
            String peerId, AppendEntriesRequest request) {
        if (appendEntriesHandler != null) {
            return CompletableFuture.completedFuture(appendEntriesHandler.apply(request));
        }
        return CompletableFuture.completedFuture(
            new AppendEntriesResponse(request.epoch(), false, -1, -1));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(
            String peerId, InstallSnapshotRequest request) {
        if (installSnapshotHandler != null) {
            return CompletableFuture.completedFuture(installSnapshotHandler.apply(request));
        }
        return CompletableFuture.completedFuture(
            new InstallSnapshotResponse(request.epoch(), 0));
    }

    @Override
    public void setOnAppendEntries(
            Function<AppendEntriesRequest, AppendEntriesResponse> handler) {
        this.appendEntriesHandler = handler;
    }

    @Override
    public void setOnInstallSnapshot(
            Function<InstallSnapshotRequest, InstallSnapshotResponse> handler) {
        this.installSnapshotHandler = handler;
    }

    @Override
    public void setCurrentEpochSupplier(java.util.function.Supplier<Long> supplier) {
        // no-op: test transport doesn't need epoch validation
    }

    @Override
    public boolean isPeerConnected(String peerId) {
        return connectedPeers.contains(peerId);
    }

    @Override
    public int connectedPeerCount() {
        return connectedPeers.size();
    }

    @Override
    public void close() {
        // no-op
    }
}
