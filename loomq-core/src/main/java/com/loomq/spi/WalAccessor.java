package com.loomq.spi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * WAL 读取访问器。
 *
 * 内核通过此接口暴露 WAL 的读取能力，供服务层（如 ReplicationManager）
 * 读取 WAL 记录用于副本同步、追赶等场景。
 *
 * 默认由 SimpleWalWriter 实现。
 */
public interface WalAccessor {

    /** 当前写入位置（全局偏移） */
    long getWritePosition();

    /** 已刷盘位置（全局偏移） */
    long getFlushedPosition();

    /**
     * 按全局位置读取一条 WAL 记录，校验 CRC 后返回 payload。
     *
     * @param position     WAL 全局起始位置
     * @param recordLength 整条记录的长度
     * @return payload 字节数组
     * @throws IOException 读取失败或 CRC 校验不通过
     */
    byte[] readRecord(long position, int recordLength) throws IOException;

    /**
     * 获取所有 WAL 段文件列表。
     *
     * @return 按序号排序的段文件信息
     */
    List<WalSegment> listSegments();

    /**
     * 快照覆盖到的最后一条 log entry index。
     *
     * @return 快照最后覆盖的 index；没有快照时返回 0
     */
    default long getSnapshotIndex() {
        return 0;
    }

    /**
     * 快照覆盖到的最后一条 log entry epoch。
     *
     * @return 快照最后覆盖的 epoch；没有快照时返回 0
     */
    default long getSnapshotEpoch() {
        return 0;
    }

    /**
     * 快照覆盖边界对应的物理 WAL 偏移。
     *
     * 该值通常是快照最后一条 entry 结束后的全局偏移，
     * 也就是第一条保留日志的起始位置。旧实现可返回 0。
     */
    default long getSnapshotOffset() {
        return 0;
    }

    /**
     * 持久化快照元数据。
     *
     * @param snapshotIndex 快照最后覆盖的 log index
     * @param snapshotEpoch  快照最后覆盖的 log epoch
     */
    default void setSnapshotMetadata(long snapshotIndex, long snapshotEpoch) {
        setSnapshotMetadata(snapshotIndex, snapshotEpoch, getWritePosition());
    }

    /**
     * 持久化快照元数据（包含物理边界偏移）。
     *
     * @param snapshotIndex  快照最后覆盖的 log index
     * @param snapshotEpoch   快照最后覆盖的 log epoch
     * @param snapshotOffset 快照最后覆盖后的物理 WAL 偏移
     */
    default void setSnapshotMetadata(long snapshotIndex, long snapshotEpoch, long snapshotOffset) {
        // No-op for read-only implementations.
    }

    /**
     * 截断指定全局偏移之前的所有 WAL 段文件（释放磁盘空间）。
     * 仅在对应偏移已被快照持久化后调用。
     *
     * @param globalOffset 截断点（该位置之前的数据可安全删除）
     */
    void truncateBefore(long globalOffset);

    // ========== Raft 共识感知方法 ==========

    /** Raft 协议：最后一条 log entry 的 index（复用 WAL 全局偏移） */
    default long getLastLogIndex() {
        return getWritePosition() > 0 ? getWritePosition() : 0;
    }

    /**
     * Raft 协议：第一条可用 log entry 的 index。
     * 用于 InstallSnapshot 决策——当 follower 的 nextIndex 低于此值时，
     * leader 必须发送快照而非 AppendEntries。
     *
     * @return 第一条可用 log index（默认快照最后覆盖 index + 1；无快照时为 1）
     */
    default long getFirstLogIndex() {
        long snapshotIndex = getSnapshotIndex();
        return snapshotIndex > 0 ? snapshotIndex + 1 : 1;
    }

    /** Raft 协议：最后一条 log entry 的 epoch */
    long getLastLogEpoch();

    /**
     * Raft 协议：最后一条实际写入的 log entry 的 epoch。
     *
     * 与 {@link #getLastLogEpoch()} 不同，此方法返回最后一条已持久化 entry 的 epoch，
     * 而非 Raft currentEpoch。用于 §5.4.1 isUpToDate 判断。
     *
     * @return 最后一条 log entry 的 epoch（无 entry 时为 0）
     */
    default long getLastLogEntryEpoch() {
        return 0;
    }

    /** Raft 协议：记录当前 epoch（leader 选举时更新） */
    void setCurrentEpoch(long epoch);

    /** Raft 协议：读取 votedFor */
    String getVotedFor();

    /** Raft 协议：记录 votedFor（选举时更新） */
    void setVotedFor(String nodeId);

    /** Raft 协议：原子性设置 epoch 和 votedFor（默认分别调用 setCurrentEpoch/setVotedFor） */
    default void setEpochAndVotedFor(long epoch, String votedFor) {
        setCurrentEpoch(epoch);
        setVotedFor(votedFor);
    }

    /**
     * 写入一条 WAL 记录（供 Raft 日志复制使用）。
     *
     * 默认实现抛出 UnsupportedOperationException。
     * 只有支持写入的 WalAccessor（如 SimpleWalWriter）才覆盖此方法。
     * 只读访问器不应调用此方法。
     *
     * @param data 原始数据（不含 WAL 帧头/CRC，由实现负责封装）
     * @return 写入后的全局结束偏移（即 log index），供 RaftLog 直接使用
     * @throws UnsupportedOperationException 如果此 WalAccessor 是只读的
     */
    default long writeEntry(byte[] data) {
        throw new UnsupportedOperationException(
            "This WalAccessor implementation is read-only and does not support writeEntry(). " +
            "Use SimpleWalWriter or another writable implementation for Raft log operations.");
    }

    /**
     * 返回 WAL 记录的固定开销（帧头 + CRC 字节数）。
     * 调用方用于从 log index（结束位置）反算起始位置。
     *
     * @return 固定字节开销（默认 8 = 4B 长度 + 4B CRC32）
     */
    default int getRecordOverhead() {
        return 8;
    }

    /**
     * 同步持久化 Raft 元数据（currentEpoch、votedFor）到磁盘。
     *
     * Raft §5.2 要求：节点在回复任何 RPC 之前，必须确保持久化 currentEpoch 和 votedFor。
     * 调用此方法可以保证所有待写入的 Raft 元数据已落盘。
     *
     * 默认实现为空操作（只读访问器无需实现）。
     * SimpleWalWriter 覆盖此方法，提交保存任务到 raftMetaExecutor 并等待完成。
     */
    default void persistRaftMeta() {
        // No-op: only writable implementations need to persist Raft metadata
    }

    /**
     * 将写入位置回退到指定全局偏移，丢弃之后的所有数据。
     *
     * 用于 Raft 日志截断：follower 发现日志冲突时，需要回退 writePosition
     * 以丢弃冲突的 entries，然后 leader 重新发送正确的 entries。
     *
     * 调用方保证 globalOffset 不会超过当前 writePosition。
     * 实现应删除 globalOffset 之后的所有段文件，并在 globalOffset 处
     * 创建新段以保证后续写入的连续性。
     *
     * 默认实现抛出 UnsupportedOperationException（只读访问器无需实现）。
     *
     * @param globalOffset 新的写入起始位置（必须 <= 当前 writePosition）
     * @throws UnsupportedOperationException 如果此 WalAccessor 不支持写入
     * @throws IllegalArgumentException 如果 globalOffset 超出范围
     */
    default void resetTo(long globalOffset) {
        throw new UnsupportedOperationException(
            "This WalAccessor implementation is read-only and does not support resetTo(). " +
            "Use SimpleWalWriter or another writable implementation for Raft log truncation.");
    }

    /**
     * WAL 段文件信息。
     */
    record WalSegment(int index, Path path, long startOffset, long endOffset, long size) {}
}
