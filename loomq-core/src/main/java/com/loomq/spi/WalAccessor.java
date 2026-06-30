package com.loomq.spi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * WAL 读取访问器。
 *
 * 内核通过此接口暴露 WAL 的读取能力，供服务层
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

    /**
     * 将写入位置回退到指定全局偏移，丢弃之后的所有数据。
     *
     * 日志截断使用：follower 发现日志冲突时，需要回退 writePosition
     * 以丢弃冲突的 entries，然后重新发送正确的 entries。
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
            "Use SimpleWalWriter or another writable implementation for log truncation.");
    }

    /**
     * WAL 段文件信息。
     */
    record WalSegment(int index, Path path, long startOffset, long endOffset, long size) {}
}
