package com.loomq.raft;

import com.loomq.spi.WalAccessor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft 日志封装。
 * 复用 SimpleWalWriter 作为持久化存储，Raft index 使用逻辑序号（1,2,3...），
 * WAL 负责保存对应的物理偏移。
 *
 * 每条 Raft log entry 的 WAL 格式：
 * [epoch (8B，大端) | payload (N)]，整体作为 WAL payload 写入。
 * WAL 帧头 + CRC 由 SimpleWalWriter 自动封装。
 */
public class RaftLog {
    private static final Logger log = LoggerFactory.getLogger(RaftLog.class);
    private static final int TERM_PREFIX_SIZE = 8;

    private final WalAccessor wal;
    private final Map<Long, Long> startPositions = new HashMap<>();
    private final Map<Long, Integer> recordLengths = new HashMap<>();
    private final Map<Long, Long> entryEpochs = new HashMap<>();
    private volatile long lastLogIndex;
    private volatile long lastLogEpoch;

    public RaftLog(WalAccessor wal) {
        this.wal = wal;
        long snapshotIndex = wal != null ? wal.getSnapshotIndex() : 0;
        this.lastLogIndex = snapshotIndex;
        this.lastLogEpoch = wal != null ? wal.getSnapshotEpoch() : 0;
        recoverFromWal();
    }

    public long lastIndex() { return lastLogIndex; }
    public long firstIndex() {
        return wal != null ? wal.getFirstLogIndex() : (lastLogIndex > 0 ? 1 : 0);
    }
    public long lastEpoch() { return lastLogEpoch; }
    public long commitIndex() { return lastLogIndex; }

    public boolean isUpToDate(long candidateLastIndex, long candidateLastEpoch) {
        long myLastEpoch = lastEpoch();
        return candidateLastEpoch > myLastEpoch
            || (candidateLastEpoch == myLastEpoch && candidateLastIndex >= lastIndex());
    }

    /**
     * 追加一条 Raft log entry。
     *
     * @param epoch 当前 epoch
     * @param data 业务 payload
     * @return Raft 逻辑 index（从 1 开始，包含快照后的后续条目）
     */
    public synchronized long appendEntry(long epoch, byte[] data) {
        byte[] framed = new byte[TERM_PREFIX_SIZE + data.length];
        ByteBuffer.wrap(framed).putLong(epoch).put(data);
        long walEnd = wal.writeEntry(framed);
        long index = ++lastLogIndex;
        // Compute start position for future readRecord() calls
        // RECORD_FIXED_OVERHEAD is 8 (4B length + 4B CRC); this is the WAL contract, not a leak
        int recordLen = wal.getRecordOverhead() + framed.length;
        long startPos = walEnd - recordLen;
        startPositions.put(index, startPos);
        recordLengths.put(index, recordLen);
        entryEpochs.put(index, epoch);
        lastLogEpoch = epoch;
        log.debug("Appended Raft entry at index={}, epoch={}, size={}", index, epoch, data.length);
        return index;
    }

    private void recoverFromWal() {
        if (wal == null) {
            return;
        }

        long snapshotIndex = wal.getSnapshotIndex();
        long snapshotEpoch = wal.getSnapshotEpoch();
        long snapshotOffset = wal.getSnapshotOffset();
        List<WalAccessor.WalSegment> segments = wal.listSegments();
        if (segments == null || segments.isEmpty()) {
            lastLogIndex = snapshotIndex;
            lastLogEpoch = snapshotEpoch;
            return;
        }

        long recoveredLastEpoch = snapshotEpoch;
        long recoveredLastIndex = snapshotIndex;
        long physicalCount = 0;
        boolean useSnapshotOffset = snapshotOffset > 0;
        for (WalAccessor.WalSegment seg : segments) {
            long localPos = 0;
            try (FileChannel channel = FileChannel.open(seg.path(), StandardOpenOption.READ)) {
                while (localPos + wal.getRecordOverhead() <= seg.size()) {
                    ByteBuffer headerBuf = ByteBuffer.allocate(Integer.BYTES)
                        .order(ByteOrder.BIG_ENDIAN);
                    int headerBytes = readFully(channel, localPos, headerBuf);
                    if (headerBytes != Integer.BYTES) {
                        break;
                    }
                    headerBuf.flip();
                    int payloadLen = headerBuf.getInt();
                    if (payloadLen <= 0) {
                        break;
                    }

                    long recordLen = wal.getRecordOverhead() + payloadLen;
                    if (localPos + recordLen > seg.size()) {
                        break;
                    }

                    ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
                    int payloadBytes = readFully(channel, localPos + Integer.BYTES, payloadBuf);
                    if (payloadBytes != payloadLen) {
                        break;
                    }
                    payloadBuf.flip();
                    byte[] payload = new byte[payloadLen];
                    payloadBuf.get(payload);

                    ByteBuffer crcBuf = ByteBuffer.allocate(Integer.BYTES)
                        .order(ByteOrder.BIG_ENDIAN);
                    int crcBytes = readFully(channel, localPos + Integer.BYTES + payloadLen, crcBuf);
                    if (crcBytes != Integer.BYTES) {
                        break;
                    }
                    crcBuf.flip();
                    int storedCrc = crcBuf.getInt();
                    int computedCrc = calculateCrc(payload);
                    if (storedCrc != computedCrc) {
                        break;
                    }

                    long recordStart = seg.startOffset() + localPos;
                    long recordEnd = recordStart + recordLen;
                    if (useSnapshotOffset) {
                        if (recordEnd <= snapshotOffset) {
                            localPos += recordLen;
                            continue;
                        }
                        if (recordStart < snapshotOffset) {
                            log.warn("Skipping WAL record that straddles snapshot boundary: boundary={}, start={}, end={}",
                                snapshotOffset, recordStart, recordEnd);
                            localPos += recordLen;
                            continue;
                        }
                    } else if (snapshotIndex > 0) {
                        physicalCount++;
                        if (physicalCount <= snapshotIndex) {
                            localPos += recordLen;
                            continue;
                        }
                    }

                    recoveredLastIndex++;
                    startPositions.put(recoveredLastIndex, recordStart);
                    recordLengths.put(recoveredLastIndex, (int) recordLen);
                    if (payload.length >= TERM_PREFIX_SIZE) {
                        recoveredLastEpoch = ByteBuffer.wrap(payload, 0, TERM_PREFIX_SIZE).getLong();
                        entryEpochs.put(recoveredLastIndex, recoveredLastEpoch);
                    } else {
                        entryEpochs.put(recoveredLastIndex, 0L);
                    }
                    localPos += recordLen;
                }
            } catch (IOException e) {
                log.warn("Failed to recover Raft log segment {}", seg.path(), e);
                break;
            }
        }

        lastLogIndex = recoveredLastIndex;
        lastLogEpoch = recoveredLastEpoch;
    }

    private long resolveSnapshotOffset(long snapshotIndex) {
        Long direct = startPositions.get(snapshotIndex + 1);
        if (direct != null) {
            return direct;
        }

        long nextIndex = Long.MAX_VALUE;
        long nextStart = -1;
        for (Map.Entry<Long, Long> entry : startPositions.entrySet()) {
            long idx = entry.getKey();
            if (idx > snapshotIndex && idx < nextIndex) {
                nextIndex = idx;
                nextStart = entry.getValue();
            }
        }
        if (nextStart >= 0) {
            return nextStart;
        }

        return wal.getWritePosition();
    }

    /**
     * 读取指定 index 处 entry 的 epoch。
     *
     * @param index log index
     * @return epoch，读取失败返回 -1
     */
    public synchronized long readEntryEpoch(long index) {
        Long cachedEpoch = entryEpochs.get(index);
        if (cachedEpoch != null) {
            return cachedEpoch;
        }
        Long startPos = startPositions.get(index);
        Integer recordLen = recordLengths.get(index);
        if (startPos == null || recordLen == null) {
            if (wal != null && wal.getSnapshotIndex() > 0 && index == wal.getSnapshotIndex()) {
                return wal.getSnapshotEpoch();
            }
            return -1;
        }
        try {
            byte[] framed = wal.readRecord(startPos, recordLen);
            return ByteBuffer.wrap(framed).getLong();
        } catch (IOException e) {
            log.error("Failed to read entry epoch at index={}", index, e);
            return -1;
        }
    }

    /**
     * 读取指定 index 处 entry 的完整 framed 数据（含 8 字节 epoch 前缀）。
     *
     * 用于 leader 复制日志条目到 follower 时保留原始 epoch 信息。
     *
     * @param index log index
     * @return 完整 framed 数据（epoch prefix + payload），读取失败返回 null
     */
    public synchronized byte[] readEntryRaw(long index) {
        Long startPos = startPositions.get(index);
        Integer recordLen = recordLengths.get(index);
        if (startPos == null || recordLen == null) return null;
        try {
            return wal.readRecord(startPos, recordLen);
        } catch (IOException e) {
            log.error("Failed to read raw Raft entry at index={}", index, e);
            return null;
        }
    }

    /**
     * 读取指定 index 处 entry 的 payload（不含 epoch 前缀）。
     *
     * @param index log index
     * @return 业务 payload，读取失败返回 null
     */
    public synchronized byte[] readEntry(long index) {
        Long startPos = startPositions.get(index);
        Integer recordLen = recordLengths.get(index);
        if (startPos == null || recordLen == null) return null;
        try {
            byte[] framed = wal.readRecord(startPos, recordLen);
            if (framed.length <= TERM_PREFIX_SIZE) return new byte[0];
            byte[] payload = new byte[framed.length - TERM_PREFIX_SIZE];
            System.arraycopy(framed, TERM_PREFIX_SIZE, payload, 0, payload.length);
            return payload;
        } catch (IOException e) {
            log.error("Failed to read Raft entry at index={}", index, e);
            return null;
        }
    }

    /** @deprecated Use {@link #readEntry(long)} or {@link #readEntryRaw(long)} instead. */
    @Deprecated
    public synchronized byte[] getEntry(long index, int length) {
        Long startPos = startPositions.get(index);
        Integer recordLen = recordLengths.get(index);
        if (startPos == null || recordLen == null) return null;
        try { return wal.readRecord(startPos, recordLen); }
        catch (Exception e) { log.error("Failed to read Raft entry at {}", index, e); return null; }
    }

    /**
     * Truncate all log entries after {@code index} (§5.3 conflict resolution).
     *
     * Removes map entries for all indices &gt; {@code index} and rewinds the WAL
     * write position to discard orphaned data. After truncation, the next
     * {@link #appendEntry} will produce contiguous indices starting from
     * the truncation point.
     *
     * Idempotent: if {@code index >= lastIndex()}, this is a no-op.
     *
     * @param index the last log index to preserve (WAL end position)
     */
    public synchronized void truncateAfter(long index) {
        if (index >= lastIndex()) {
            return; // nothing to truncate
        }

        long lastValidEpoch = 0;
        long snapshotIndex = wal != null ? wal.getSnapshotIndex() : 0;
        if (index < snapshotIndex) {
            log.warn("Ignoring truncateAfter({}) before snapshot index {}", index, snapshotIndex);
            return;
        }

        Long startPos = startPositions.get(index);
        Integer recLen = recordLengths.get(index);
        long resetPos = 0;
        if (startPos != null && recLen != null) {
            // Read the epoch of the entry being kept as the anchor
            try {
                byte[] framed = wal.readRecord(startPos, recLen);
                lastValidEpoch = java.nio.ByteBuffer.wrap(framed).getLong();
                resetPos = startPos + recLen;
            } catch (IOException e) {
                log.warn("Could not read epoch at index {} during truncation", index, e);
            }
        } else if (index == snapshotIndex) {
            lastValidEpoch = wal.getSnapshotEpoch();
            Long nextStart = startPositions.get(index + 1);
            if (nextStart != null) {
                resetPos = nextStart;
            } else {
                resetPos = wal.getWritePosition();
            }
        } else {
            Long nextStart = startPositions.get(index + 1);
            if (nextStart != null) {
                resetPos = nextStart;
            } else {
                log.warn("Could not resolve reset position for truncateAfter({})", index);
                return;
            }
        }

        // Remove all map entries with key > index
        startPositions.keySet().removeIf(k -> k > index);
        recordLengths.keySet().removeIf(k -> k > index);
        entryEpochs.keySet().removeIf(k -> k > index);

        // Rewind WAL to discard orphaned data past this index
        // (resetPos is the end position of the last valid record = start of next write)
        wal.resetTo(resetPos);

        lastLogIndex = index;
        lastLogEpoch = lastValidEpoch;
        log.debug("Truncated log after index={}, new lastEpoch={}", index, lastValidEpoch);
    }

    /**
     * Compact the log through {@code snapshotIndex}.
     *
     * The logical snapshot metadata is persisted together with the physical WAL
     * boundary so recovery can skip the compacted prefix without re-scanning the
     * deleted records.
     *
     * @param snapshotIndex last included index in the snapshot
     * @return physical snapshot boundary offset
     */
    public synchronized long compactThrough(long snapshotIndex) {
        long snapshotEpoch = snapshotIndex > 0 ? readEntryEpoch(snapshotIndex) : 0;
        return compactThrough(snapshotIndex, snapshotEpoch);
    }

    /**
     * Compact the log through {@code snapshotIndex} using an explicit epoch.
     *
     * @param snapshotIndex last included index in the snapshot
     * @param snapshotEpoch  epoch of the last included entry
     * @return physical snapshot boundary offset
     */
    public synchronized long compactThrough(long snapshotIndex, long snapshotEpoch) {
        if (wal == null) {
            return 0;
        }
        if (snapshotIndex <= 0) {
            return wal.getSnapshotOffset();
        }
        if (snapshotIndex < wal.getSnapshotIndex()) {
            log.debug("Ignoring compaction behind current snapshot boundary: requested={}, current={}",
                snapshotIndex, wal.getSnapshotIndex());
            return wal.getSnapshotOffset();
        }
        if (snapshotEpoch < 0) {
            throw new IllegalStateException("Cannot compact snapshot index " + snapshotIndex + " without a valid epoch");
        }

        long snapshotOffset = resolveSnapshotOffset(snapshotIndex);

        startPositions.keySet().removeIf(k -> k <= snapshotIndex);
        recordLengths.keySet().removeIf(k -> k <= snapshotIndex);
        entryEpochs.keySet().removeIf(k -> k <= snapshotIndex);

        wal.setSnapshotMetadata(snapshotIndex, snapshotEpoch, snapshotOffset);
        wal.truncateBefore(snapshotOffset);

        if (snapshotIndex >= lastLogIndex) {
            lastLogIndex = snapshotIndex;
            lastLogEpoch = snapshotEpoch;
        }

        log.info("Compacted Raft log through index={} (epoch={}, offset={})",
            snapshotIndex, snapshotEpoch, snapshotOffset);
        return snapshotOffset;
    }

    private int readFully(FileChannel channel, long position, ByteBuffer buffer) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + total);
            if (read < 0) {
                return total;
            }
            total += read;
        }
        return total;
    }

    private int calculateCrc(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data, 0, data.length);
        return (int) crc.getValue();
    }
}
