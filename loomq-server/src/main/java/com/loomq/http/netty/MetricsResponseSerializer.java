package com.loomq.http.netty;

import com.loomq.metrics.LoomQMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * /metrics 响应的手写 JSON 序列化器。
 *
 * 该响应高频出现在探针和监控抓取中，因此避免 Jackson 和中间对象。
 */
public final class MetricsResponseSerializer {

    private static final byte[] OPEN_OBJECT = "{".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLOSE_OBJECT = "}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMA = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COLON = ":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] QUOTE = "\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NULL_LITERAL = "null".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_QUOTE = "\\\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_BACKSLASH = "\\\\".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_NEWLINE = "\\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_CARRIAGE_RETURN = "\\r".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_TAB = "\\t".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MIN_LONG_BYTES = "-9223372036854775808".getBytes(StandardCharsets.UTF_8);

    private static final byte[] FIELD_INTENTS_CREATED = "\"intentsCreated\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_INTENTS_COMPLETED = "\"intentsCompleted\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_INTENTS_CANCELLED = "\"intentsCancelled\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_INTENTS_EXPIRED = "\"intentsExpired\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_INTENTS_DEAD_LETTERED = "\"intentsDeadLettered\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_INTENTS_BY_STATUS = "\"intentsByStatus\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_DELIVERIES_TOTAL = "\"deliveriesTotal\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_DELIVERIES_SUCCESS = "\"deliveriesSuccess\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_DELIVERIES_FAILED = "\"deliveriesFailed\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_DELIVERIES_RETRIED = "\"deliveriesRetried\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_AVG_DELIVERY_LATENCY = "\"avgDeliveryLatencyMs\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_MAX_DELIVERY_LATENCY = "\"maxDeliveryLatencyMs\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_REPLICATION_RECORDS_SENT = "\"replicationRecordsSent\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_REPLICATION_RECORDS_ACKED = "\"replicationRecordsAcked\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_REPLICATION_LAG_MS = "\"replicationLagMs\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_WAL_RECORDS_WRITTEN = "\"walRecordsWritten\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_SNAPSHOTS_CREATED = "\"snapshotsCreated\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_PENDING_INTENTS = "\"pendingIntents\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_ACTIVE_DISPATCHES = "\"activeDispatches\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_WAL_HEALTHY = "\"walHealthy\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_WAL_LAST_FLUSH_TIME = "\"walLastFlushTime\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_WAL_IDLE_TIME_MS = "\"walIdleTimeMs\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_WAL_FLUSH_ERROR_COUNT = "\"walFlushErrorCount\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_AVG_WAL_FLUSH_LATENCY = "\"avgWalFlushLatencyMs\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_MAX_WAL_FLUSH_LATENCY = "\"maxWalFlushLatencyMs\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_WAL_PENDING_WRITES = "\"walPendingWrites\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_WAL_RING_BUFFER_SIZE = "\"walRingBufferSize\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_ROLE = "\"raftRole\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_LEADER_ID = "\"raftLeaderId\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_EPOCH = "\"raftEpoch\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_COMMIT_INDEX = "\"raftCommitIndex\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_LAST_APPLIED = "\"raftLastApplied\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_COMMIT_LAG = "\"raftCommitLag\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_REPLICATION_LAG = "\"raftReplicationLag\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_CONNECTED_PEERS = "\"raftConnectedPeers\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_TOTAL_PEERS = "\"raftTotalPeers\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_PENDING_WRITES = "\"raftPendingWrites\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_WRITE_PROPOSAL_LATENCY = "\"raftWriteProposalLatencyMs\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_WRITE_PROPOSAL_LATENCY_MAX = "\"raftWriteProposalLatencyMaxMs\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_WRITE_TIMEOUTS = "\"raftWriteTimeouts\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_WRITE_STEPDOWN_ABORTS = "\"raftWriteStepDownAborts\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_WRITE_BACKPRESSURE_REJECTS = "\"raftWriteBackpressureRejects\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_RAFT_WRITE_REVISION_CONFLICTS = "\"raftWriteRevisionConflicts\"".getBytes(StandardCharsets.UTF_8);

    private MetricsResponseSerializer() {}

    public static void write(LoomQMetrics.MetricsSnapshot snapshot, ByteBuf buf) {
        buf.writeBytes(OPEN_OBJECT);

        boolean first = true;
        first = writeLongField(buf, first, FIELD_INTENTS_CREATED, snapshot.intentsCreated());
        first = writeLongField(buf, first, FIELD_INTENTS_COMPLETED, snapshot.intentsCompleted());
        first = writeLongField(buf, first, FIELD_INTENTS_CANCELLED, snapshot.intentsCancelled());
        first = writeLongField(buf, first, FIELD_INTENTS_EXPIRED, snapshot.intentsExpired());
        first = writeLongField(buf, first, FIELD_INTENTS_DEAD_LETTERED, snapshot.intentsDeadLettered());
        first = writeStatusMap(buf, first, snapshot.intentsByStatus());
        first = writeLongField(buf, first, FIELD_DELIVERIES_TOTAL, snapshot.deliveriesTotal());
        first = writeLongField(buf, first, FIELD_DELIVERIES_SUCCESS, snapshot.deliveriesSuccess());
        first = writeLongField(buf, first, FIELD_DELIVERIES_FAILED, snapshot.deliveriesFailed());
        first = writeLongField(buf, first, FIELD_DELIVERIES_RETRIED, snapshot.deliveriesRetried());
        first = writeDoubleField(buf, first, FIELD_AVG_DELIVERY_LATENCY, snapshot.avgDeliveryLatencyMs());
        first = writeLongField(buf, first, FIELD_MAX_DELIVERY_LATENCY, snapshot.maxDeliveryLatencyMs());
        first = writeLongField(buf, first, FIELD_REPLICATION_RECORDS_SENT, snapshot.replicationRecordsSent());
        first = writeLongField(buf, first, FIELD_REPLICATION_RECORDS_ACKED, snapshot.replicationRecordsAcked());
        first = writeLongField(buf, first, FIELD_REPLICATION_LAG_MS, snapshot.replicationLagMs());
        first = writeLongField(buf, first, FIELD_WAL_RECORDS_WRITTEN, snapshot.walRecordsWritten());
        first = writeLongField(buf, first, FIELD_SNAPSHOTS_CREATED, snapshot.snapshotsCreated());
        first = writeLongField(buf, first, FIELD_PENDING_INTENTS, snapshot.pendingIntents());
        first = writeLongField(buf, first, FIELD_ACTIVE_DISPATCHES, snapshot.activeDispatches());
        first = writeBooleanField(buf, first, FIELD_WAL_HEALTHY, snapshot.walHealthy());
        first = writeLongField(buf, first, FIELD_WAL_LAST_FLUSH_TIME, snapshot.walLastFlushTime());
        first = writeLongField(buf, first, FIELD_WAL_IDLE_TIME_MS, snapshot.walIdleTimeMs());
        first = writeLongField(buf, first, FIELD_WAL_FLUSH_ERROR_COUNT, snapshot.walFlushErrorCount());
        first = writeDoubleField(buf, first, FIELD_AVG_WAL_FLUSH_LATENCY, snapshot.avgWalFlushLatencyMs());
        first = writeLongField(buf, first, FIELD_MAX_WAL_FLUSH_LATENCY, snapshot.maxWalFlushLatencyMs());
        first = writeLongField(buf, first, FIELD_WAL_PENDING_WRITES, snapshot.walPendingWrites());
        first = writeLongField(buf, first, FIELD_WAL_RING_BUFFER_SIZE, snapshot.walRingBufferSize());
        first = writeStringField(buf, first, FIELD_RAFT_ROLE, snapshot.raftRole());
        first = writeStringField(buf, first, FIELD_RAFT_LEADER_ID, snapshot.raftLeaderId());
        first = writeLongField(buf, first, FIELD_RAFT_EPOCH, snapshot.raftEpoch());
        first = writeLongField(buf, first, FIELD_RAFT_COMMIT_INDEX, snapshot.raftCommitIndex());
        first = writeLongField(buf, first, FIELD_RAFT_LAST_APPLIED, snapshot.raftLastApplied());
        first = writeLongField(buf, first, FIELD_RAFT_COMMIT_LAG, snapshot.raftCommitLag());
        first = writeLongField(buf, first, FIELD_RAFT_REPLICATION_LAG, snapshot.raftReplicationLag());
        first = writeLongField(buf, first, FIELD_RAFT_CONNECTED_PEERS, snapshot.raftConnectedPeers());
        first = writeLongField(buf, first, FIELD_RAFT_TOTAL_PEERS, snapshot.raftTotalPeers());
        first = writeLongField(buf, first, FIELD_RAFT_PENDING_WRITES, snapshot.raftPendingWrites());
        first = writeDoubleField(buf, first, FIELD_RAFT_WRITE_PROPOSAL_LATENCY, snapshot.raftWriteProposalLatencyMs());
        first = writeLongField(buf, first, FIELD_RAFT_WRITE_PROPOSAL_LATENCY_MAX, snapshot.raftWriteProposalLatencyMaxMs());
        first = writeLongField(buf, first, FIELD_RAFT_WRITE_TIMEOUTS, snapshot.raftWriteTimeouts());
        first = writeLongField(buf, first, FIELD_RAFT_WRITE_STEPDOWN_ABORTS, snapshot.raftWriteStepDownAborts());
        first = writeLongField(buf, first, FIELD_RAFT_WRITE_BACKPRESSURE_REJECTS, snapshot.raftWriteBackpressureRejects());
        writeLongField(buf, first, FIELD_RAFT_WRITE_REVISION_CONFLICTS, snapshot.raftWriteRevisionConflicts());

        buf.writeBytes(CLOSE_OBJECT);
    }

    public static int estimateSize(LoomQMetrics.MetricsSnapshot snapshot) {
        int size = 1024;
        size += snapshot.intentsByStatus() != null ? snapshot.intentsByStatus().size() * 32 : 0;
        return size;
    }

    public static byte[] toBytes(LoomQMetrics.MetricsSnapshot snapshot) {
        ByteBuf buf = io.netty.buffer.Unpooled.buffer(estimateSize(snapshot));
        try {
            write(snapshot, buf);
            byte[] result = new byte[buf.readableBytes()];
            buf.readBytes(result);
            return result;
        } finally {
            buf.release();
        }
    }

    private static boolean writeStatusMap(ByteBuf buf, boolean first, Map<String, Long> statuses) {
        writeFieldPrefix(buf, first, FIELD_INTENTS_BY_STATUS);
        if (statuses == null) {
            buf.writeBytes(NULL_LITERAL);
            return false;
        }

        buf.writeBytes(OPEN_OBJECT);
        boolean entryFirst = true;
        for (Map.Entry<String, Long> entry : statuses.entrySet()) {
            if (!entryFirst) {
                buf.writeBytes(COMMA);
            }
            entryFirst = false;
            writeQuotedString(buf, entry.getKey());
            buf.writeBytes(COLON);
            writeLong(buf, entry.getValue() != null ? entry.getValue() : 0L);
        }
        buf.writeBytes(CLOSE_OBJECT);
        return false;
    }

    private static boolean writeLongField(ByteBuf buf, boolean first, byte[] fieldName, long value) {
        writeFieldPrefix(buf, first, fieldName);
        writeLong(buf, value);
        return false;
    }

    private static boolean writeDoubleField(ByteBuf buf, boolean first, byte[] fieldName, double value) {
        writeFieldPrefix(buf, first, fieldName);
        ByteBufUtil.writeUtf8(buf, Double.toString(value));
        return false;
    }

    private static boolean writeBooleanField(ByteBuf buf, boolean first, byte[] fieldName, boolean value) {
        writeFieldPrefix(buf, first, fieldName);
        ByteBufUtil.writeUtf8(buf, Boolean.toString(value));
        return false;
    }

    private static boolean writeStringField(ByteBuf buf, boolean first, byte[] fieldName, String value) {
        writeFieldPrefix(buf, first, fieldName);
        writeQuotedString(buf, value);
        return false;
    }

    private static void writeFieldPrefix(ByteBuf buf, boolean first, byte[] fieldName) {
        if (!first) {
            buf.writeBytes(COMMA);
        }
        buf.writeBytes(fieldName);
        buf.writeBytes(COLON);
    }

    private static void writeQuotedString(ByteBuf buf, String value) {
        if (value == null) {
            buf.writeBytes(NULL_LITERAL);
            return;
        }
        buf.writeBytes(QUOTE);
        writeEscapedString(value, buf);
        buf.writeBytes(QUOTE);
    }

    private static void writeEscapedString(String value, ByteBuf buf) {
        int len = value.length();
        int escapePos = -1;
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\' || c < ' ') {
                escapePos = i;
                break;
            }
        }

        if (escapePos < 0) {
            ByteBufUtil.writeUtf8(buf, value);
            return;
        }

        if (escapePos > 0) {
            buf.writeCharSequence(value.subSequence(0, escapePos), StandardCharsets.UTF_8);
        }

        for (int i = escapePos; i < len; i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> buf.writeBytes(ESCAPED_QUOTE);
                case '\\' -> buf.writeBytes(ESCAPED_BACKSLASH);
                case '\n' -> buf.writeBytes(ESCAPED_NEWLINE);
                case '\r' -> buf.writeBytes(ESCAPED_CARRIAGE_RETURN);
                case '\t' -> buf.writeBytes(ESCAPED_TAB);
                default -> {
                    if (c < ' ') {
                        buf.writeByte('\\');
                        buf.writeByte('u');
                        writeHex4(c, buf);
                    } else {
                        buf.writeByte((byte) c);
                    }
                }
            }
        }
    }

    private static void writeHex4(char value, ByteBuf buf) {
        int n = value;
        buf.writeByte(toHex((n >>> 12) & 0xF));
        buf.writeByte(toHex((n >>> 8) & 0xF));
        buf.writeByte(toHex((n >>> 4) & 0xF));
        buf.writeByte(toHex(n & 0xF));
    }

    private static byte toHex(int value) {
        return (byte) (value < 10 ? ('0' + value) : ('a' + (value - 10)));
    }

    private static void writeLong(ByteBuf buf, long value) {
        if (value == 0L) {
            buf.writeByte('0');
            return;
        }
        if (value == Long.MIN_VALUE) {
            buf.writeBytes(MIN_LONG_BYTES);
            return;
        }

        boolean negative = value < 0;
        long v = negative ? -value : value;
        int digits = digitCount(v) + (negative ? 1 : 0);
        int writerIndex = buf.writerIndex();
        buf.ensureWritable(digits);

        int pos = writerIndex + digits - 1;
        do {
            long q = v / 10;
            int r = (int) (v - (q * 10));
            buf.setByte(pos--, '0' + r);
            v = q;
        } while (v != 0L);

        if (negative) {
            buf.setByte(pos, '-');
        }

        buf.writerIndex(writerIndex + digits);
    }

    private static int digitCount(long value) {
        int digits = 1;
        while ((value /= 10L) != 0L) {
            digits++;
        }
        return digits;
    }
}
