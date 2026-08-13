package com.loomq.infrastructure.wheel;

import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.domain.intent.WalMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 定长槽编解码。格式(256B):
 * status(1) | revision(8) | CRC(4) | executeAt(8) | intentIdLen(1) | intentId(≤24) | payload(≤210)
 *
 * status=0 表示空槽。CRC 覆盖 status+revision([0..8]) 和 executeAt..end([13..255]),
 * 跳过 CRC 自身([9..12]),撕裂写可检出。
 */
public final class SlotCodec {
    public static final int SLOT_SIZE = 256;
    private static final int OFF_STATUS = 0;
    private static final int OFF_REVISION = 1;
    private static final int OFF_CRC = 9;
    private static final int OFF_EXECUTE_AT = 13;
    private static final int OFF_ID_LEN = 21;
    private static final int OFF_ID = 22;
    public static final int MAX_ID_LEN = Intent.MAX_ID_BYTES;
    private static final int OFF_PAYLOAD = OFF_ID + MAX_ID_LEN; // 46
    private static final int PAYLOAD_SIZE = SLOT_SIZE - OFF_PAYLOAD; // 210

    private SlotCodec() {}

    public static byte[] encode(Intent intent) {
        byte[] slot = new byte[SLOT_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(slot).order(ByteOrder.BIG_ENDIAN);

        byte[] idBytes = intent.getIntentId().getBytes(StandardCharsets.UTF_8);
        if (idBytes.length > MAX_ID_LEN) {
            throw new SlotOverflowException("intentId too long: " + idBytes.length);
        }

        // payload 编码到临时缓冲,再拷入
        byte[] payload = encodePayload(intent);
        if (payload.length > PAYLOAD_SIZE) {
            throw new SlotOverflowException("payload " + payload.length + " > " + PAYLOAD_SIZE);
        }

        buf.put(OFF_STATUS, (byte) (intent.getStatus() == null ? 0 : intent.getStatus().ordinal() + 1));
        buf.putLong(OFF_REVISION, intent.getRevision());
        // ADAPTATION: brief 用 toEpochMilli() 存储会丢失亚毫秒精度,而 shouldRoundTripTypicalIntent
        // 断言 executeAt 精确相等(本 JVM Instant.now() 为微秒精度)。在固定 8B 内以
        // (epochSecond<<30)|nano 打包,保留完整纳秒精度;0 表示 executeAt 为 null。
        buf.putLong(OFF_EXECUTE_AT, intent.getExecuteAt() == null ? 0L : packExecuteAt(intent.getExecuteAt()));
        buf.put(OFF_ID_LEN, (byte) idBytes.length);
        System.arraycopy(idBytes, 0, slot, OFF_ID, idBytes.length);
        System.arraycopy(payload, 0, slot, OFF_PAYLOAD, payload.length);

        // CRC over [0..8] (status+revision) + [13..end] (executeAt..end), skipping CRC field [9..12]
        int crc = computeCrc(slot);
        buf.putInt(OFF_CRC, crc);
        return slot;
    }

    public static Intent decode(byte[] slot) {
        if (slot == null || slot.length != SLOT_SIZE) {
            throw new IllegalArgumentException("slot must be 256 bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(slot).order(ByteOrder.BIG_ENDIAN);
        byte statusOrd = buf.get(OFF_STATUS);
        if (statusOrd == 0) return null; // empty

        int storedCrc = buf.getInt(OFF_CRC);
        int actualCrc = computeCrc(slot);
        if (storedCrc != actualCrc) {
            throw new IllegalStateException("CRC mismatch: torn slot");
        }

        long revision = buf.getLong(OFF_REVISION);
        long executeAtPacked = buf.getLong(OFF_EXECUTE_AT);
        int idLen = buf.get(OFF_ID_LEN) & 0xFF;
        byte[] idBytes = new byte[idLen];
        System.arraycopy(slot, OFF_ID, idBytes, 0, idLen);
        String intentId = new String(idBytes, StandardCharsets.UTF_8);

        byte[] payload = new byte[PAYLOAD_SIZE];
        System.arraycopy(slot, OFF_PAYLOAD, payload, 0, PAYLOAD_SIZE);
        return decodePayload(payload, intentId, statusOrd, revision, executeAtPacked);
    }

    public static boolean isOccupied(byte[] slot) {
        return slot != null && slot.length == SLOT_SIZE && slot[OFF_STATUS] != 0;
    }

    public static boolean isTorn(byte[] slot) {
        if (!isOccupied(slot)) return false;
        ByteBuffer buf = ByteBuffer.wrap(slot).order(ByteOrder.BIG_ENDIAN);
        int stored = buf.getInt(OFF_CRC);
        int actual = computeCrc(slot);
        return stored != actual;
    }

    // ===== payload 编码(变长 TLV,复用 IntentBinaryCodec 思路,但限定 210B) =====
    private static byte[] encodePayload(Intent intent) {
        // ADAPTATION: 按 intent 实际字段预计算容量,而非固定 PAYLOAD_SIZE。
        // 否则超长 payload 在写入阶段就抛 BufferOverflowException(而非 SlotOverflowException),
        // 使 encode() 中的长度校验失效。预计算后,超长会以"返回长数组"形式被 encode() 兜底拒绝。
        ByteBuffer b = ByteBuffer.allocate(payloadCapacity(intent)).order(ByteOrder.BIG_ENDIAN);
        putStr(b, (byte) 0x01, intent.getTraceId());
        putLong(b, (byte) 0x02, intent.getCreatedAt() == null ? 0 : intent.getCreatedAt().toEpochMilli());
        putLong(b, (byte) 0x03, intent.getUpdatedAt() == null ? 0 : intent.getUpdatedAt().toEpochMilli());
        putLong(b, (byte) 0x04, intent.getDeadline() == null ? 0 : intent.getDeadline().toEpochMilli());
        if (intent.getExpiredAction() != null) putByte(b, (byte) 0x05, (byte) intent.getExpiredAction().ordinal());
        if (intent.getPrecisionTier() != null) putByte(b, (byte) 0x06, (byte) intent.getPrecisionTier().ordinal());
        if (intent.getWalMode() != null) putByte(b, (byte) 0x07, (byte) intent.getWalMode().ordinal());
        if (intent.getShardKey() != null) putStr(b, (byte) 0x08, intent.getShardKey());
        if (intent.getShardId() != null) putStr(b, (byte) 0x09, intent.getShardId());
        if (intent.getAttempts() > 0) putInt(b, (byte) 0x0B, intent.getAttempts());
        if (intent.getLastDeliveryId() != null) putStr(b, (byte) 0x0C, intent.getLastDeliveryId());
        if (intent.getIdempotencyKey() != null) putStr(b, (byte) 0x0D, intent.getIdempotencyKey());
        // ADAPTATION: brief 原始 encodePayload 未编码 tags,但 shouldRejectOversizedPayload
        // 依赖 tags 作为业务负载触发溢出。补齐 tags 的 TLV 编解码(0x0E)。
        putTags(b, (byte) 0x0E, intent.getTags());
        // R20: redelivery 策略必须随槽持久化——重试语义是 Intent 的耐久契约,崩溃恢复后
        // 丢失会静默回退默认(5000ms/5 次),maxAttempts=2 变 5 次(超契约投递)或
        // maxAttempts=100 提前死信。仅非 null 时写入(默认 Intent 无策略,零额外体积)。
        RedeliveryPolicy rp = intent.getRedelivery();
        if (rp != null) {
            putInt(b, (byte) 0x0F, rp.getMaxAttempts());
            putStr(b, (byte) 0x10, rp.getBackoff());
            putLong(b, (byte) 0x11, rp.getInitialDelayMs());
            putLong(b, (byte) 0x12, rp.getMaxDelayMs());
            putDouble(b, (byte) 0x13, rp.getMultiplier());
            putByte(b, (byte) 0x14, (byte) (rp.isJitter() ? 1 : 0));
        }
        byte[] result = new byte[b.position()];
        System.arraycopy(b.array(), 0, result, 0, result.length);
        return result;
    }

    private static Intent decodePayload(byte[] payload, String intentId, byte statusOrd, long revision, long executeAtPacked) {
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String traceId = null; long createdAt = 0, updatedAt = 0, deadline = 0;
        ExpiredAction expiredAction = null; PrecisionTier tier = null; WalMode walMode = null;
        String shardKey = null, shardId = null;
        int attempts = 0; String lastDeliveryId = null, idempotencyKey = null;
        Map<String, String> tags = null; // ADAPTATION: 支持 tags 回读
        boolean hasRedelivery = false;
        int maxAttempts = 0; String backoff = null;
        long initialDelayMs = 0, maxDelayMs = 0; double multiplier = 0; boolean jitter = false;

        while (b.hasRemaining()) {
            byte type = b.get();
            // ADAPTATION: 0x00 不是合法 TLV 类型,代表 payload 区尾部的零填充/结束。
            // brief 原始循环在非 5 整数倍的零填充上会因 getInt() 下溢而崩溃。
            if (type == 0) break;
            int len = b.getInt();
            if (len < 0 || len > b.remaining()) break;
            int start = b.position();
            switch (type) {
                case 0x01 -> traceId = getStr(b, len);
                case 0x02 -> createdAt = b.getLong();
                case 0x03 -> updatedAt = b.getLong();
                case 0x04 -> deadline = b.getLong();
                case 0x05 -> expiredAction = ExpiredAction.values()[b.get()];
                case 0x06 -> tier = PrecisionTierCatalog.defaultCatalog().tierByOrdinal(b.get() & 0xFF);
                case 0x07 -> walMode = decodeWalMode(b.get() & 0xFF);
                case 0x08 -> shardKey = getStr(b, len);
                case 0x09 -> shardId = getStr(b, len);
                case 0x0B -> attempts = b.getInt();
                case 0x0C -> lastDeliveryId = getStr(b, len);
                case 0x0D -> idempotencyKey = getStr(b, len);
                case 0x0E -> tags = getTags(b, len); // ADAPTATION: tags 回读
                case 0x0F -> { maxAttempts = b.getInt(); hasRedelivery = true; }
                case 0x10 -> backoff = getStr(b, len);
                case 0x11 -> initialDelayMs = b.getLong();
                case 0x12 -> maxDelayMs = b.getLong();
                case 0x13 -> multiplier = b.getDouble();
                case 0x14 -> jitter = b.get() != 0;
                default -> b.position(start + len);
            }
            if (b.position() != start + len) b.position(start + len); // safety
        }

        IntentStatus status = IntentStatus.values()[statusOrd - 1];
        return Intent.restore(traceId, intentId, status,
            Instant.ofEpochMilli(createdAt), Instant.ofEpochMilli(updatedAt),
            unpackExecuteAt(executeAtPacked),
            deadline == 0 ? null : Instant.ofEpochMilli(deadline),
            expiredAction, tier, walMode, shardKey, shardId,
            null,
            hasRedelivery ? new RedeliveryPolicy(maxAttempts, backoff, initialDelayMs, maxDelayMs,
                multiplier, jitter) : null,
            idempotencyKey, tags, attempts, lastDeliveryId, revision);
    }

    /**
     * v0.9.x 精简:WalMode 仅剩 ASYNC(0)/DURABLE(1)。越界 ordinal(如断裂前的旧 DURABLE=2)
     * 返回 null,由调用方回退 tier 默认档;与 PrecisionTierCatalog.tierByOrdinal 的守卫同旨
     * (都避免 ArrayIndexOutOfBoundsException),但返回 null 而非 defaultTier——WalMode 无
     * "默认值"概念,回退交给 resolveWalMode。注意:断裂前的旧 BATCH_DEFERRED ordinal=1 在本
     * 枚举中落在界内,会读成 DURABLE,属清库契约内的一次性碰撞,不返回 null。
     */
    static WalMode decodeWalMode(int ordinal) {
        return ordinal >= 0 && ordinal < WalMode.values().length
            ? WalMode.values()[ordinal]
            : null;
    }

    private static void putStr(ByteBuffer b, byte type, String v) {
        if (v == null) return;
        byte[] bs = v.getBytes(StandardCharsets.UTF_8);
        b.put(type); b.putInt(2 + bs.length); b.putShort((short) bs.length); b.put(bs);
    }
    private static void putLong(ByteBuffer b, byte type, long v) { b.put(type); b.putInt(8); b.putLong(v); }
    private static void putInt(ByteBuffer b, byte type, int v) { b.put(type); b.putInt(4); b.putInt(v); }
    private static void putByte(ByteBuffer b, byte type, byte v) { b.put(type); b.putInt(1); b.put(v); }
    private static void putDouble(ByteBuffer b, byte type, double v) { b.put(type); b.putInt(8); b.putDouble(v); }
    private static String getStr(ByteBuffer b, int len) {
        short sl = b.getShort();
        byte[] bs = new byte[sl]; b.get(bs);
        if (2 + sl < len) b.position(b.position() + (len - 2 - sl));
        return new String(bs, StandardCharsets.UTF_8);
    }

    // ADAPTATION: tags 的 TLV 编解码,与 IntentBinaryCodec 的 Map 格式一致(count + [klen+k + vlen+v])。
    private static void putTags(ByteBuffer b, byte type, Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) return;
        int save = b.position();
        b.put(type);
        b.putInt(0); // 占位,稍后回填 value 长度
        int start = b.position();
        b.putInt(tags.size());
        for (Map.Entry<String, String> e : tags.entrySet()) {
            byte[] k = e.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = e.getValue().getBytes(StandardCharsets.UTF_8);
            b.putShort((short) k.length); b.put(k);
            b.putShort((short) v.length); b.put(v);
        }
        b.putInt(save + 1, b.position() - start); // 回填 value 长度
    }
    private static Map<String, String> getTags(ByteBuffer b, int len) {
        int count = b.getInt();
        Map<String, String> m = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            short kl = b.getShort(); byte[] kb = new byte[kl & 0xFFFF]; b.get(kb);
            short vl = b.getShort(); byte[] vb = new byte[vl & 0xFFFF]; b.get(vb);
            m.put(new String(kb, StandardCharsets.UTF_8), new String(vb, StandardCharsets.UTF_8));
        }
        return m;
    }

    // ADAPTATION: 预计算 payload 编码所需字节数,使 encodePayload 在超长时返回长数组而非抛
    // BufferOverflowException。需与 encodePayload 的写入字段保持同步。
    private static int payloadCapacity(Intent intent) {
        int n = 0;
        if (intent.getTraceId() != null) n += 7 + utf8Len(intent.getTraceId());
        n += 13; // createdAt
        n += 13; // updatedAt
        n += 13; // deadline
        if (intent.getExpiredAction() != null) n += 6;
        if (intent.getPrecisionTier() != null) n += 6;
        if (intent.getWalMode() != null) n += 6;
        if (intent.getShardKey() != null) n += 7 + utf8Len(intent.getShardKey());
        if (intent.getShardId() != null) n += 7 + utf8Len(intent.getShardId());
        if (intent.getAttempts() > 0) n += 9;
        if (intent.getLastDeliveryId() != null) n += 7 + utf8Len(intent.getLastDeliveryId());
        if (intent.getIdempotencyKey() != null) n += 7 + utf8Len(intent.getIdempotencyKey());
        Map<String, String> tags = intent.getTags();
        if (tags != null && !tags.isEmpty()) {
            int t = 4; // count
            for (Map.Entry<String, String> e : tags.entrySet()) {
                t += 2 + utf8Len(e.getKey()) + 2 + utf8Len(e.getValue());
            }
            n += 5 + t; // type(1) + len(4) + value
        }
        RedeliveryPolicy rp = intent.getRedelivery();
        if (rp != null) {
            n += 9;  // maxAttempts int
            n += rp.getBackoff() == null ? 0 : 7 + utf8Len(rp.getBackoff());
            n += 13; // initialDelayMs
            n += 13; // maxDelayMs
            n += 13; // multiplier double
            n += 6;  // jitter byte
        }
        return n;
    }
    private static int utf8Len(String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    // ADAPTATION: executeAt 8B 打包/解包,保留完整纳秒精度。
    // nano < 2^30(1073741824 > 999_999_999),占低 30 位;epochSecond 占高位。
    // 0 表示 null。epochSecond<<30 在约 2242 年前不会 long 溢出,对本系统足够。
    private static long packExecuteAt(Instant t) {
        return (t.getEpochSecond() << 30) | (t.getNano() & 0x3FFFFFFFL);
    }
    private static Instant unpackExecuteAt(long packed) {
        if (packed == 0L) return null;
        return Instant.ofEpochSecond(packed >>> 30, packed & 0x3FFFFFFFL);
    }

    private static final ThreadLocal<CRC32> CRC = ThreadLocal.withInitial(CRC32::new);
    private static int crc(byte[] data, int off, int len) {
        CRC32 c = CRC.get(); c.reset();
        c.update(data, off, len);
        return (int) c.getValue();
    }

    /**
     * 计算槽 CRC,覆盖 status+revision([0..8]) 和 executeAt..end([13..255]),
     * 跳过 CRC 自身([9..12])。两段累加,CRC32.update 可多次调用。
     */
    private static int computeCrc(byte[] slot) {
        CRC32 c = CRC.get(); c.reset();
        c.update(slot, 0, OFF_CRC);                              // [0..8]: status + revision
        c.update(slot, OFF_EXECUTE_AT, SLOT_SIZE - OFF_EXECUTE_AT); // [13..255]: executeAt..end
        return (int) c.getValue();
    }
}
