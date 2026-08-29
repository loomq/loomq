package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;

/**
 * Round 11 弃用 TLV 0x08(shardKey)/0x09(shardId) 后的旧格式兼容守卫：
 * 含弃用 tag 的历史槽必须被 default-skip 分支安全解码（字段消失而非解析失败）。
 * 直接手工拼装槽字节（不复用编码器）——若未来槽布局变更导致本测试失败，
 * 属预期的响亮失败（格式契约变化必须显式评估兼容性）。
 */
class SlotCodecLegacyTlvCompatTest {

    /** 槽头偏移（SlotCodec 私有常量在此固化一份，注释注明来源）。 */
    private static final int OFF_CRC = 9;
    private static final int OFF_EXECUTE_AT = 13;
    private static final int OFF_PAYLOAD = 46;
    private static final int PAYLOAD_SIZE = 210;

    @Test
    void decodeSkipsRetiredShardTlvEntries() throws Exception {
        Intent intent = new Intent("intent_legacy_tlv01");
        intent.setExecuteAt(Instant.parse("2026-06-30T00:00:05Z"));
        intent.transitionTo(IntentStatus.SCHEDULED);
        byte[] slot = SlotCodec.encode(intent);

        // 在 payload 区头部插入两条弃用 TLV（0x08="\0\1a", 0x09="\0\1b"，各 8 字节：tag(1)+len(4)+值(3)），
        // 尾部等量零填充被覆盖——先断言尾部确为零再丢弃。
        byte[] legacyTlv = {
            0x08, 0, 0, 0, 3, 0, 1, 'a',
            0x09, 0, 0, 0, 3, 0, 1, 'b',
        };
        for (int i = PAYLOAD_SIZE - legacyTlv.length; i < PAYLOAD_SIZE; i++) {
            assertEquals(0, slot[OFF_PAYLOAD + i], "payload 尾部应为零填充");
        }
        byte[] payload = new byte[PAYLOAD_SIZE];
        System.arraycopy(slot, OFF_PAYLOAD, payload, 0, PAYLOAD_SIZE);
        byte[] shifted = new byte[PAYLOAD_SIZE];
        System.arraycopy(legacyTlv, 0, shifted, 0, legacyTlv.length);
        System.arraycopy(payload, 0, shifted, legacyTlv.length,
            PAYLOAD_SIZE - legacyTlv.length);

        byte[] mutated = slot.clone();
        System.arraycopy(shifted, 0, mutated, OFF_PAYLOAD, PAYLOAD_SIZE);
        writeCrc(mutated);

        Intent decoded = SlotCodec.decode(mutated);
        assertNotNull(decoded);
        assertEquals("intent_legacy_tlv01", decoded.getIntentId());
        assertEquals(IntentStatus.SCHEDULED, decoded.getStatus());
        assertEquals(intent.getRevision(), decoded.getRevision());
        assertEquals(intent.getExecuteAt(), decoded.getExecuteAt());
        assertNull(decoded.getIdempotencyKey());
    }

    @Test
    void decodeSafeToleratesRetiredShardTlvEntries() throws Exception {
        Intent intent = new Intent("intent_legacy_tlv02");
        intent.setExecuteAt(Instant.parse("2026-06-30T00:00:05Z"));
        intent.transitionTo(IntentStatus.SCHEDULED);
        byte[] slot = SlotCodec.encode(intent);
        // 同款变异（复用辅助逻辑的精简版：直接插到 payload 头部）
        byte[] legacyTlv =
            {0x08, 0, 0, 0, 3, 0, 1, 'a', 0x09, 0, 0, 0, 3, 0, 1, 'b'};
        for (int i = PAYLOAD_SIZE - legacyTlv.length; i < PAYLOAD_SIZE; i++) {
            assertEquals(0, slot[OFF_PAYLOAD + i]);
        }
        byte[] shifted = new byte[PAYLOAD_SIZE];
        System.arraycopy(legacyTlv, 0, shifted, 0, legacyTlv.length);
        System.arraycopy(slot, OFF_PAYLOAD, shifted, legacyTlv.length,
            PAYLOAD_SIZE - legacyTlv.length);
        byte[] mutated = slot.clone();
        System.arraycopy(shifted, 0, mutated, OFF_PAYLOAD, PAYLOAD_SIZE);
        writeCrc(mutated);

        assertNotNull(SlotCodec.decodeSafe(mutated));
    }

    /** 复刻 computeCrc 覆盖范围：[0..9) + [13..256)，标准 CRC32（来源：SlotCodec.computeCrc）。 */
    private static void writeCrc(byte[] slot) {
        CRC32 crc = new CRC32();
        crc.update(slot, 0, OFF_CRC);
        crc.update(slot, OFF_EXECUTE_AT, slot.length - OFF_EXECUTE_AT);
        ByteBuffer buf = ByteBuffer.wrap(slot).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(OFF_CRC, (int) crc.getValue());
    }
}
