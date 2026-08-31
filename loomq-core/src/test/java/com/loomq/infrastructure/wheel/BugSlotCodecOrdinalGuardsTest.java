package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;

/**
 * R21: SlotCodec.decode 对 statusOrd 越界无守卫(对比 decodeWalMode/tierByOrdinal 均有界
 * 守卫)——新版本写入含新 IntentStatus 序号的槽后降级运行旧版本:CRC 合法(版本无关字段
 * 覆盖),decode 在 values()[statusOrd-1] 抛 ArrayIndexOutOfBoundsException,恢复扫描路径
 * 无 try/catch → 引擎启动失败。修复:有界校验,越界抛明确 IllegalStateException
 * (fail-loudly 契约),不再裸 AIOOBE。
 */
class BugSlotCodecOrdinalGuardsTest {

    @Test
    void decodeMustRejectOutOfRangeStatusOrdinalWithClearError() {
        Intent intent = new Intent("r21-ordinal-0001");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.incrementRevision();

        byte[] slot = SlotCodec.encode(intent);
        // 模拟"未来版本"写入的高位 statusOrd(0 = 空槽,占位;取 30,远超当前枚举长度)
        slot[0] = 30;
        // 重算 CRC:覆盖 [0..8] status+revision 与 [13..255] executeAt..end(与 computeCrc 同构)
        CRC32 crc = new CRC32();
        crc.update(slot, 0, 9);
        crc.update(slot, 13, SlotCodec.SLOT_SIZE - 13);
        ByteBuffer.wrap(slot).order(ByteOrder.BIG_ENDIAN).putInt(9, (int) crc.getValue());

        // 修复前:ArrayIndexOutOfBoundsException(裸越界,恢复路径中断且无明确原因);
        // 修复后:明确的 IllegalStateException。
        assertThrows(IllegalStateException.class, () -> SlotCodec.decode(slot),
            "out-of-range status ordinal must fail with a clear ISE, not AIOOBE");
    }
}
