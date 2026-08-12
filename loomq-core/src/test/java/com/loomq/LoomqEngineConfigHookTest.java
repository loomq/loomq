package com.loomq;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wheel.WheelConfig;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LoomqEngineConfigHookTest {
    @TempDir Path tmp;

    /**
     * A4:Builder.wheelConfig(cfg) 必须被尊重 —— 用极小 slotsPerBucket=2,9 个同执行时刻的
     * Intent：SEC(2)+MIN(2)+HOUR(2)+DAY(2)=8 落整条链,第 9 个触发链终点溢出
     * (若用默认 1024 则不会溢出)。证明 wheelConfig 生效,同时覆盖 spill 链。
     */
    @Test
    void builderWheelConfigHookIsHonored() throws Exception {
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 2, 1, 10_000L, 60L * 60_000L, 60_000L, null);
        LoomqEngine engine = LoomqEngine.builder().wheelConfig(cfg).build();
        engine.start();
        try {
            Instant exec = Instant.now().plusSeconds(10);
            for (int i = 0; i < 8; i++) {
                Intent it = new Intent("intent_cfg0000000" + i);
                it.setExecuteAt(exec);
                it.setPrecisionTier(PrecisionTier.STANDARD);
                engine.createIntent(it, AckMode.ASYNC).join();
            }
            Intent ninth = new Intent("intent_cfg00000008");
            ninth.setExecuteAt(exec);
            ninth.setPrecisionTier(PrecisionTier.STANDARD);
            // 第 9 个同秒 Intent:整条溢出链(2+2+2+2)耗尽 → createIntent 抛错
            assertThrows(RuntimeException.class,
                () -> engine.createIntent(ninth, AckMode.ASYNC).join(),
                "slotsPerBucket=2 must exhaust spill chain on 9th same-instant intent (proves wheelConfig honored)");
        } finally {
            engine.close();
        }
    }

    /** A4:dataDir(Path) 与 walDir(Path) 等价(都设数据目录,引擎可启动)。 */
    @Test
    void dataDirAndWalDirAreEquivalent() throws Exception {
        LoomqEngine a = LoomqEngine.builder().dataDir(tmp.resolve("a")).build();
        a.start();
        a.close();

        LoomqEngine b = LoomqEngine.builder().walDir(tmp.resolve("b")).build();
        b.start();
        b.close();
        // 两个引擎都能正常启停即通过(废弃别名仍可用)
    }
}
