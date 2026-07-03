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
     * A4:Builder.wheelConfig(cfg) 必须被尊重 —— 用极小 slotsPerBucket=2,3 个同执行时刻的
     * Intent 落同一桶,第 3 个触发桶溢出(若用默认 1024 则不会溢出)。证明 wheelConfig 生效。
     */
    @Test
    void builderWheelConfigHookIsHonored() throws Exception {
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 2, 1, 10_000L, 60L * 60_000L, 60_000L, null);
        LoomqEngine engine = LoomqEngine.builder().wheelConfig(cfg).build();
        engine.start();
        try {
            Instant exec = Instant.now().plusSeconds(10);
            for (int i = 0; i < 2; i++) {
                Intent it = new Intent("intent_cfg0000000" + i);
                it.setExecuteAt(exec);
                it.setPrecisionTier(PrecisionTier.STANDARD);
                engine.createIntent(it, AckMode.ASYNC).join();
            }
            Intent third = new Intent("intent_cfg00000002");
            third.setExecuteAt(exec);
            third.setPrecisionTier(PrecisionTier.STANDARD);
            // 第 3 个同桶 Intent:slotsPerBucket=2 → 桶溢出 → createIntent 抛错
            assertThrows(RuntimeException.class,
                () -> engine.createIntent(third, AckMode.ASYNC).join(),
                "slotsPerBucket=2 must cause bucket overflow on 3rd same-bucket intent (proves wheelConfig honored)");
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
