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
        WheelConfig cfg = new WheelConfig(tmp.toString(), 30, 2, 1, 10_000L, 60L * 60_000L, 60_000L);
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

    /**
     * A4:工厂路径的 wheel.default_tier 必须生效。此前 LoomqEngine 构造只读
     * builder.defaultTier(工厂路径恒 null),WheelConfig.defaultTier 被完全忽略
     * ——Properties 配置的默认档静默失效,createIntent 沿用 intent 自带档位。
     */
    @Test
    void wheelDefaultTierPropertyIsHonored() throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("loomq.dataDir", tmp.resolve("d").toString());
        props.setProperty("wheel.default_tier", "FAST");
        LoomqEngine engine = LoomqEngineFactory.createFromProperties(props);
        engine.start();
        try {
            Intent it = new Intent("intent_cfg_tier0001");
            it.setExecuteAt(Instant.now().plusSeconds(30));
            it.setPrecisionTier(PrecisionTier.STANDARD); // 显式 STANDARD;引擎默认档应覆盖为 FAST
            engine.createIntent(it, AckMode.ASYNC).join();
            assertEquals(PrecisionTier.FAST, it.getPrecisionTier(),
                "wheel.default_tier=FAST must be honored as the engine-level default tier");
        } finally {
            engine.close();
        }
    }
}
