package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import java.time.Instant;
import java.util.EnumMap;
import org.junit.jupiter.api.Test;

/**
 * R21: resolveWalMode 第 3 级回退硬编码 PrecisionTierCatalog.defaultCatalog().walMode(...),
 * 无视引擎注入的自定义目录——自定义目录中档位的 walMode 默认(如 ASYNC)被静默替换为
 * DURABLE:耐久性/性能与配置不符,非 DURABLE 崩溃窗口告警永不出现,批量创建的 durable
 * 预扫描也基于同一错误解析(整批被强制 awaitCommit)。修复:回退使用注入的 catalog。
 */
class BugResolveWalModeCustomCatalogTest {

    /** 自定义目录:STANDARD 档 walMode 默认 ASYNC(默认目录为 DURABLE)。 */
    private static PrecisionTierCatalog customCatalog() {
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.ULTRA, new PrecisionTierProfile(10, 200, 1, 5, 16, 200 * 16,
            WalMode.DURABLE, 10, false, true));
        profiles.put(PrecisionTier.FAST, new PrecisionTierProfile(50, 150, 1, 10, 12, 150 * 16,
            WalMode.DURABLE, 50));
        profiles.put(PrecisionTier.STANDARD, new PrecisionTierProfile(500, 50, 20, 100, 3, 50 * 16,
            WalMode.ASYNC, 500));
        profiles.put(PrecisionTier.MILLI, new PrecisionTierProfile(1, 100, 1, 1, 8, 100 * 16,
            WalMode.DURABLE, 1, true, true, 200_000));
        return PrecisionTierCatalog.of(profiles, PrecisionTier.STANDARD);
    }

    @Test
    void resolveWalModeMustHonorInjectedCatalog() {
        Intent intent = new Intent("r21-wal-0001");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        // 修复前:第 3 级回退走 defaultCatalog → DURABLE;修复后:注入目录 → ASYNC
        assertEquals(WalMode.ASYNC,
            IntentCommandService.resolveWalMode(customCatalog(), intent, null),
            "custom catalog's ASYNC default must not be silently replaced by the default catalog's DURABLE");
        // ackMode 显式时优先级不变(向后兼容):DURABLE 显式 > 目录 ASYNC 默认
        assertEquals(WalMode.DURABLE,
            IntentCommandService.resolveWalMode(customCatalog(), intent, AckMode.DURABLE));
        // intent 级 walMode 覆盖优先级不变:intent ASYNC > 目录 ASYNC 默认(ackMode 缺席时)
        intent.setWalMode(WalMode.ASYNC);
        assertEquals(WalMode.ASYNC,
            IntentCommandService.resolveWalMode(customCatalog(), intent, null));
    }
}
