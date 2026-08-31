package com.loomq.testutil;

import com.loomq.infrastructure.wheel.WheelConfig;
import java.nio.file.Path;

/**
 * 测试共享 WheelConfig 工厂。
 *
 * <p>收敛 19 个测试文件里逐字重复的 7 参魔数字面量(30/16/1/10_000/60min/60s),
 * 默认值变更只改此处;带自定义参数的变体仍直接构造(编码测试意图)。</p>
 */
public final class TestWheelConfigs {

    private TestWheelConfigs() {
    }

    /** 测试默认:30 天视界 / 16 槽 / 1ms 组提交 / 10s 提交超时 / 60min 热边界 / 60s 提升提前量。 */
    public static WheelConfig defaults(Path dataDir) {
        return defaults(dataDir, 16);
    }

    public static WheelConfig defaults(Path dataDir, int slotsPerBucket) {
        return new WheelConfig(dataDir.toString(), 30, slotsPerBucket, 1, 10_000L,
            60L * 60_000L, 60_000L);
    }

    /** 慢组提交变体(awaitCommit 超时/内联 force 兜底相关测试用)。 */
    public static WheelConfig slowCommit(Path dataDir) {
        return new WheelConfig(dataDir.toString(), 30, 16, 60_000L, 50L,
            60L * 60_000L, 60_000L);
    }
}
