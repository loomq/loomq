package com.loomq;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wheel.WheelConfig;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoomQ 引擎工厂。
 *
 * 提供从 Properties 创建 LoomqEngine 的工厂方法。
 *
 * @author loomq
 */
public final class LoomqEngineFactory {

    private static final Logger logger = LoggerFactory.getLogger(LoomqEngineFactory.class);

    private LoomqEngineFactory() {
        // 工具类，禁止实例化
    }

    /**
     * 从 Properties 创建引擎
     *
     * @param props 配置属性
     * @return LoomqEngine 实例
     */
    public static LoomqEngine createFromProperties(Properties props) {
        logger.info("Creating LoomqEngine from properties");
        if (props == null) {
            props = new Properties();
        }

        return baseBuilder(props).build();
    }

    // ========== 内部方法 ==========

    private static LoomqEngine.Builder baseBuilder(Properties props) {
        LoomqEngine.Builder builder = LoomqEngine.builder();

        String nodeId = props.getProperty("loomq.nodeId", props.getProperty("loomq.node.id", "default-node"));
        builder.nodeId(nodeId);

        // 数据目录:首选 loomq.dataDir,回退 loomq.walDir/loomq.wal.dir(默认 ./data)。
        // walDir 键为 PHTW 前的 WAL 时代遗留,保留读取仅为兼容旧配置;命中时提示迁移。
        String walLegacy = props.getProperty("loomq.walDir", props.getProperty("loomq.wal.dir"));
        String dataDir = props.getProperty("loomq.dataDir",
            walLegacy != null ? walLegacy : "./data");
        if (walLegacy != null) {
            logger.warn("Property loomq.walDir/loomq.wal.dir is deprecated; use loomq.dataDir");
        }

        // 读 wheel.* 属性(horizonDays/slots/groupCommitInterval/awaitCommitTimeout/hotBoundary/promotionLead/defaultTier/shardId),
        // 再用顶层 dataDir 覆盖 wheel.data_dir(保持现行行为:顶层目录赢)
        WheelConfig wheelConfig = WheelConfig.fromProperties(props).withDataDir(dataDir);
        builder.wheelConfig(wheelConfig);

        // wheel.default_tier 显式配置 → 引擎级默认档(与 Builder.defaultTier 同语义)。
        // 此前 WheelConfig.defaultTier 被 LoomqEngine 构造完全忽略(只读 builder.defaultTier,
        // 工厂路径恒 null)——Properties 配置的默认档静默失效。
        String defaultTier = props.getProperty("wheel.default_tier", props.getProperty("wheel.defaultTier"));
        if (defaultTier != null && !defaultTier.isBlank()) {
            builder.defaultTier(PrecisionTier.fromString(defaultTier));
        }

        return builder;
    }
}
