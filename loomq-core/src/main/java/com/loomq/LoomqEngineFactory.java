package com.loomq;

import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.spi.CallbackHandler;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.RedeliveryDecider;
import java.nio.file.Path;
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

        return baseBuilder(props).build();
    }

    /**
     * 从 Properties 创建引擎并注册回调处理器
     *
     * @param props           配置属性
     * @param callbackHandler 回调处理器
     * @return LoomqEngine 实例
     */
    public static LoomqEngine createFromProperties(Properties props, CallbackHandler callbackHandler) {
        LoomqEngine engine = createFromProperties(props);
        engine.registerCallbackHandler(callbackHandler);
        return engine;
    }

    /**
     * 从 Properties 创建引擎并配置投递处理器
     *
     * @param props           配置属性
     * @param deliveryHandler 投递处理器
     * @return LoomqEngine 实例
     */
    public static LoomqEngine createFromProperties(Properties props, DeliveryHandler deliveryHandler) {
        logger.info("Creating LoomqEngine from properties with DeliveryHandler");

        return baseBuilder(props)
            .deliveryHandler(deliveryHandler)
            .build();
    }

    /**
     * 从 Properties 创建引擎（完整配置）
     *
     * @param props             配置属性
     * @param deliveryHandler   投递处理器
     * @param redeliveryDecider 重投决策器
     * @return LoomqEngine 实例
     */
    public static LoomqEngine createFromProperties(Properties props, DeliveryHandler deliveryHandler, RedeliveryDecider redeliveryDecider) {
        logger.info("Creating LoomqEngine from properties with DeliveryHandler and RedeliveryDecider");

        return baseBuilder(props)
            .deliveryHandler(deliveryHandler)
            .redeliveryDecider(redeliveryDecider)
            .build();
    }

    /**
     * 快速创建引擎（使用默认配置）
     *
     * @param dataDir 数据目录
     * @return LoomqEngine 实例
     */
    public static LoomqEngine createDefault(Path dataDir) {
        return LoomqEngine.builder()
            .dataDir(dataDir)
            .build();
    }

    /**
     * 快速创建引擎并注册回调
     *
     * @param dataDir         数据目录
     * @param callbackHandler 回调处理器
     * @return LoomqEngine 实例
     */
    public static LoomqEngine createDefault(Path dataDir, CallbackHandler callbackHandler) {
        LoomqEngine engine = createDefault(dataDir);
        engine.registerCallbackHandler(callbackHandler);
        return engine;
    }

    /**
     * 快速创建引擎并配置投递处理器
     *
     * @param dataDir         数据目录
     * @param deliveryHandler 投递处理器
     * @return LoomqEngine 实例
     */
    public static LoomqEngine createDefault(Path dataDir, DeliveryHandler deliveryHandler) {
        return LoomqEngine.builder()
            .dataDir(dataDir)
            .deliveryHandler(deliveryHandler)
            .build();
    }

    // ========== 内部方法 ==========

    private static LoomqEngine.Builder baseBuilder(Properties props) {
        LoomqEngine.Builder builder = LoomqEngine.builder();

        String nodeId = props.getProperty("loomq.nodeId", props.getProperty("loomq.node.id", "default-node"));
        builder.nodeId(nodeId);

        // 数据目录:首选 loomq.dataDir,回退 loomq.walDir/loomq.wal.dir(默认 ./data)
        String dataDir = props.getProperty("loomq.dataDir",
            props.getProperty("loomq.walDir", props.getProperty("loomq.wal.dir", "./data")));

        // 读 wheel.* 属性(horizonDays/slots/groupCommitInterval/awaitCommitTimeout/hotBoundary/promotionLead/defaultTier/shardId),
        // 再用顶层 dataDir 覆盖 wheel.data_dir(保持现行行为:顶层目录赢)
        WheelConfig wheelConfig = WheelConfig.fromProperties(props).withDataDir(dataDir);
        builder.wheelConfig(wheelConfig);

        return builder;
    }
}
