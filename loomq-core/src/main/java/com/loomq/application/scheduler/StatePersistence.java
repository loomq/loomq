package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 状态变更持久化通道(StateChangeSink 包装):把"非阻塞 put"与"阻塞等待落盘"分离,
 * 使调度器可在 synchronized(intent) 内只做 put、在锁外 awaitCommit(VT 不再 pin carrier)。
 *
 * <p><b>I6 容错:</b>持久化失败(如 SlotOverflowException / 慢盘双超时)不阻塞调度流程,
 * 吞掉并计数 persistFailures —— 否则异常在 synchronized 块内传播会跳过观察器通知,
 * 导致基准在途槽位泄漏与引擎死锁(Issue B)。代价:终态未落盘时重启可能按旧 revision 重投,
 * 属可接受耐久劣化。</p>
 */
final class StatePersistence {

    private static final Logger logger = LoggerFactory.getLogger(StatePersistence.class);

    private volatile StateChangeSink sink;
    private final MetricsCollector metrics;

    StatePersistence(MetricsCollector metrics) {
        this.metrics = java.util.Objects.requireNonNull(metrics, "metrics");
    }

    void setSink(StateChangeSink sink) {
        this.sink = sink;
    }

    /** 非持久化失败计数(updateStoreBestEffort 等内存镜像路径共用)。 */
    void countFailure() {
        metrics.incrementPersistFailures();
    }

    /** revision 递增 + 非阻塞 put;须在 synchronized(intent) 内调用以维持 I2/I3 原子性。 */
    void persistStateChange(Intent intent) {
        intent.incrementRevision();
        StateChangeSink s = sink;
        if (s == null) return;
        try {
            s.persist(intent);
        } catch (Exception e) {
            metrics.incrementPersistFailures();
            logger.error("persistStateChange failed for intent {} (revision {}): {}",
                intent.getIntentId(), intent.getRevision(), e.getMessage(), e);
        }
    }

    /** 终态原地覆写(I6 镜像):revision 递增 + 非阻塞原地覆写。 */
    void persistTerminal(Intent intent) {
        intent.incrementRevision();
        StateChangeSink s = sink;
        if (s == null) return;
        try {
            s.persistTerminalInPlace(intent);
        } catch (Exception e) {
            metrics.incrementPersistFailures();
            logger.error("persistTerminalInPlace failed for intent {} (revision {}): {}",
                intent.getIntentId(), intent.getRevision(), e.getMessage(), e);
        }
    }

    /** 终态槽回收;须在 awaitCommit 之后调用。 */
    void reclaimTerminal(String intentId) {
        StateChangeSink s = sink;
        if (s == null) return;
        try {
            s.reclaimTerminal(intentId);
        } catch (Exception e) {
            logger.warn("reclaimTerminal failed for {}: {}", intentId, e.getMessage(), e);
        }
    }

    /** 阻塞到最近一次 put 落盘;须在 synchronized(intent) 之外调用(VT 可正常 unmount)。 */
    void awaitCommit() {
        StateChangeSink s = sink;
        if (s == null) return;
        try {
            s.awaitCommit();
        } catch (Exception e) {
            metrics.incrementPersistFailures();
            logger.error("awaitCommit failed: {}", e.getMessage(), e);
        }
    }
}
