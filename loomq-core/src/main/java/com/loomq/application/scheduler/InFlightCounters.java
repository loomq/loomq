package com.loomq.application.scheduler;

import com.loomq.domain.intent.PrecisionTier;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在途投递计数(含跨档借用的投递)。stop() 排空的可靠依据:semaphore 只能感知本档 permit,
 * 借用他档 permit 的在途投递会被漏检,导致 sharedExecutor 提前关闭、完成回调被 reject、
 * ACK/重试决策丢失。dispatch 前 +1,结算任务 finally -1。
 */
final class InFlightCounters {

    private static final Logger logger = LoggerFactory.getLogger(InFlightCounters.class);

    private final Map<PrecisionTier, AtomicInteger> tierInFlight = new EnumMap<>(PrecisionTier.class);

    InFlightCounters(Iterable<PrecisionTier> tiers) {
        for (PrecisionTier tier : tiers) {
            tierInFlight.put(tier, new AtomicInteger(0));
        }
    }

    void increment(PrecisionTier tier) {
        tierInFlight.get(tier).incrementAndGet();
    }

    void decrement(PrecisionTier tier) {
        tierInFlight.get(tier).decrementAndGet();
    }

    int get(PrecisionTier tier) {
        return tierInFlight.get(tier).get();
    }

    /** 等待全部在途归零(10s 超时,超时告警)。 */
    void drainAll() {
        long drainDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (Map.Entry<PrecisionTier, AtomicInteger> entry : tierInFlight.entrySet()) {
            PrecisionTier tier = entry.getKey();
            AtomicInteger inFlight = entry.getValue();
            while (inFlight.get() > 0) {
                if (System.nanoTime() > drainDeadlineNs) {
                    logger.warn("Tier {} has {} intents still in-flight after drain timeout",
                        tier, inFlight.get());
                    break;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
        }
    }
}
