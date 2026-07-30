package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CohortManagerTest {

    private static CohortManager createCohortManager() {
        BucketGroupManager bucketGroupManager = new BucketGroupManager();
        return new CohortManager(bucketGroupManager, PrecisionTierCatalog.defaultCatalog(), null, new MetricsCollector());
    }

    private static CohortManager createCohortManager(Consumer<Collection<Intent>> scanTrigger) {
        BucketGroupManager bucketGroupManager = new BucketGroupManager();
        return new CohortManager(bucketGroupManager, PrecisionTierCatalog.defaultCatalog(), scanTrigger, new MetricsCollector());
    }

    private static Intent readyIntent(String id, long delaySeconds, PrecisionTier tier) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.now().plusSeconds(delaySeconds));
        intent.setDeadline(Instant.now().plusSeconds(delaySeconds + 3600));
        intent.setPrecisionTier(tier);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    /**
     * 创建一个 executeAt 已过期的 intent，用于触发即时 flush。
     */
    private static Intent pastIntent(String id, PrecisionTier tier) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.now().minusMillis(100));
        intent.setDeadline(Instant.now().plusSeconds(3600));
        intent.setPrecisionTier(tier);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    @Nested
    @DisplayName("getUpcomingWakes 时间窗口")
    class GetUpcomingWakes {

        @Test
        @DisplayName("范围内返回 cohort")
        void returnsWakesInRange() {
            CohortManager cm = createCohortManager();

            Intent i1 = readyIntent("wake-1", 10, PrecisionTier.STANDARD);
            Intent i2 = readyIntent("wake-2", 30, PrecisionTier.FAST);
            cm.register(i1);
            cm.register(i2);

            Instant from = Instant.now();
            Instant to = from.plusSeconds(60);

            List<CohortManager.CohortWake> wakes = cm.getUpcomingWakes(from, to);
            assertNotNull(wakes);
            assertTrue(wakes.size() >= 1, "Should have at least one cohort in range");
            for (CohortManager.CohortWake wake : wakes) {
                assertTrue(wake.wakeAtMs() > 0);
                assertTrue(wake.intentCount() >= 1);
                assertNotNull(wake.tier());
            }
        }

        @Test
        @DisplayName("范围外返回空")
        void returnsEmptyForNoMatches() {
            CohortManager cm = createCohortManager();

            Intent i1 = readyIntent("far-1", 60, PrecisionTier.STANDARD);
            cm.register(i1);

            Instant from = Instant.now().plus(Duration.ofSeconds(120));
            Instant to = Instant.now().plus(Duration.ofSeconds(180));

            List<CohortManager.CohortWake> wakes = cm.getUpcomingWakes(from, to);
            assertNotNull(wakes);
            assertTrue(wakes.isEmpty(),
                "Should be empty when all cohorts are before the query window");
        }

        @Test
        @DisplayName("空 CohortManager 返回空列表")
        void emptyCohortManager() {
            CohortManager cm = createCohortManager();

            Instant from = Instant.now();
            Instant to = from.plusSeconds(60);

            List<CohortManager.CohortWake> wakes = cm.getUpcomingWakes(from, to);
            assertNotNull(wakes);
            assertTrue(wakes.isEmpty());
        }

        @Test
        @DisplayName("register 批量 intent 后 getUpcomingWakes 正确返回")
        void multipleIntentsSameCohort() {
            CohortManager cm = createCohortManager();

            // Register multiple intents with the same executeAt to create one cohort
            for (int i = 0; i < 5; i++) {
                Intent intent = readyIntent("batch-" + i, 5, PrecisionTier.HIGH);
                cm.register(intent);
            }

            Instant from = Instant.now();
            Instant to = from.plusSeconds(30);

            List<CohortManager.CohortWake> wakes = cm.getUpcomingWakes(from, to);
            assertTrue(wakes.size() >= 1);
            CohortManager.CohortWake wake = wakes.getFirst();
            assertTrue(wake.intentCount() >= 5,
                "Cohort should contain all registered intents");
        }
    }

    @Nested
    @DisplayName("并发安全")
    class Concurrency {

        @Test
        @DisplayName("并发 register + getUpcomingWakes 不抛异常")
        void concurrentRegisterAndGetWakes() throws Exception {
            CohortManager cm = createCohortManager();
            int iterations = 100;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch finishLatch = new CountDownLatch(2);

            var executor = Executors.newVirtualThreadPerTaskExecutor();
            try {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < iterations; i++) {
                            Intent intent = readyIntent("concurrent-" + i, 5, PrecisionTier.FAST);
                            cm.register(intent);
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        finishLatch.countDown();
                    }
                });

                executor.submit(() -> {
                    try {
                        startLatch.await();
                        Instant from = Instant.now();
                        Instant to = from.plusSeconds(120);
                        for (int i = 0; i < iterations; i++) {
                            assertDoesNotThrow(() -> {
                                List<CohortManager.CohortWake> wakes =
                                    cm.getUpcomingWakes(from, to);
                                assertNotNull(wakes);
                            }, "getUpcomingWakes should not throw under concurrent register");
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        finishLatch.countDown();
                    }
                });

                startLatch.countDown();
                assertTrue(finishLatch.await(10, TimeUnit.SECONDS),
                    "Concurrent test should complete within 10s");
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Nested
    @DisplayName("scanTrigger 回调")
    class ScanTrigger {

        @Test
        @DisplayName("cohort flush 时 scanTrigger 被调用，传入非终态 intents")
        void scanTriggerCalledOnFlush() throws Exception {
            CountDownLatch latch = new CountDownLatch(1);
            List<Intent> captured = new CopyOnWriteArrayList<>();

            CohortManager cm = createCohortManager(intents -> {
                captured.addAll(intents);
                latch.countDown();
            });
            try {
                // 使用相同的 executeAt 确保两个 intent 落入同一个 cohort
                Instant pastTime = Instant.now().minusMillis(100);
                Intent i1 = new Intent("flush-1");
                i1.setExecuteAt(pastTime);
                i1.setDeadline(Instant.now().plusSeconds(3600));
                i1.setPrecisionTier(PrecisionTier.STANDARD);
                i1.transitionTo(IntentStatus.SCHEDULED);

                Intent i2 = new Intent("flush-2");
                i2.setExecuteAt(pastTime);
                i2.setDeadline(Instant.now().plusSeconds(3600));
                i2.setPrecisionTier(PrecisionTier.STANDARD);
                i2.transitionTo(IntentStatus.SCHEDULED);

                // 先注册再启动，避免 wake thread 在两次 register 之间抢先 flush
                cm.register(i1);
                cm.register(i2);
                cm.start();

                assertTrue(latch.await(5, TimeUnit.SECONDS),
                    "scanTrigger should be called within 5s");
                assertEquals(2, captured.size(), "scanTrigger should receive 2 intents");
            } finally {
                cm.stop();
            }
        }

        @Test
        @DisplayName("所有 intent 为终态时 scanTrigger 不被调用")
        void scanTriggerNotCalledWhenAllTerminal() throws Exception {
            AtomicInteger callCount = new AtomicInteger(0);

            CohortManager cm = createCohortManager(intents -> callCount.incrementAndGet());
            cm.start();
            try {
                Intent terminal = pastIntent("terminal-1", PrecisionTier.STANDARD);
                terminal.transitionTo(IntentStatus.CANCELED);
                cm.register(terminal);

                Thread.sleep(500);
                assertEquals(0, callCount.get(),
                    "scanTrigger should not be called when all intents are terminal");
            } finally {
                cm.stop();
            }
        }

        @Test
        @DisplayName("scanTrigger 为 null 时 flush 正常工作不抛异常")
        void scanTriggerNullDoesNotThrow() throws Exception {
            CohortManager cm = createCohortManager(null);
            cm.start();
            try {
                assertDoesNotThrow(() -> {
                    cm.register(pastIntent("null-trigger-1", PrecisionTier.STANDARD));
                    Thread.sleep(500);
                });
            } finally {
                cm.stop();
            }
        }
    }
}
