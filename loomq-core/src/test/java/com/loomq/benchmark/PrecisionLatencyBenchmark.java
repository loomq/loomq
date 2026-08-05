package com.loomq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.LoomqEngine;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.IntentObserver;
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 触发精度基准：测量 t_dispatch - executeAt 的分布（p50/p99/p999）+ 空闲 CPU。
 * 用 MetricsCollector.getWakeupLatencySnapshot(tier) 读唤醒延迟直方图（微秒）。
 * 输出 RESULT|precision|... 标记，兼容 benchmark 脚本解析。
 * 基线与改造后各跑一次，PR4 对比。
 */
@Tag("benchmark")
class PrecisionLatencyBenchmark {

    private static final DeliveryHandler SUCCESS = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    private static final int COUNT = 1000;
    private static final long MIN_DELAY_MS = 100;
    private static final long MAX_DELAY_MS = 60_000;

    @Test
    void measurePrecision_Ultra(@TempDir Path tmp) throws Exception {
        measurePrecision(tmp, PrecisionTier.ULTRA);
    }

    @Test
    void measurePrecision_Milli(@TempDir Path tmp) throws Exception {
        measurePrecision(tmp, PrecisionTier.MILLI);
    }

    @Test
    void measurePrecision_Fast(@TempDir Path tmp) throws Exception {
        measurePrecision(tmp, PrecisionTier.FAST);
    }

    @Test
    void measurePrecision_High(@TempDir Path tmp) throws Exception {
        measurePrecision(tmp, PrecisionTier.HIGH);
    }

    @Test
    void measurePrecision_Standard(@TempDir Path tmp) throws Exception {
        measurePrecision(tmp, PrecisionTier.STANDARD);
    }

    @Test
    void measurePrecision_Economy(@TempDir Path tmp) throws Exception {
        measurePrecision(tmp, PrecisionTier.ECONOMY);
    }

    private void measurePrecision(Path tmp, PrecisionTier tier) throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("prec-" + tier.name())
                .deliveryHandler(SUCCESS).build()) {
            engine.start();

            AtomicBoolean running = new AtomicBoolean(true);
            OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            Thread cpuProbe = Thread.ofPlatform().daemon(true).unstarted(() -> {
                double last = os.getCpuLoad();
                while (running.get()) {
                    double load = os.getCpuLoad();
                    if (load >= 0 && last >= 0) {
                        System.out.printf("CPUSAMPLE|tier=%s|load=%.3f%n", tier, load);
                    }
                    last = load;
                    try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                }
            });
            cpuProbe.start();

            CountDownLatch latch = new CountDownLatch(COUNT);
            engine.registerObserver(new IntentObserver() {
                @Override public void onScheduled(Intent i) {}
                @Override public void onDelivered(Intent i, DeliveryHandler.DeliveryResult r) { latch.countDown(); }
                @Override public void onDeadLettered(Intent i) {}
                @Override public void onExpired(Intent i) {}
                @Override public void onDeliveryFailed(Intent i, Throwable e) {}
            });

            long now = System.currentTimeMillis();
            List<Intent> intents = new ArrayList<>(COUNT);
            for (int i = 0; i < COUNT; i++) {
                long delay = MIN_DELAY_MS + (long) (Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS));
                Intent intent = new Intent();
                intent.setExecuteAt(Instant.ofEpochMilli(now + delay));
                intent.setPrecisionTier(tier);
                intents.add(intent);
            }
            engine.createIntents(intents, AckMode.DURABLE).get();
            assertTrue(latch.await(120, TimeUnit.SECONDS), "Not all intents delivered for tier " + tier);
            running.set(false);
            cpuProbe.join(1000);

            MetricsCollector.LatencySnapshot wake = engine.getMetricsCollector().getWakeupLatencySnapshot(tier);
            long p50 = wake.p50() / 1000;   // 微秒 -> 毫秒
            long p99 = wake.p99() / 1000;
            long p999 = wake.p999() / 1000;
            System.out.printf("RESULT|precision|tier=%s|count=%d|p50_ms=%d|p99_ms=%d|p999_ms=%d%n",
                tier, COUNT, p50, p99, p999);
            System.out.printf("[Benchmark] %s precision: p50=%dms p99=%dms p999=%dms%n",
                tier, p50, p99, p999);
        }
    }
}