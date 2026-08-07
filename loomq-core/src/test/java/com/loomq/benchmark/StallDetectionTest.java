package com.loomq.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.IntentObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 停摆检测：ULTRA 闭合稳态负载下，若投递在 stallThresholdMs 内零完成则判定停摆，
 * 并在停摆瞬间抓 JVM 线程转储到 dumpPath（核心取证）。
 *
 * 复用 DeliveryPathBenchmark 的 SUCCESS handler + withSlotsPerBucket 大桶。
 * 目标：把"10/11 偶发停摆"变成可判定、可取证、可回归的检测。
 *
 * 运行参数（系统属性）：
 *   -Dstall.rounds=N          轮数，默认 10
 *   -Dstall.thresholdMs=N     停摆阈值，默认 3000
 *   -Dstall.expectReproduce=true  调查模式：不断言（复现+转储，不红），默认 false=回归门禁
 */
@Tag("benchmark")
class StallDetectionTest {

    private static final DeliveryHandler SUCCESS = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
    private static final PrecisionTier TIER = PrecisionTier.ULTRA;

    @Test
    void detectStall_Ultra(@TempDir Path tmp) throws Exception {
        int rounds = Integer.parseInt(System.getProperty("stall.rounds", "10"));
        int thresholdMs = Integer.parseInt(System.getProperty("stall.thresholdMs", "3000"));
        boolean expectReproduce = Boolean.parseBoolean(System.getProperty("stall.expectReproduce", "false"));
        Path dumpRoot = Path.of("target", "stall-dumps");
        Files.createDirectories(dumpRoot);
        Path dumpDir = dumpRoot;

        int stalls = 0;
        for (int r = 0; r < rounds; r++) {
            boolean stalled = runOneRound(
                tmp.resolve("data-round-" + r),
                dumpDir.resolve("stall-round-" + r + "-dump.txt"),
                thresholdMs);
            if (stalled) stalls++;
        }
        System.out.printf("RESULT|stall|rounds=%d|stalls=%d|dumpDir=%s%n", rounds, stalls, dumpDir);
        if (expectReproduce) {
            System.out.println("STALL_INVESTIGATION|expectReproduce=true, 未断言（调查模式）");
        } else {
            assertEquals(0, stalls,
                "引擎投递停摆 " + stalls + "/" + rounds + " 轮；线程转储见 " + dumpDir);
        }
    }

    /** 单轮：跑闭合稳态负载，检测停摆；停摆时抓转储，返回是否停摆。 */
    private boolean runOneRound(Path dataDir, Path dumpPath, int stallThresholdMs) throws Exception {
        var base = PrecisionTierCatalog.defaultCatalog();
        int inFlight = base.profile(TIER).maxConcurrency();
        var wheel = WheelConfig.defaultConfig().withDataDir(dataDir.toString()).withSlotsPerBucket(262_144);

        try (LoomqEngine engine = LoomqEngine.builder()
                .wheelConfig(wheel).nodeId("stall-t")
                .catalog(base)
                .deliveryHandler(SUCCESS).build()) {
            engine.start();

            Semaphore slots = new Semaphore(inFlight);
            AtomicLong completions = new AtomicLong();
            AtomicLong submitted = new AtomicLong();
            AtomicLong lastCompleteNs = new AtomicLong(System.nanoTime());
            AtomicBoolean running = new AtomicBoolean(true);
            java.util.concurrent.ConcurrentHashMap<String, Boolean> outstanding = new java.util.concurrent.ConcurrentHashMap<>();

            Runnable submitOne = () -> {
                long delayMs = 1 + ThreadLocalRandom.current().nextInt(5); // 1..5ms
                Intent intent = new Intent();
                intent.setExecuteAt(Instant.now().plusMillis(delayMs));
                intent.setPrecisionTier(TIER);
                engine.createIntent(intent, AckMode.DURABLE);
                submitted.incrementAndGet();   // createIntent DURABLE 返回 = 已提交
                outstanding.put(intent.getIntentId(), Boolean.TRUE);
            };

            AtomicLong expired = new AtomicLong();
            AtomicLong deadLettered = new AtomicLong();
            AtomicLong deliveryFailed = new AtomicLong();
            engine.registerObserver(new IntentObserver() {
                @Override public void onDelivered(Intent i, DeliveryHandler.DeliveryResult r) {
                    completions.incrementAndGet();
                    lastCompleteNs.set(System.nanoTime());
                    slots.release();
                    outstanding.remove(i.getIntentId());
                }
                @Override public void onScheduled(Intent i) {}
                @Override public void onDeadLettered(Intent i) { deadLettered.incrementAndGet(); }
                @Override public void onExpired(Intent i) { expired.incrementAndGet(); }
                @Override public void onDeliveryFailed(Intent i, Throwable e) { deliveryFailed.incrementAndGet(); }
            });

            Thread producer = Thread.ofVirtual().name("stall-producer").start(() -> {
                while (running.get()) {
                    try { slots.acquire(); } catch (InterruptedException e) { return; }
                    try { submitOne.run(); } catch (RuntimeException e) { /* 单条失败不拖垮 */ }
                }
            });

            boolean stalled = false;
            long runEndNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < runEndNs) {
                Thread.sleep(50);
                long idleNs = System.nanoTime() - lastCompleteNs.get();
                if (idleNs > TimeUnit.MILLISECONDS.toNanos(stallThresholdMs)) {
                    stalled = true;
                    dumpThreads(dumpPath, engine, submitted.get(), completions.get(), outstanding);
                    System.out.println("STALL_DETECTED|round=dump=" + dumpPath + "|idleMs="
                        + TimeUnit.NANOSECONDS.toMillis(idleNs)
                        + "|submitted=" + submitted.get()
                        + "|delivered=" + completions.get()
                        + "|expired=" + expired.get()
                        + "|deadLettered=" + deadLettered.get()
                        + "|deliveryFailed=" + deliveryFailed.get()
                        + "|outstanding=" + outstanding.size());
                    break;
                }
            }

            running.set(false);
            for (int i = 0; i < 1000 && producer.isAlive(); i++) { slots.release(); Thread.sleep(1); }
            producer.join(2000);
            return stalled;
        }
    }

    /** 抓全 JVM 线程转储 + 引擎背压状态到 dumpPath。 */
    private void dumpThreads(Path dumpPath, LoomqEngine engine, long submitted, long delivered,
                             java.util.concurrent.ConcurrentHashMap<String, Boolean> outstanding) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("=== harness counters at stall ===\n");
        sb.append("submitted(createIntent 返回)=").append(submitted)
          .append(" delivered(onDelivered)=").append(delivered)
          .append(" outstanding=").append(submitted - delivered).append("\n");
        sb.append("=== outstanding intent states (engine store) ===\n");
        int shown = 0;
        for (String id : outstanding.keySet()) {
            if (shown >= 20) { sb.append("... 省略 ").append(outstanding.size() - shown).append(" 个\n"); break; }
            try {
                var opt = engine.getIntent(id);
                var it = opt.orElse(null);
                if (it == null) { sb.append(id).append(": NOT_IN_STORE\n"); }
                else { sb.append(id).append(": status=").append(it.getStatus())
                    .append(" rev=").append(it.getRevision())
                    .append(" executeAt=").append(it.getExecuteAt()).append("\n"); }
            } catch (Exception ex) { sb.append(id).append(": ERR ").append(ex).append("\n"); }
            shown++;
        }
        sb.append("\n=== engine finalize diagnostics ===\n");
        try {
            var s = engine.getScheduler();
            sb.append("finalizeTaskExceptions=").append(s.getFinalizeTaskExceptions())
              .append(" persistFailures=").append(s.getPersistFailures()).append("\n");
            for (String ex : s.getFinalizeExceptionSamples()) sb.append("  finalizeException: ").append(ex).append("\n");
        } catch (Exception ignored) { }
        sb.append("\n=== engine backpressure at stall ===\n");
        try {
            for (var e : engine.getScheduler().getBackpressureStatus().entrySet()) {
                var bi = e.getValue();
                sb.append(e.getKey()).append(": queue=").append(bi.queueSize())
                  .append(" availPermits=").append(bi.availablePermits())
                  .append(" activeDispatch=").append(bi.activeDispatches())
                  .append(" borrowed=").append(bi.borrowedCount()).append("\n");
            }
        } catch (Exception ignored) { }
        sb.append("\n=== full JVM thread dump at stall ===\n");
        for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
            Thread t = e.getKey();
            sb.append("### ").append(t.getName())
              .append(" [").append(t.getState()).append("] daemon=").append(t.isDaemon()).append("\n");
            for (StackTraceElement el : e.getValue()) sb.append("    ").append(el).append("\n");
        }
        Files.writeString(dumpPath, sb.toString());
        System.out.println("STALL_DUMP_WRITTEN|" + dumpPath);
    }
}