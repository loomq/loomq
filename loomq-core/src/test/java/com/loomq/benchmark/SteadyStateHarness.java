package com.loomq.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** 闭环稳态吞吐测量器：维持固定 inFlight 在途，纳秒精度，重复整窗采样（免疫窗内突发性），预热丢弃。 */
public final class SteadyStateHarness {

    private final Semaphore slots;
    private final AtomicLong completions = new AtomicLong();
    private final ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread producer;
    private final Runnable submitOne;

    public SteadyStateHarness(int inFlight, Runnable submitOne) {
        this.slots = new Semaphore(inFlight);
        this.submitOne = submitOne;
        // 延迟到 run() 再 start：避免构造期即产出，调用方回调引用尚未初始化的 harness 实例。
        this.producer = Thread.ofVirtual().name("bench-producer").unstarted(this::produceLoop);
    }

    private void produceLoop() {
        while (running.get()) {
            try {
                slots.acquire();
            } catch (InterruptedException e) {
                return;
            }
            try {
                submitOne.run();
            } catch (RuntimeException e) {
                // 单条提交失败不拖垮测量；continue 等下一槽
            }
        }
    }

    /** 由调用方在工作项完成时调用（observer / future 回调）。 */
    public void onComplete(long latencyUs) {
        completions.incrementAndGet();
        latencies.add(latencyUs);
        slots.release();
    }

    public Result run(long warmupMs, long measureWindowMs, int windows) throws InterruptedException {
        producer.start();
        // 预热：跑满管道，丢弃
        long warmupEnd = System.nanoTime() + warmupMs * 1_000_000L;
        while (System.nanoTime() < warmupEnd) Thread.sleep(20);
        completions.set(0);

        // 测量：每窗一个总吞吐样本（免疫窗内突发性）
        List<Double> windowQps = new ArrayList<>();
        for (int w = 0; w < windows; w++) {
            completions.set(0);
            long startNs = System.nanoTime();
            long endNs = startNs + measureWindowMs * 1_000_000L;
            while (System.nanoTime() < endNs) Thread.sleep(Math.min(50, measureWindowMs / 10));
            long c = completions.get();
            long elapsedNs = System.nanoTime() - startNs;
            if (elapsedNs > 0) windowQps.add(c * 1_000_000_000.0 / elapsedNs);
        }
        running.set(false);
        // 释放足够 permit 让 producer 退出
        for (int i = 0; i < 1000 && producer.isAlive(); i++) { slots.release(); Thread.sleep(1); }
        producer.join(2000);

        double[] sq = Stats.sorted(windowQps.stream().mapToDouble(Double::doubleValue).toArray());
        double[] sl = Stats.sorted(latencies.stream().mapToLong(Long::longValue).asDoubleStream().toArray());
        return new Result(
            Stats.median(sq), Stats.iqr(sq), sq.length,
            (long) (sl.length == 0 ? 0 : Stats.median(sl)),
            (long) (sl.length == 0 ? 0 : Stats.p99(sl)),
            (long) (sl.length == 0 ? 0 : Stats.p999(sl)));
    }

    public record Result(
        double qpsMedian, double qpsIqr, int qpsSamples,
        long e2eP50Us, long e2eP99Us, long e2eP999Us
    ) {}
}