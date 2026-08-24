package com.loomq.infrastructure.wheel;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rendezvous group-commit daemon:周期性对所有脏桶执行 msync(forceDirty)。
 * DURABLE 写者 awaitCommit() 阻塞到覆盖其写入的 force 完成。
 *
 * <p>使用单调递增的 durability-ticket 模型:写者在 store.put()(字节已进 mmap)之后
 * 调用 awaitCommit() 领取一个 ticket,等待某次"force 前 snapshot 已包含该 ticket"的
 * force 完成。这保证写者返回时其写入一定被一次覆盖它的 force 刷盘——不会出现
 * "force 在 put 之前执行、写者却得到 DURABLE 返回"的 under-wait。</p>
 */
public final class GroupCommitBarrier implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GroupCommitBarrier.class);

    private final WheelStore store;
    private final TailIndex tail;
    private final long intervalNs;
    private final long awaitCommitTimeoutNs;
    private final Thread thread;
    private final AtomicBoolean running = new AtomicBoolean();

    private final Lock lock = new ReentrantLock();
    private final Condition committed = lock.newCondition();
    private final AtomicLong writeTicket = new AtomicLong();
    private volatile long flushedTicket = 0;  // durability frontier: all writes with ticket <= flushedTicket are forced

    /**
     * 慢盘超时兜底的单飞行标志:多个 DURABLE 写者同时超时时,只让一个做内联 force,
     * 其余等它推进 frontier——避免 N 个写者各做一次全量 forceDirty 的写放大(惊群)。
     */
    private final AtomicBoolean inlineForceInFlight = new AtomicBoolean(false);
    /** 内联 force 兜底执行器(平台线程):msync 是 native 阻塞,在 VT 上会 pin carrier,
     *  慢盘下批量 pin 致 VT 调度雪崩。移交平台线程,调用 VT 在 future.get() 上 park(不 pin)。 */
    private final ExecutorService inlineForceExecutor =
        Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "wheel-inline-force");
            t.setDaemon(true);
            return t;
        });

    public GroupCommitBarrier(WheelStore store, TailIndex tail, long intervalMs, long awaitCommitTimeoutMs) {
        this.store = store;
        this.tail = tail;
        this.intervalNs = intervalMs * 1_000_000L;
        this.awaitCommitTimeoutNs = awaitCommitTimeoutMs * 1_000_000L;
        this.thread = Thread.ofPlatform().name("wheel-group-commit").daemon(true).unstarted(this::loop);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            thread.start();
            log.info("GroupCommitBarrier started, interval={}ms", intervalNs / 1_000_000);
        }
    }

    /**
     * 阻塞到覆盖本次写入的 force 完成。
     * 写者必须在 store.put()(字节已进 mmap)之后调用本方法:领取一个 ticket,
     * 等待某次 snapshot 了 ticket >= myTicket 的 force 完成。
     *
     * <p>超时不再抛错:慢盘下 daemon 未能及时推进 frontier 时,改走内联 force 兜底
     * (store.forceDirty()+tail.flush()),随后推进 flushedTicket 并返回成功。
     * 这保证 DURABLE 契约成立 —— 否则 createIntent 内存回滚 + FAILED,而
     * wheelStore.close() 仍刷盘 → 重启 recovery 复活 = ghost 投递。</p>
     */
    public long awaitCommit() {
        // Writer calls this AFTER store.put() (bytes already in mmap). Claim a ticket;
        // wait until a force that snapshot-ed a ticket >= mine has completed.
        long myTicket = writeTicket.incrementAndGet();
        long deadline = System.nanoTime() + awaitCommitTimeoutNs;

        // 段 1:等待 daemon 在超时内推进 frontier
        lock.lock();
        try {
            while (flushedTicket < myTicket) {
                if (System.nanoTime() >= deadline) break; // 超时 → 走内联 force 兜底
                try {
                    long remainingNs = deadline - System.nanoTime();
                    if (remainingNs <= 0) break;
                    committed.awaitNanos(Math.min(remainingNs, 100_000_000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            if (flushedTicket >= myTicket) {
                return flushedTicket; // daemon 已覆盖,正常返回
            }
        } finally { lock.unlock(); }

        // 段 2:超时兜底 —— daemon 未在超时内推进 frontier。
        // P1-10 + 惊群治理:单飞行(只一个写者做 force)+ 平台线程(msync 不 pin VT)。
        // 其余超时写者等 frontier 推进;若等不到再轮流做 force。force 在 pending 快照之后
        // 才执行,确保只发布 force 已覆盖的 ticket(同 daemon loop 的安全模式)。
        long fallbackDeadline = System.nanoTime() + awaitCommitTimeoutNs;
        while (flushedTicket < myTicket) {
            if (inlineForceInFlight.compareAndSet(false, true)) {
                // 本轮由我执行内联 force
                final long pending = writeTicket.get();
                try {
                    inlineForceExecutor.submit(() -> doInlineForce(pending)).get();
                } catch (RejectedExecutionException ree) {
                    // R21: close() 已关执行器(stop/close 交错)——写者字节已在 mmap,若按
                    // 失败回滚而字节已持久化,重启后 SCHEDULED 槽复活 = 幽灵投递(正是段 2
                    // 兜底要防的窗口)。shutdown 期间在调用线程同步 force(接受短暂 pin,
                    // 正确性优先);force 成功即发布 frontier,写者按成功返回。
                    doInlineForce(pending);
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
                    throw new RuntimeException("inline force fallback failed, ticket=" + myTicket, cause);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                } finally {
                    inlineForceInFlight.set(false);
                }
                // doInlineForce 已推进 flushedTicket + signalAll;循环条件重判
            } else {
                // 另一写者正在做内联 force:等它推进 frontier,不重复 force
                lock.lock();
                try {
                    if (flushedTicket >= myTicket) break;
                    long rem = fallbackDeadline - System.nanoTime();
                    if (rem <= 0) break;
                    try {
                        committed.awaitNanos(Math.min(rem, 100_000_000L));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(ie);
                    }
                } finally { lock.unlock(); }
                if (System.nanoTime() > fallbackDeadline) break;
            }
        }
        lock.lock();
        try {
            if (flushedTicket < myTicket) {
                // 兜底仍未覆盖——极端慢盘/故障。不谎报 DURABLE 成功,抛错让调用方决策。
                throw new RuntimeException("inline force fallback timed out, ticket=" + myTicket
                    + " flushedTicket=" + flushedTicket);
            }
            return flushedTicket;
        } finally { lock.unlock(); }
    }

    /**
     * 内联 force 主体(在平台线程执行):snapshot 已取,force 成功后推进 frontier 并唤醒等待者。
     *
     * <p><b>失败不发布</b>:forceDirty/flush 抛异常(ENOSPC/EIO/段已关闭)时,字节并未落盘,
     * 推进 frontier 会让其他在兜底分支等待的写者误判"已被覆盖"而返回成功——虚假的 DURABLE
     * 确认,崩溃即丢数据。与 daemon loop 一致:仅成功后发布;失败时唤醒等待者使其接管重试,
     * 异常上抛给本轮执行者(其 awaitCommit 抛错,由调用方决策补偿)。
     */
    private void doInlineForce(long pending) {
        try {
            store.forceDirty();
            tail.flush();
        } catch (RuntimeException e) {
            lock.lock();
            try {
                committed.signalAll();   // 唤醒等待者重试/自行接管 force;frontier 不动
            } finally { lock.unlock(); }
            throw e;
        }
        lock.lock();
        try {
            if (pending > flushedTicket) {   // 不回退:daemon 可能已推进更高 frontier
                flushedTicket = pending;
            }
            committed.signalAll();
        } finally { lock.unlock(); }
    }

    public long currentGeneration() { return flushedTicket; }

    private void loop() {
        while (running.get()) {
            try {
                long pending = writeTicket.get();   // snapshot BEFORE force: any write whose ticket <= pending is in mmap now
                store.forceDirty();                 // forces all dirty buckets' current mmap contents
                tail.flush();                       // forces tail run file buffer (>30-day Intent writes) to disk
                lock.lock();
                try {
                    // C2-11: 单调发布守卫(镜像 doInlineForce)——daemon force 期间可能有超时
                    // 写者走内联 force 并发布了更高 frontier;无条件回退发布会让 frontier
                    // 非单调(违反 javadoc 表述)。守卫仅保守多等,无虚假成功。
                    if (pending > flushedTicket) {
                        flushedTicket = pending;    // publish: writes with ticket <= pending are now forced
                    }
                    committed.signalAll();
                } finally { lock.unlock(); }
            } catch (Exception e) {
                log.error("group-commit error", e);
            }
            LockSupport.parkNanos(intervalNs);
        }
        log.info("GroupCommitBarrier stopped");
    }

    @Override public void close() {
        boolean wasRunning = running.compareAndSet(true, false);
        if (wasRunning) {
            // Drain in-flight DURABLE writers: a final force covers all writes whose bytes are
            // already in mmap, then publish the durability frontier so blocked awaitCommit callers
            // return success. Without this, close() stops the loop without advancing flushedTicket;
            // in-flight writers would each hit the configurable awaitCommit timeout and fall back to
            // their own inline force — racing wheelStore.close()'s force during shutdown. The drain
            // makes shutdown deterministic and fast.
            try {
                store.forceDirty();
                tail.flush();
                long pending = writeTicket.get();
                lock.lock();
                try {
                    if (pending > flushedTicket) {   // C2-11: 单调发布(同 daemon loop)
                        flushedTicket = pending;
                    }
                    committed.signalAll();
                } finally { lock.unlock(); }
            } catch (Exception e) {
                log.error("final group-commit force on close failed", e);
            }
        }
        // 无论是否曾启动，都尝试中断/join 线程；未启动线程的 join 会立即返回。
        thread.interrupt();
        try { thread.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        // 关闭内联 force 执行器(平台线程)。即使 barrier 从未 start，也必须释放该线程池。
        inlineForceExecutor.shutdown();
        try {
            if (!inlineForceExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                inlineForceExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            inlineForceExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** 仅停止 daemon，不执行最终 force（test-only：供 simulateCrash 使用；非生产 API）。 */
    public void stopWithoutFlush() {
        running.set(false);
    }
}
