package com.loomq.infrastructure.wheel;

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
    private final Thread thread;
    private final AtomicBoolean running = new AtomicBoolean();

    private final Lock lock = new ReentrantLock();
    private final Condition committed = lock.newCondition();
    private final AtomicLong writeTicket = new AtomicLong();
    private volatile long flushedTicket = 0;  // durability frontier: all writes with ticket <= flushedTicket are forced

    public GroupCommitBarrier(WheelStore store, TailIndex tail, long intervalMs) {
        this.store = store;
        this.tail = tail;
        this.intervalNs = intervalMs * 1_000_000L;
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
     */
    public long awaitCommit() {
        // Writer calls this AFTER store.put() (bytes already in mmap). Claim a ticket;
        // wait until a force that snapshot-ed a ticket >= mine has completed.
        long myTicket = writeTicket.incrementAndGet();
        long deadline = System.nanoTime() + 5_000_000_000L;
        lock.lock();
        try {
            while (flushedTicket < myTicket) {
                if (System.nanoTime() > deadline) {
                    throw new RuntimeException("group-commit timeout, ticket=" + myTicket + ", flushed=" + flushedTicket);
                }
                try {
                    long remainingNs = deadline - System.nanoTime();
                    if (remainingNs <= 0) break;
                    committed.awaitNanos(Math.min(remainingNs, 100_000_000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return flushedTicket;
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
                    flushedTicket = pending;        // publish: writes with ticket <= pending are now forced
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
        if (!running.compareAndSet(true, false)) return;
        // Drain in-flight DURABLE writers: a final force covers all writes whose bytes are
        // already in mmap, then publish the durability frontier so blocked awaitCommit callers
        // return success. Without this, close() stops the loop without advancing flushedTicket,
        // so in-flight writers can only exit via the 5s timeout → createIntent catches, rolls
        // back memory-only and throws FAILED, while wheelStore.close() later flushes the slot →
        // on restart WheelRecovery restores+schedules it → ghost delivery of an intent the
        // caller believes was not created.
        try {
            store.forceDirty();
            tail.flush();
            long pending = writeTicket.get();
            lock.lock();
            try {
                flushedTicket = pending;
                committed.signalAll();
            } finally { lock.unlock(); }
        } catch (Exception e) {
            log.error("final group-commit force on close failed", e);
        }
        thread.interrupt();
        try { thread.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
