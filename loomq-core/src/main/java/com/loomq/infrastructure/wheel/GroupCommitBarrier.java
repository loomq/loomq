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
    private final long awaitCommitTimeoutNs;
    private final Thread thread;
    private final AtomicBoolean running = new AtomicBoolean();

    private final Lock lock = new ReentrantLock();
    private final Condition committed = lock.newCondition();
    private final AtomicLong writeTicket = new AtomicLong();
    private volatile long flushedTicket = 0;  // durability frontier: all writes with ticket <= flushedTicket are forced

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

        // 段 2:超时兜底 —— daemon 未在超时内推进 frontier。不持锁 force(避免阻塞 daemon loop)。
        // 先快照 pending(writeTicket 当前值,含 myTicket)再 force —— 镜像 daemon loop 的安全模式:
        // 若 force 之后才读 writeTicket,并发写者 B 在 force 与读之间 put+领 ticket 会被错误地
        // 标记为已持久(flushedTicket >= myTicket_B),但其字节在 force 之后才入 mmap → 崩溃丢失,
        // 违反 DURABLE 契约。快照在 force 前,确保只发布 force 已覆盖的 ticket。
        long pending = writeTicket.get();
        try {
            store.forceDirty();
            tail.flush();
        } catch (Exception forceEx) {
            // 真 I/O 故障:字节已在 mmap,崩溃可能丢失,属磁盘故障极端边缘,不静默吞。
            throw new RuntimeException("inline force fallback failed, ticket=" + myTicket, forceEx);
        }
        lock.lock();
        try {
            if (pending > flushedTicket) {   // 不回退:daemon 可能在 force 期间已推进更高 frontier
                flushedTicket = pending;
            }
            committed.signalAll();
            return flushedTicket;            // >= myTicket(pending 含 myTicket,且不回退)
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
