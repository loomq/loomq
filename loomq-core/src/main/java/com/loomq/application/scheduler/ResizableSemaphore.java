package com.loomq.application.scheduler;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Semaphore with cross-tier borrowing support.
 *
 * Extends {@link Semaphore} for zero-overhead acquire/tryAcquire on the hot path.
 * {@link #release()} 继承自 Semaphore 不做覆写;跨档借用的 permit 释放统一走调度器的
 * {@code releasePermit},与 {@link #decrementBorrowed()} 配对,杜绝借用计数泄漏。
 *
 * <p>Runtime resizing ({@code resize}/{@code resizeImmediate}) was removed as dead code
 * (no production caller). If dynamic concurrency adjustment is needed in the future,
 * it should be re-implemented with proper wiring to an adaptive controller.</p>
 */
public class ResizableSemaphore extends Semaphore {

    private final AtomicInteger currentMax;
    private final AtomicInteger borrowedCount;

    public ResizableSemaphore(int initialMax) {
        super(initialMax);
        this.currentMax = new AtomicInteger(initialMax);
        this.borrowedCount = new AtomicInteger(0);
    }

    public int getCurrentMax() { return currentMax.get(); }

    // AdapTBF: cross-tier borrowing tracking
    public int getBorrowedCount() { return borrowedCount.get(); }
    public void incrementBorrowed() { borrowedCount.incrementAndGet(); }
    public void decrementBorrowed() { borrowedCount.decrementAndGet(); }
}
