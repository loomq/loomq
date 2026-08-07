package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * ResizableSemaphore 借用计数不变量测试。
 *
 * <p>验证 acquireWithBorrow -> releasePermit 的完整生命周期：
 * <ul>
 *   <li>本档有 permit 时不借用，borrowedCount 不变</li>
 *   <li>本档空、借他档时，他档 borrowedCount +1</li>
 *   <li>释放借用 permit 时配对 decrementBorrowed，borrowedCount 归零</li>
 *   <li>空 poll 路径正确释放借用 permit（P1-1 修复的泄漏路径之一）</li>
 *   <li>批量中断路径释放全部已获取 permit（P1-1 修复的泄漏路径之二）</li>
 * </ul>
 */
class BorrowCountInvariantTest {

    /**
     * 模拟 PrecisionScheduler.releasePermit 的逻辑：
     * 如果 acquired != 本档 semaphore，则 decrementBorrowed + release。
     */
    private void releasePermit(ResizableSemaphore ownTier, ResizableSemaphore acquired) {
        if (acquired != ownTier) {
            acquired.decrementBorrowed();
        }
        acquired.release();
    }

    @Test
    void ownAcquireDoesNotIncrementBorrowedCount() {
        ResizableSemaphore own = new ResizableSemaphore(2);
        assertEquals(0, own.getBorrowedCount());

        assertTrue(own.tryAcquire());
        assertEquals(1, own.availablePermits());
        assertEquals(0, own.getBorrowedCount(), "own-tier acquire must not increment borrowedCount");

        releasePermit(own, own);
        assertEquals(2, own.availablePermits());
        assertEquals(0, own.getBorrowedCount(), "release of own permit must not change borrowedCount");
    }

    @Test
    void borrowedAcquireIncrementsAndReleaseDecrements() {
        ResizableSemaphore own = new ResizableSemaphore(1);
        ResizableSemaphore other = new ResizableSemaphore(2);

        // Exhaust own permits
        assertTrue(own.tryAcquire());
        assertEquals(0, own.availablePermits());

        // Borrow from other tier (simulating acquireWithBorrow)
        assertTrue(other.tryAcquire());
        other.incrementBorrowed();
        assertEquals(1, other.getBorrowedCount(), "borrowed tier's borrowedCount must be 1");
        assertEquals(1, other.availablePermits());

        // Release the borrowed permit
        releasePermit(own, other);
        assertEquals(0, other.getBorrowedCount(), "releasePermit must decrementBorrowed for borrowed permit");
        assertEquals(2, other.availablePermits(), "releasePermit must release the permit back");

        // Release own permit
        releasePermit(own, own);
        assertEquals(1, own.availablePermits());
        assertEquals(0, own.getBorrowedCount());
    }

    @Test
    void emptyPollReleasesBorrowedPermit() {
        // Simulates runSingleIntentConsumer's empty-poll path:
        // acquire -> queue.poll() returns null -> releasePermit
        ResizableSemaphore own = new ResizableSemaphore(1);
        ResizableSemaphore other = new ResizableSemaphore(1);

        // Exhaust own, borrow from other
        assertTrue(own.tryAcquire());
        assertTrue(other.tryAcquire());
        other.incrementBorrowed();
        assertEquals(1, other.getBorrowedCount());

        // Empty poll: release immediately (Fix 7: 空 poll 释放同样配对 decrementBorrowed)
        releasePermit(own, other);
        assertEquals(0, other.getBorrowedCount(),
            "P1-1 fix: empty-poll path must decrementBorrowed (was a leak before fix)");
        assertEquals(1, other.availablePermits());
    }

    @Test
    void batchInterruptReleasesAllAcquiredPermits() throws Exception {
        // Simulates runBatchDrainConsumer's interrupt path:
        // acquire N permits (some borrowed) -> InterruptedException -> release all
        ResizableSemaphore own = new ResizableSemaphore(1);
        ResizableSemaphore other1 = new ResizableSemaphore(2);
        ResizableSemaphore other2 = new ResizableSemaphore(2);

        // Phase 1: acquire 3 permits (1 own, 2 borrowed)
        assertTrue(own.tryAcquire());           // own permit
        assertTrue(other1.tryAcquire());         // borrowed from other1
        other1.incrementBorrowed();
        assertTrue(other2.tryAcquire());         // borrowed from other2
        other2.incrementBorrowed();

        assertEquals(1, other1.getBorrowedCount());
        assertEquals(1, other2.getBorrowedCount());

        // Phase 2: interrupted - release all acquired permits (Fix 7: 中断释放同样配对 decrementBorrowed)
        releasePermit(own, own);
        releasePermit(own, other1);
        releasePermit(own, other2);

        // All borrowed counts must be zero
        assertEquals(0, other1.getBorrowedCount(),
            "P1-1 fix: interrupt path must decrementBorrowed for all borrowed permits");
        assertEquals(0, other2.getBorrowedCount(),
            "P1-1 fix: interrupt path must decrementBorrowed for all borrowed permits");

        // All permits restored
        assertEquals(1, own.availablePermits());
        assertEquals(2, other1.availablePermits());
        assertEquals(2, other2.availablePermits());
    }

    @Test
    void multipleBorrowAndReleaseCyclesConverge() {
        // Verify that repeated borrow/release cycles don't leak borrowedCount
        ResizableSemaphore own = new ResizableSemaphore(1);
        ResizableSemaphore other = new ResizableSemaphore(3);

        for (int i = 0; i < 100; i++) {
            // Exhaust own, borrow from other
            assertTrue(own.tryAcquire());
            assertTrue(other.tryAcquire());
            other.incrementBorrowed();

            // Release both
            releasePermit(own, own);
            releasePermit(own, other);
        }

        assertEquals(0, other.getBorrowedCount(),
            "after 100 borrow/release cycles, borrowedCount must be 0 (no leak)");
        assertEquals(1, own.availablePermits());
        assertEquals(3, other.availablePermits());
    }
}
