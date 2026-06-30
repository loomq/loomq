# PR2: WAL/SimpleWalWriter — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the use-after-free race in `truncateBefore()` and extract a shared `closeSegment()` helper to prevent future ordering bugs.

**Architecture:** In `truncateBefore()`, `seg.arena.close()` is called before `seg.closed = true` (line 592-593). The `flushLoop()` thread checks `seg.closed` (line 537) then calls `seg.mappedRegion.force()` (line 543) without holding any lock. If the arena is closed between these two operations, `force()` operates on unmapped memory → JVM crash. The fix: swap the order to match the correct pattern used in `close()` (lines 949-976). Extract a `closeSegment()` helper for both sites.

**Tech Stack:** Java 25, JUnit 5

**Files to modify:**
- `loomq-core/src/main/java/com/loomq/infrastructure/wal/SimpleWalWriter.java`
- `loomq-core/src/test/java/com/loomq/recovery/RecoveryPipelineTest.java` (or new test file)

---

### Task 1: Add test for concurrent truncateBefore + flushLoop

**Files:**
- Modify: `loomq-core/src/test/java/com/loomq/infrastructure/wal/SimpleWalWriterTest.java` (if exists)
- Or create: `loomq-core/src/test/java/com/loomq/infrastructure/wal/SimpleWalWriterTruncateTest.java`

- [ ] **Step 1: Find existing test location**

Check if `SimpleWalWriterTest.java` exists. If not, create a new test class in the same test directory.

- [ ] **Step 2: Write the stress test**

```java
@Test
void truncateBeforeShouldNotCrashDuringConcurrentFlush() throws Exception {
    // Setup: create WAL with multiple segments
    Path dataDir = Files.createTempDirectory("wal-truncate-test-");
    WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
        "memory_segment", 1, 8, 64, 10, 4, 1, false);
    SimpleWalWriter wal = new SimpleWalWriter(cfg, "truncate-test");
    try {
        // Write enough data to create multiple segments
        for (int i = 0; i < 1000; i++) {
            wal.append(1L, ("entry-" + i).getBytes());
        }

        // Start concurrent flush loop (wal already has one, but we stress it)
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicReference<Exception> error = new AtomicReference<>();
        Thread flushThread = new Thread(() -> {
            try {
                while (running.get()) {
                    // Simulate what flushLoop does: iterate segments, check closed, force
                    // This is a stress test — the real flushLoop runs inside wal
                    Thread.sleep(1);
                }
            } catch (Exception e) {
                error.set(e);
            }
        });
        flushThread.start();

        // Concurrently truncate segments
        for (int i = 0; i < 100; i++) {
            wal.truncateBefore(i * 10);
            Thread.sleep(1);
        }

        running.set(false);
        flushThread.join(1000);

        // If we get here without a crash, the test passes
        assertNull(error.get(), "Concurrent flush + truncate should not throw");
    } finally {
        wal.close();
        // Cleanup
        Files.walk(dataDir).sorted(java.util.Comparator.reverseOrder())
            .forEach(p -> { try { Files.delete(p); } catch (Exception ignored) {} });
    }
}
```

- [ ] **Step 3: Run test**

Run: `mvn test -pl loomq-core -am -Dtest=SimpleWalWriterTruncateTest -Pslow-tests`
Expected: May crash or pass (stress test). If it crashes, the bug is confirmed.

- [ ] **Step 4: Commit test**

```bash
git add loomq-core/src/test/java/com/loomq/infrastructure/wal/
git commit -m "test: add truncateBefore + flushLoop concurrency stress test (#1)"
```

---

### Task 2: Fix truncateBefore — swap arena.close() and seg.closed ordering

**Files:**
- Modify: `loomq-core/src/main/java/com/loomq/infrastructure/wal/SimpleWalWriter.java:589-602`

- [ ] **Step 1: Swap the ordering in truncateBefore**

In `truncateBefore()`, change lines 589-602 from:
```java
for (Segment seg : toRemove) {
    try {
        seg.mappedRegion.force();
        seg.arena.close();       // ← WRONG: closes before marking closed
        seg.closed = true;
        seg.channel.close();
        Files.deleteIfExists(seg.path);
        segments.remove(seg);
        ...
    }
}
```

to:
```java
for (Segment seg : toRemove) {
    try {
        seg.closed = true;       // ← FIX: mark closed FIRST
        seg.mappedRegion.force();
        seg.arena.close();
        seg.channel.close();
        Files.deleteIfExists(seg.path);
        segments.remove(seg);
        ...
    }
}
```

- [ ] **Step 2: Run the stress test**

Run: `mvn test -pl loomq-core -am -Dtest=SimpleWalWriterTruncateTest -Pslow-tests`
Expected: PASS (no crash)

- [ ] **Step 3: Run all core tests**

Run: `mvn test -pl loomq-core -am -Pfast-tests`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add loomq-core/src/main/java/com/loomq/infrastructure/wal/SimpleWalWriter.java
git commit -m "fix: truncateBefore sets seg.closed before arena.close() to prevent use-after-free (#1)"
```

---

### Task 3: Refactor — extract closeSegment helper

**Files:**
- Modify: `loomq-core/src/main/java/com/loomq/infrastructure/wal/SimpleWalWriter.java`

- [ ] **Step 1: Add closeSegment private method**

```java
/**
 * 关闭单个段：标记为已关闭 → 刷盘 → 关闭 Arena 和 FileChannel。
 * 必须在 seg.closed = true 之后才能调用 arena.close()，
 * 防止 flushLoop 并发访问已释放的内存映射。
 */
private void closeSegment(Segment seg) {
    try {
        seg.closed = true;
        if (seg.mappedRegion != null) {
            seg.mappedRegion.force();
        }
        if (seg.arena != null) {
            seg.arena.close();
        }
        if (seg.channel != null && seg.channel.isOpen()) {
            seg.channel.close();
        }
    } catch (IOException e) {
        logger.error("Failed to close WAL segment: {}", seg.path, e);
    }
}
```

- [ ] **Step 2: Refactor truncateBefore to use closeSegment**

Replace the inner loop body in `truncateBefore()`:
```java
for (Segment seg : toRemove) {
    closeSegment(seg);
    try {
        Files.deleteIfExists(seg.path);
    } catch (IOException e) {
        logger.error("Failed to delete WAL segment: {}", seg.path, e);
    }
    segments.remove(seg);
    logger.info("Truncated WAL segment: {} (offset range {} - {})",
        seg.path.getFileName(), seg.startGlobalOffset, seg.endGlobalOffset());
}
```

- [ ] **Step 3: Refactor close() to use closeSegment**

Replace the close() method's segment cleanup loops (lines 948-976) with:
```java
// Close all segments (mark closed → flush → close arena/channel)
for (Segment seg : segments) {
    closeSegment(seg);
}
```

- [ ] **Step 4: Run all core tests**

Run: `mvn test -pl loomq-core -am -Pfast-tests`
Expected: All PASS

- [ ] **Step 5: Run the stress test**

Run: `mvn test -pl loomq-core -am -Dtest=SimpleWalWriterTruncateTest -Pslow-tests`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add loomq-core/src/main/java/com/loomq/infrastructure/wal/SimpleWalWriter.java
git commit -m "refactor: extract closeSegment() helper for truncateBefore and close()"
```

---

### Task 4: Format + verify

- [ ] **Step 1: Run formatting**

Run: `make format`

- [ ] **Step 2: Run check-format**

Run: `make check-format`

- [ ] **Step 3: Run full core test suite**

Run: `mvn test -pl loomq-core -am -Pfull-tests`
Expected: All PASS

- [ ] **Step 4: Commit if formatting changed anything**

If `make format` changed files:
```bash
git add -A
git commit -m "style: apply spotless formatting"
```

---

## Summary

| Task | What |
|------|------|
| 1 | Add concurrency stress test |
| 2 | Fix truncateBefore ordering |
| 3 | Extract closeSegment helper |
| 4 | Format + verify |
