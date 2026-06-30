# Raft Module Code Review Fixes — Design Spec

**Date:** 2026-06-03
**Scope:** 15 findings from code review (commit c397fbd0 → HEAD)
**Approach:** Fix + Refactor + Tests, 5 PRs by subsystem in dependency order

## Findings Summary

| # | Severity | File | Issue |
|---|----------|------|-------|
| 1 | Critical | SimpleWalWriter:592 | Use-after-free: arena.close() before seg.closed=true |
| 2 | Critical | LoomqServerApplication:177 | StandaloneElection default for raft.enabled → split-brain |
| 3 | Critical | GrpcRaftTransport:384 | InstallSnapshot buffers ~5GB before epoch check |
| 4 | Critical | LogReplication:212 | applyCommitted break leaves later waiters stuck forever |
| 5 | High | K8sLeaseElection:193 | stepDown → becomeLeader in same tryAcquireLease call |
| 6 | High | K8sLeaseElection:145 | onAppendEntries missing stopped guard |
| 7 | High | RaftNode:379 | Store dirty before compactThrough failure |
| 8 | High | PlatformThreadIntentStore:155 | No awaitTermination before delegate.shutdown() |
| 9 | Medium | GrpcRaftTransport:235 | sendInstallSnapshot no backpressure |
| 10 | Medium | ColdIntentSwapper:186 | isCold() ignores cancelledColdIds |
| 11 | Medium | InMemoryRaftTransport:67 | connectedPeerCount()=0 contradicts isPeerConnected()=true |
| 12 | Medium | K8sLeaseElection:168 | Listeners overwrite instead of accumulate |
| 13 | Medium | StandaloneElection:81 | addBecomeFollowerListener is no-op |
| 14 | Medium | GrpcRaftTransport:117 | connect() stub-channel TOCTOU |
| 15 | Medium | RaftWriteCoordinator:270 | intentLock unlock-then-remove race |

---

## PR1: K8sLeaseElection (#5, #6, #12)

### Bug Fixes

**#5 stepDown race in tryAcquireLease:**
- Add `return;` after `stepDown(currentEpoch)` in `tryAcquireLease()` line 193.
- The method exits after stepping down rather than continuing to attempt lease acquisition.
- This prevents the spurious FOLLOWER→LEADER transition where the K8s lease wall-clock hasn't expired but the monotonic clock has.

**#6 missing stopped guard in onAppendEntries:**
- Add `if (stopped) return;` at the top of `onAppendEntries()`.
- Keep the `if (leaderEpoch >= currentEpoch)` guard — only higher-epoch AppendEntries trigger role transitions.
- Inside the guard, delegate to `stepDown(leaderEpoch)` instead of duplicating role-change logic inline.
- `stepDown()` already delegates to the shared `transitionRole` helper (see refactor below).

**#12 listener overwrite:**
- Change `onBecomeLeader`/`onBecomeFollower` from `volatile Consumer<Long>` to `CopyOnWriteArrayList<Consumer<Long>>`.
- `addBecomeLeaderListener` / `addBecomeFollowerListener` add to the list.
- `transitionRole` iterates and invokes all listeners.

### Refactor

Extract `transitionRole(RaftRole target, String leader, long epoch)`:
- Checks `if (stopped) return;`
- Computes `safeEpoch = Math.max(epoch, currentEpoch)`
- Guards on `role != target`
- Updates `role`, `currentLeader`, `currentEpoch`
- Calls `persistEpoch(safeEpoch)`
- Logs the transition
- Notifies all listeners in the list
- In the `else` branch (already in target role): updates leader and conditionally updates epoch

Delegation chain:
- `stepDown(newEpoch)` → `transitionRole(FOLLOWER, null, newEpoch)` + null-leader handling
- `becomeLeader(epoch)` → `transitionRole(LEADER, config.podName(), epoch)`
- `becomeFollower(leader, epoch)` → `transitionRole(FOLLOWER, leader, epoch)`
- `onAppendEntries(leaderEpoch, leaderId)` → keeps `if (leaderEpoch >= currentEpoch)`, then calls `stepDown(leaderEpoch)` or updates leader only

### Tests

- `onAppendEntriesAfterStopShouldBeNoOp()` — verify no role change or callback after stop
- `stepDownThenContinueShouldNotReacquire()` — verify tryAcquireLease exits after stepDown
- `multipleListenersShouldAllFire()` — verify both listeners are invoked

---

## PR2: WAL/SimpleWalWriter (#1)

### Bug Fix

**#1 use-after-free race in truncateBefore:**
- In `truncateBefore()` line 592-593, swap the order: set `seg.closed = true` BEFORE `seg.arena.close()`.
- This matches the correct pattern already used in `close()` (lines 949-976).

### Refactor

Extract `closeSegment(Segment seg)` private helper:
- `seg.closed = true`
- `seg.mappedRegion.force()` (if non-null)
- `seg.arena.close()` (if non-null)
- `seg.channel.close()` (if open)

Both `truncateBefore()` and `close()` call this helper, eliminating duplication and preventing future ordering bugs.

### Tests

- `truncateBeforeShouldNotCrashDuringConcurrentFlush()` — stress test: start flushLoop thread, call truncateBefore on a segment, verify no crash or IllegalStateException

---

## PR3: GrpcRaftTransport (#3, #9, #14)

### Bug Fixes

**#3 InstallSnapshot memory exhaustion:**
- Add `Supplier<Long> currentEpochSupplier` field to `GrpcRaftTransport`.
- Set it during initialization (same path as `setOnInstallSnapshot` — RaftNode passes `election::currentEpoch`).
- In `onNext()`, on the first chunk: `if (currentEpochSupplier.get() > chunk.getEpoch())` → reject immediately with `errored.set(true)` + `responseObserver.onError(...)`.
- Add `MAX_SNAPSHOT_BYTES = 256 * 1024 * 1024` constant. On first chunk: `if ((long) totalChunks * SNAPSHOT_CHUNK_SIZE > MAX_SNAPSHOT_BYTES)` → reject.
- This limits peak memory to ~256MB instead of ~5GB.

**#9 sendInstallSnapshot backpressure:**
- Replace the tight `for` loop with `isReady()` + `setOnReadyHandler` pattern:
  ```java
  void sendChunk(int i) {
      if (i >= totalChunks) { requestObserver.onCompleted(); return; }
      requestObserver.onNext(chunk[i]);
      if (requestObserver.isReady()) {
          sendChunk(i + 1);  // continue immediately
      } else {
          requestObserver.setOnReadyHandler(() -> sendChunk(i + 1));
      }
  }
  ```
- Non-blocking, respects gRPC flow control. Deadline protects against hangs.
- Note: `setOnReadyHandler` must be called before the first `onNext` or after the stream becomes not-ready. Need to handle the initial call correctly.

**#14 connect() stub-channel race:**
- Make the channel+stubs update atomic. Use a lock or `ConcurrentHashMap.compute()`:
  ```java
  synchronized (connectionLock) {
      ManagedChannel existing = channels.put(peerId, newChannel);
      if (existing != null) existing.shutdown();
      blockingStubs.put(peerId, newBlockingStub(newChannel));
      asyncStubs.put(peerId, newStub(newChannel));
  }
  ```

### Refactor

Extract `gracefulShutdown(ExecutorService svc, String name)` private helper:
- `svc.shutdown()`
- `if (!svc.awaitTermination(5, SECONDS)) svc.shutdownNow()`
- Handle `InterruptedException`

Extract `gracefulShutdownServer(Server server)` for the gRPC server.

Both used in `close()` — replaces 4x copy-pasted shutdown blocks.

### Tests

- `installSnapshotShouldRejectStaleEpochEarly()` — send chunks with stale epoch, verify rejection before all chunks buffered
- `installSnapshotShouldEnforceMaxSize()` — send totalChunks exceeding size limit, verify rejection
- `connectShouldBeAtomicWithDisconnect()` — concurrent connect+disconnect, verify no null stubs

---

## PR4: LogReplication + RaftNode (#4, #7, #2)

### Bug Fixes

**#4 applyCommitted stuck waiters:**
- When entry N's upsert fails, fail waiters for ALL entries from N to `commitIndex` (not just N). Then `break`.
- This preserves Raft's sequential application guarantee while ensuring no waiter hangs.
- Implementation: after `failAppliedWaiter(applied, e)`, add loop: `for (long i = applied + 1; i <= committed; i++) failAppliedWaiter(i, new IllegalStateException("Skipped due to failure at index " + applied, e));` then break.
- Add per-index retry counter (`Map<Long, Integer>`). If entry N fails 3 consecutive times, log critical error ("store may be corrupted") and fail all remaining waiters with descriptive exception. Prevents infinite retry on permanently broken store.

**#7 store dirty before compactThrough:**
- Move `raftLog.compactThrough()` inside the `synchronized(replication)` block (after `resetToSnapshot`, before releasing the lock).
- The original comment claimed deadlock risk (compactThrough synchronized on RaftLog, another thread holding RaftLog waiting for replication lock). Examining the code paths: `handleAppendEntries` → `raftLog.appendEntry()` is called BEFORE `applyCommitted()`, and `applyCommitted()` is the method that acquires the replication lock. There is no lock-order inversion. Move compactThrough inside the lock to make store update + WAL compaction atomic.
- If a deadlock scenario is later discovered, use a `volatile boolean snapshotInProgress` flag that `applyCommitted` checks before reading entries.

**#2 StandaloneElection split-brain:**
- **Option A (chosen):** Remove the `raft.enabled=true` backward-compatible path from `LoomqServerApplication`.
- The old `RaftElection` class (vote-based election) was deleted in this PR. The `raft.enabled=true` path now defaults to `StandaloneElection`, which self-elects all nodes as LEADER — a silent split-brain.
- Remove the `else if (raft.enabled)` branch (lines 152-197). Users should use `mode=distributed` with K8s Lease.
- Add a startup warning: if `LOOMQ_RAFT_ENABLED=true` is detected, log an error with migration instructions and refuse to start.

### Tests

- `applyCommittedShouldFailAllSubsequentWaiters()` — commit entries 1,2,3; make entry 2's upsert fail; verify entries 2 AND 3's waiters are failed (entry 1 applied OK)
- `applyCommittedShouldRetryBeforeGivingUp()` — verify entry N is retried up to 3 times before permanent failure
- `handleInstallSnapshotShouldBeAtomic()` — verify store and WAL are consistent after snapshot application (no window where store has snapshot data but WAL has old entries)
- `raftEnabledWithMultiplePeersShouldFail()` — verify clear error message when misconfigured

---

## PR5: Core + Tests (#8, #10, #11, #13, #15)

### Bug Fixes

**#8 PlatformThreadIntentStore shutdown:**
- Add `platformExecutor.awaitTermination(5, TimeUnit.SECONDS)` between `shutdown()` and `delegate.shutdown()`.
- If timeout, call `shutdownNow()` and log a warning.
- This ensures in-flight operations complete before the delegate is closed.

**#10 isCold() Javadoc mismatch:**
- Fix `isCold()` to check `cancelledColdIds.contains(intentId)` — if true, return false.
- This matches the Javadoc: "检查指定 Intent 是否在冷索引中（不含已取消的）".

**#11 InMemoryRaftTransport.connectedPeerCount():**
- Return the actual connected peer count instead of hardcoded 0.
- If there's no explicit connected set, compute from the peer list: `return Math.max(0, peerIds.size() - 1)` (excluding self) to match `isPeerConnected()` always returning true.

**#13 StandaloneElection no-ops:**
- `addBecomeFollowerListener`: store the listener in a field (even though standalone never becomes follower during normal operation, `stop()` semantics may need it in the future).
- `stepDown`: log a warning ("StandaloneElection.stepDown called — ignoring, single-node mode"). A silent no-op hides misconfiguration.

**#15 intentLock race:**
- After `lock.unlock()` in the finally block, use `ConcurrentHashMap.remove(key, lock)` (the 2-arg overload that only removes if the value matches):
  ```java
  lock.unlock();
  if (!lock.isLocked()) {
      intentLocks.remove(intentId, lock);  // only removes if it's the same lock
  }
  ```
- This prevents deletion of a lock that was acquired by another thread between unlock and remove.

### Tests

- `shutdownShouldAwaitInFlightOperations()` — submit an operation, call shutdown concurrently, verify operation completes before delegate closes
- `isColdShouldReturnFalseForCancelled()` — cancel a cold intent, verify isCold returns false
- `connectedPeerCountShouldMatchIsPeerConnected()` — verify consistency
- `intentLockShouldNotBeRemovedWhileHeld()` — concurrent mutations on same intent, verify no lock corruption
- `standaloneElectionStepDownShouldLog()` — verify warning log on stepDown attempt

---

## PR Order

1. **PR1: K8sLeaseElection** — foundational election fixes, no external dependencies
2. **PR2: WAL/SimpleWalWriter** — isolated WAL fix, no cross-module impact
3. **PR3: GrpcRaftTransport** — transport layer fixes, depends on election interface from PR1
4. **PR4: LogReplication + RaftNode** — replication correctness, depends on transport from PR3
5. **PR5: Core + Tests** — peripheral fixes and test coverage, lowest risk
