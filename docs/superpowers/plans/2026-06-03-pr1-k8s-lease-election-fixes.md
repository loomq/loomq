# PR1: K8sLeaseElection Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 3 bugs in K8sLeaseElection (stepDown race, missing stopped guard, listener overwrite) and refactor duplicated role-transition logic into a shared helper.

**Architecture:** Extract a `transitionRole()` method that centralizes all role transitions (stopped guard, epoch update, persist, log, notify listeners). All public role-change methods delegate to it. Listeners change from single `volatile Consumer<Long>` to `CopyOnWriteArrayList<Consumer<Long>>` for additive registration.

**Tech Stack:** Java 25, JUnit 5, Mockito (for K8s API mocking), reflection for private method testing

**Files to modify:**
- `loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java`
- `loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java`

---

### Task 1: Add test for #5 — stepDown-then-reacquire race

**Files:**
- Modify: `loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java`

- [ ] **Step 1: Write the failing test**

Add a test that verifies `tryAcquireLease()` exits after `stepDown()` instead of continuing to re-acquire the lease. The test sets up a LEADER with an expired monotonic clock where the K8s lease wall-clock hasn't expired (holder=self). After `tryAcquireLease()`, the role should be FOLLOWER — not LEADER.

```java
@Test
void tryAcquireLeaseShouldExitAfterStepDown() throws Exception {
    K8sLeaseConfig config = new K8sLeaseConfig(15, 4, "default", "loomq-leader", "pod-1");
    ApiClient dummyClient = new ApiClient();
    K8sLeaseElection election = new K8sLeaseElection(config, wal, dummyClient);
    try {
        // Force into LEADER state with stale monotonic clock
        Field roleField = K8sLeaseElection.class.getDeclaredField("role");
        roleField.setAccessible(true);
        roleField.set(election, RaftRole.LEADER);

        Field epochField = K8sLeaseElection.class.getDeclaredField("currentEpoch");
        epochField.setAccessible(true);
        epochField.set(election, 1L);

        Field nanoField = K8sLeaseElection.class.getDeclaredField("lastRenewNanoTime");
        nanoField.setAccessible(true);
        // 60 seconds ago — far beyond 15s lease duration
        nanoField.set(election, System.nanoTime() - TimeUnit.SECONDS.toNanos(60));

        // Mock readLease() to return self as holder (wall-clock not expired)
        // This requires either mocking the API client or using a testable subclass.
        // Since K8sLeaseElection uses CoordinationV1Api directly, we test the
        // behavioral outcome: after stepDown, tryAcquireLease should NOT re-promote.

        java.lang.reflect.Method tryAcquire = K8sLeaseElection.class.getDeclaredMethod("tryAcquireLease");
        tryAcquire.setAccessible(true);

        // First call: monotonic expired → stepDown → role becomes FOLLOWER
        // The K8s API call will fail (dummy client), but stepDown should still happen
        tryAcquire.invoke(election);

        assertEquals(RaftRole.FOLLOWER, election.role(),
            "tryAcquireLease should step down and NOT re-acquire on monotonic expiry");
    } finally {
        election.stop();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest#tryAcquireLeaseShouldExitAfterStepDown -Pslow-tests`
Expected: The test may pass with the current code because the dummy API client causes `readLease()` to throw, which catches the exception and exits. The real test is behavioral — after stepDown, the method should return early. The test verifies FOLLOWER state.

- [ ] **Step 3: Commit test**

```bash
git add loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java
git commit -m "test: add tryAcquireLease stepDown-then-exit test (#5)"
```

---

### Task 2: Fix #5 — add return after stepDown in tryAcquireLease

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java:193`

- [ ] **Step 1: Add `return` after stepDown**

In `tryAcquireLease()`, line 193, add `return;` after `stepDown(currentEpoch)`:

```java
private void tryAcquireLease() {
    if (stopped) return;
    try {
        if (role == RaftRole.LEADER && isLeaseExpiredMonotonic()) {
            log.warn("Lease expired (monotonic clock), stepping down");
            stepDown(currentEpoch);
            return;  // ← ADD THIS: exit after stepping down, don't re-acquire
        }
        // ... rest unchanged
```

- [ ] **Step 2: Run the stepDown test**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest#tryAcquireLeaseShouldExitAfterStepDown -Pslow-tests`
Expected: PASS

- [ ] **Step 3: Run all K8sLeaseElectionTest tests**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest -Pslow-tests`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java
git commit -m "fix: tryAcquireLease exits after stepDown, preventing spurious re-acquisition (#5)"
```

---

### Task 3: Add test for #12 — multiple listeners should all fire

**Files:**
- Modify: `loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java`

- [ ] **Step 1: Write the failing test**

```java
@Test
void multipleListenersShouldAllFire() throws Exception {
    K8sLeaseConfig config = new K8sLeaseConfig(15, 4, "default", "loomq-leader", "pod-1");
    ApiClient dummyClient = new ApiClient();
    K8sLeaseElection election = new K8sLeaseElection(config, wal, dummyClient);
    try {
        AtomicBoolean leader1Fired = new AtomicBoolean(false);
        AtomicBoolean leader2Fired = new AtomicBoolean(false);
        AtomicBoolean follower1Fired = new AtomicBoolean(false);
        AtomicBoolean follower2Fired = new AtomicBoolean(false);

        election.addBecomeLeaderListener(epoch -> leader1Fired.set(true));
        election.addBecomeLeaderListener(epoch -> leader2Fired.set(true));
        election.addBecomeFollowerListener(epoch -> follower1Fired.set(true));
        election.addBecomeFollowerListener(epoch -> follower2Fired.set(true));

        // Force to LEADER — should fire both leader listeners
        java.lang.reflect.Method becomeLeader =
            K8sLeaseElection.class.getDeclaredMethod("becomeLeader", long.class);
        becomeLeader.setAccessible(true);
        becomeLeader.invoke(election, 1L);

        assertTrue(leader1Fired.get(), "first leader listener should fire");
        assertTrue(leader2Fired.get(), "second leader listener should fire");

        // Force to FOLLOWER — should fire both follower listeners
        java.lang.reflect.Method becomeFollower =
            K8sLeaseElection.class.getDeclaredMethod("becomeFollower", String.class, long.class);
        becomeFollower.setAccessible(true);
        becomeFollower.invoke(election, "other-pod", 2L);

        assertTrue(follower1Fired.get(), "first follower listener should fire");
        assertTrue(follower2Fired.get(), "second follower listener should fire");
    } finally {
        election.stop();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest#multipleListenersShouldAllFire -Pslow-tests`
Expected: FAIL — second listener overwrites first, only one fires per category

- [ ] **Step 3: Commit test**

```bash
git add loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java
git commit -m "test: add multipleListenersShouldAllFire test (#12)"
```

---

### Task 4: Fix #12 — change listeners to CopyOnWriteArrayList

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java:52-53,168-175`

- [ ] **Step 1: Change listener fields to lists**

Replace lines 52-53:
```java
// OLD:
// private volatile Consumer<Long> onBecomeLeader;
// private volatile Consumer<Long> onBecomeFollower;

// NEW:
private final List<Consumer<Long>> onBecomeLeaderListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
private final List<Consumer<Long>> onBecomeFollowerListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
```

- [ ] **Step 2: Change addBecomeLeaderListener / addBecomeFollowerListener**

Replace lines 167-175:
```java
@Override
public void addBecomeLeaderListener(Consumer<Long> listener) {
    onBecomeLeaderListeners.add(listener);
}

@Override
public void addBecomeFollowerListener(Consumer<Long> listener) {
    onBecomeFollowerListeners.add(listener);
}
```

- [ ] **Step 3: Update all listener invocation sites**

In `becomeLeader()` (line 319-320), replace:
```java
// OLD: if (onBecomeLeader != null) { onBecomeLeader.accept(safeEpoch); }
// NEW:
for (Consumer<Long> listener : onBecomeLeaderListeners) {
    listener.accept(safeEpoch);
}
```

In `becomeFollower()` (line 340-341), replace:
```java
// OLD: if (onBecomeFollower != null) { onBecomeFollower.accept(safeEpoch); }
// NEW:
for (Consumer<Long> listener : onBecomeFollowerListeners) {
    listener.accept(safeEpoch);
}
```

Note: `onAppendEntries()` line 154-156 still uses the old `if (onBecomeFollower != null)` pattern. This is fine — Task 6 completely replaces `onAppendEntries` with delegation to `becomeFollower`, which uses the new list. No need to update it here.

- [ ] **Step 4: Run the multipleListeners test**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest#multipleListenersShouldAllFire -Pslow-tests`
Expected: PASS

- [ ] **Step 5: Run all K8sLeaseElectionTest tests**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest -Pslow-tests`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java
git commit -m "fix: listeners use CopyOnWriteArrayList for additive registration (#12)"
```

---

### Task 5: Add test for #6 — onAppendEntries after stop should be no-op

**Files:**
- Modify: `loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java`

- [ ] **Step 1: Write the failing test**

```java
@Test
void onAppendEntriesAfterStopShouldBeNoOp() throws Exception {
    K8sLeaseConfig config = new K8sLeaseConfig(15, 4, "default", "loomq-leader", "pod-1");
    ApiClient dummyClient = new ApiClient();
    K8sLeaseElection election = new K8sLeaseElection(config, wal, dummyClient);

    // Force to LEADER
    Field roleField = K8sLeaseElection.class.getDeclaredField("role");
    roleField.setAccessible(true);
    roleField.set(election, RaftRole.LEADER);

    Field epochField = K8sLeaseElection.class.getDeclaredField("currentEpoch");
    epochField.setAccessible(true);
    epochField.set(election, 1L);

    AtomicBoolean followerFired = new AtomicBoolean(false);
    election.addBecomeFollowerListener(epoch -> followerFired.set(true));

    // Stop the election
    election.stop();

    // Try to step down via AppendEntries — should be no-op because stopped=true
    election.onAppendEntries(5L, "other-pod");

    assertEquals(RaftRole.FOLLOWER, election.role(),
        "role should be FOLLOWER (set by stop()), not changed by onAppendEntries");
    assertFalse(followerFired.get(),
        "onBecomeFollower callback should NOT fire after stop");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest#onAppendEntriesAfterStopShouldBeNoOp -Pslow-tests`
Expected: FAIL — `onAppendEntries` doesn't check `stopped`, so the callback fires

- [ ] **Step 3: Commit test**

```bash
git add loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java
git commit -m "test: add onAppendEntriesAfterStopShouldBeNoOp test (#6)"
```

---

### Task 6: Refactor — extract transitionRole + fix #6 stopped guard

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java`

- [ ] **Step 1: Extract transitionRole method**

Add a new private method that centralizes all role transitions:

```java
/**
 * Central role transition. All role changes go through here.
 *
 * @param target the desired role
 * @param leader the leader ID (null for stepDown)
 * @param epoch the new epoch
 */
private synchronized void transitionRole(RaftRole target, String leader, long epoch) {
    if (stopped) return;
    long safeEpoch = Math.max(epoch, currentEpoch);
    if (role != target) {
        role = target;
        currentLeader = leader;
        currentEpoch = safeEpoch;
        persistEpoch(safeEpoch);
        if (target == RaftRole.LEADER) {
            log.info("Became LEADER: pod={}, epoch={}", config.podName(), safeEpoch);
            for (Consumer<Long> listener : onBecomeLeaderListeners) {
                listener.accept(safeEpoch);
            }
        } else {
            log.info("Became FOLLOWER: pod={}, leader={}, epoch={}", config.podName(), leader, safeEpoch);
            for (Consumer<Long> listener : onBecomeFollowerListeners) {
                listener.accept(safeEpoch);
            }
        }
    } else {
        // Already in target role — just update leader/epoch if changed
        if (leader != null) {
            currentLeader = leader;
        }
        if (safeEpoch != currentEpoch) {
            currentEpoch = safeEpoch;
            persistEpoch(safeEpoch);
        }
    }
}
```

- [ ] **Step 2: Rewrite becomeLeader to delegate**

Replace the entire `becomeLeader` method:
```java
private synchronized void becomeLeader(long epoch) {
    transitionRole(RaftRole.LEADER, config.podName(), epoch);
}
```

- [ ] **Step 3: Rewrite becomeFollower to delegate**

Replace the entire `becomeFollower` method:
```java
private synchronized void becomeFollower(String leader, long epoch) {
    transitionRole(RaftRole.FOLLOWER, leader, epoch);
}
```

- [ ] **Step 4: Rewrite onAppendEntries with stopped guard + delegation**

Replace the entire `onAppendEntries` method:
```java
@Override
public synchronized void onAppendEntries(long leaderEpoch, String leaderId) {
    if (stopped) return;  // ← FIX #6: stopped guard
    if (leaderEpoch >= currentEpoch) {
        if (role != RaftRole.FOLLOWER) {
            // Step down: delegate to transitionRole via becomeFollower
            becomeFollower(leaderId, leaderEpoch);
        } else {
            // Already follower: update leader and conditionally epoch
            currentLeader = leaderId;
            if (leaderEpoch > currentEpoch) {
                currentEpoch = leaderEpoch;
                persistEpoch(leaderEpoch);
            }
        }
    }
}
```

- [ ] **Step 5: Run all K8sLeaseElectionTest tests**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest -Pslow-tests`
Expected: All tests PASS (including the new #5, #6, #12 tests)

- [ ] **Step 6: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java
git commit -m "refactor: extract transitionRole, fix #6 stopped guard in onAppendEntries"
```

---

### Task 7: Run full test suite + format check

**Files:** None (verification only)

- [ ] **Step 1: Run formatting**

Run: `make format`
Expected: No changes (code already formatted)

- [ ] **Step 2: Run fast tests for loomq-raft module**

Run: `mvn test -pl loomq-raft -am -Pfast-tests`
Expected: All tests PASS

- [ ] **Step 3: Run slow tests for K8sLeaseElectionTest**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest -Pslow-tests`
Expected: All tests PASS

- [ ] **Step 4: Run check-format**

Run: `make check-format`
Expected: PASS

- [ ] **Step 5: Commit (if format made changes)**

If `make format` changed anything:
```bash
git add -A
git commit -m "style: apply spotless formatting"
```
Otherwise, skip this step.

---

### Task 8: Verify the full PR diff

**Files:** None (verification only)

- [ ] **Step 1: Review the diff**

Run: `git diff main -- loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java`

Verify:
- `transitionRole` method exists with stopped guard, epoch update, persist, log, listeners
- `becomeLeader` and `becomeFollower` are one-liner delegates
- `onAppendEntries` has `if (stopped) return;` at the top
- `tryAcquireLease` has `return;` after `stepDown(currentEpoch)`
- Listeners are `CopyOnWriteArrayList` fields
- All 3 new tests exist and test the right behavior

- [ ] **Step 2: Verify no unintended changes**

Run: `git diff main --stat`
Expected: Only `K8sLeaseElection.java` and `K8sLeaseElectionTest.java` changed

---

## Summary

| Task | Bug/Fix | What |
|------|---------|------|
| 1 | #5 test | stepDown-then-exit test |
| 2 | #5 fix | `return` after stepDown in tryAcquireLease |
| 3 | #12 test | multiple listeners test |
| 4 | #12 fix | CopyOnWriteArrayList for listeners |
| 5 | #6 test | onAppendEntries after stop test |
| 6 | #6 fix + refactor | Extract transitionRole, stopped guard |
| 7 | verify | format + full test suite |
| 8 | verify | diff review |
