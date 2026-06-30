package com.loomq.raft;

/**
 * Result of an AppendEntries RPC.
 *
 * On success: {@code success=true, matchIndex=last index written}.
 * On failure: {@code success=false}. If {@code conflictIndex >= 0},
 * the leader can skip directly to that index for log reconciliation
 * instead of decrementing one-by-one (Raft §5.3 optimization).
 */
public class AppendEntriesResult {
    public final boolean success;
    public final long epoch;
    public final long matchIndex;
    /** Index of the first conflicting entry, or -1 if unknown. */
    public final long conflictIndex;

    private AppendEntriesResult(boolean s, long t, long m, long c) {
        success = s; epoch = t; matchIndex = m; conflictIndex = c;
    }

    public static AppendEntriesResult success(long epoch, long matchIndex) {
        return new AppendEntriesResult(true, epoch, matchIndex, -1);
    }

    public static AppendEntriesResult fail(long epoch) {
        return new AppendEntriesResult(false, epoch, -1, -1);
    }

    /** Fail with a known conflict index for efficient log backtracking. */
    public static AppendEntriesResult fail(long epoch, long conflictIndex) {
        return new AppendEntriesResult(false, epoch, -1, conflictIndex);
    }
}
