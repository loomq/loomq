package com.loomq.raft;

import com.loomq.common.RaftRole;
import com.loomq.domain.intent.Intent;

/**
 * Hooks from the Raft layer back into the local runtime.
 *
 * The runtime uses these callbacks to keep scheduler state aligned with the
 * current Raft role and to reflect committed intents into the active leader's
 * in-memory dispatch structures.
 */
public interface RaftRuntimeListener {

    /**
     * Called whenever the node's visible Raft role changes, including initial
     * startup.
     */
    default void onRoleChanged(RaftRole role, long epoch) {}

    /**
     * Called after a committed intent snapshot has been applied to the local store.
     *
     * @param intent the committed intent snapshot
     * @param isNew whether this intent did not previously exist in the local store
     * @param leader whether this node is currently the Raft leader
     */
    default void onCommittedIntent(Intent intent, boolean isNew, boolean leader) {}
}
