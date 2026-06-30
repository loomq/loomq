package com.loomq.raft;

/**
 * 日志复制模式。
 *
 * <p>ASYNC: Leader 写入本地 WAL 后立即返回，后台异步复制到 Followers。
 * 默认模式，写入延迟最低，但 Leader 崩溃可能丢失最后一批未复制的日志。
 *
 * <p>SYNC: Leader 写入本地 WAL 后，等待多数节点确认后才返回。
 * 数据可靠性最高，但写入延迟较高。
 */
public enum ReplicationMode {
    ASYNC,
    SYNC
}
