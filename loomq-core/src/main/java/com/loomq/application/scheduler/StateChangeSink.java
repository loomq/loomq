package com.loomq.application.scheduler;

import com.loomq.domain.intent.Intent;

/**
 * 状态变更持久化通道：把"非阻塞 put"与"阻塞等待落盘"分离，使调度器可在
 * synchronized(intent) 内只做 put、在锁外 awaitCommit（VT 不再 pin carrier）。
 *
 * <p>根因：VT 在 synchronized 块内 park 无法 unmount，会 pin 住 carrier；把阻塞的
 * awaitCommit 移到锁外，VT 在 LockSupport.park 上正常 unmount。</p>
 */
public interface StateChangeSink {
    /** 非阻塞：写 PHTW + 索引，不等待落盘。须在 synchronized(intent) 内调用以保持 I2/I3 原子性。 */
    void persist(Intent intent);
    /** 非阻塞：终态原地覆写最新槽(不追加;无索引/tail 时回退追加)；须在 synchronized(intent) 内调用。 */
    void persistTerminalInPlace(Intent intent);
    /** 阻塞到持久化完成；仅在 synchronized(intent) 之外调用（VT 可正常 unmount）。 */
    void awaitCommit();
    /** 终态槽回收（单槽清空入 free-list；多槽保留 tombstone）；仅在 awaitCommit 之后调用。 */
    void reclaimTerminal(String intentId);
}
