package com.loomq.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.loomq.domain.intent.PrecisionTier;
import org.junit.jupiter.api.Test;

/**
 * IntentTraceStore 淘汰队列语义测试。
 *
 * <p>techdebt D3:10 万条上限的 EvictionQueue 淘汰逻辑此前零直接测试——淘汰路径
 * (满则丢最旧)与同 id 重写不膨胀队列从未执行断言。</p>
 */
class IntentTraceStoreTest {

    @Test
    void evictionDropsOldestWhenFull() {
        IntentTraceStore store = new IntentTraceStore(3);
        store.recordCreated("evict-1", "t1", PrecisionTier.STANDARD, 100);
        store.recordCreated("evict-2", "t2", PrecisionTier.STANDARD, 200);
        store.recordCreated("evict-3", "t3", PrecisionTier.STANDARD, 300);
        store.recordCreated("evict-4", "t4", PrecisionTier.STANDARD, 400);
        store.recordCreated("evict-5", "t5", PrecisionTier.STANDARD, 500);

        // 容量封顶 3,最旧的 evict-1/evict-2 被淘汰
        assertNull(store.get("evict-1"), "最旧条目必须被淘汰");
        assertNull(store.get("evict-2"), "第二旧条目必须被淘汰");
        assertNotNull(store.get("evict-3"));
        assertNotNull(store.get("evict-4"));
        assertNotNull(store.get("evict-5"));
    }

    @Test
    void sameIdRewriteDoesNotGrowQueue() {
        IntentTraceStore store = new IntentTraceStore(2);
        store.recordCreated("rewrite-1", "t1", PrecisionTier.FAST, 100);
        store.recordCreated("rewrite-2", "t2", PrecisionTier.FAST, 200);
        // 同 id 重写:put 覆盖已有条目,不新增队列项(FIFO 位置不移动——非真 LRU)
        store.recordCreated("rewrite-1", "t1b", PrecisionTier.FAST, 300);
        store.recordCreated("rewrite-3", "t3", PrecisionTier.FAST, 400);

        // 队列仍只含 3 个唯一 id;FIFO 头是首个加入的 rewrite-1(重写不移位)→ 被淘汰
        assertNull(store.get("rewrite-1"), "FIFO 头(最早加入)被淘汰,重写不改变位置");
        assertNotNull(store.get("rewrite-2"));
        assertNotNull(store.get("rewrite-3"));
        assertEquals("t2", store.get("rewrite-2").traceId());
    }
}
