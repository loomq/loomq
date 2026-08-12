package com.loomq.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import org.junit.jupiter.api.Test;

/**
 * 幂等 key 变更归属回归：upsert 变更 idempotencyKey 时不得误删同 key 新 intent 的记录。
 *
 * <p>记录 map 按 key 覆盖写入（createIntent 不做幂等检查，同 key 可被后写者占用）。
 * 修复前 upsertInternal 在 key 变更时按 key 无条件 remove：A 变更 key 会连带摘掉
 * 已归 B 的记录，使 B 的幂等保证失效——后续同 key 请求被当作新请求，重复业务写入。
 * 与 round 1 修复的 delete()/checkIdempotency() 归属校验同旨，本测试补 key 变更路径。</p>
 *
 * <p>注：key 变更需经"同 id 新实例"触发（createIntent 同 id 二次创建即此形态）；
 * 原地 mutate 同一对象会使 previousKey==newKey，remove 分支不触发。</p>
 */
class BugIdempotencyKeyChangeTest {

    @Test
    void upsertKeyChangeMustNotRemoveAnotherIntentsRecord() {
        ConcurrentIntentStore store = new ConcurrentIntentStore();

        Intent a = new Intent("owner-a");
        a.setIdempotencyKey("key-shared");
        store.save(a);

        // 同 key 第二个 intent（createIntent 不做幂等检查，记录按 key 覆盖写 → 现归 B）
        Intent b = new Intent("owner-b");
        b.setIdempotencyKey("key-shared");
        store.save(b);

        // 同 id 二次创建：新实例 + 不同 key（修复前：按 key 无条件 remove → B 的记录被误删）
        Intent a2 = new Intent("owner-a");
        a2.setIdempotencyKey("key-other");
        store.upsert(a2);

        IdempotencyResult r = store.checkIdempotency("key-shared");
        assertFalse(r.isAllowed(), "key-shared 必须仍被识别为重复（归 B 所有）");
        assertTrue(r.isDuplicateActive(), "记录必须仍为 B 的活跃记录");
        assertEquals("owner-b", r.getIntent().getIntentId(), "幸存记录必须属于 B，而非已变更 key 的 A");
    }

    @Test
    void upsertKeyChangeStillRemovesOwnRecordWhenNoUsurper() {
        ConcurrentIntentStore store = new ConcurrentIntentStore();

        Intent a = new Intent("owner-a");
        a.setIdempotencyKey("key-alone");
        store.save(a);

        Intent a2 = new Intent("owner-a");
        a2.setIdempotencyKey("key-other");
        store.upsert(a2);

        assertTrue(store.checkIdempotency("key-alone").isAllowed(),
            "无 usurper 时自己的旧记录随 key 变更移除");
    }
}
