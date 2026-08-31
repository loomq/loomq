package com.loomq.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import org.junit.jupiter.api.Test;

/**
 * 幂等记录归属回归：delete / checkIdempotency 不得误删同 key 新 intent 的幂等记录。
 *
 * <p>记录 map 按 key 覆盖写入（createIntent 不做幂等检查，同 key 可被后写者占用）。
 * 修复前 delete() 按 key 无条件 remove：删除旧 intent 会连带摘掉新 intent 的幂等记录，
 * 使其幂等保证失效——后续同 key 请求被当作新请求，产生重复业务写入。</p>
 */
class IdempotencyRecordOwnershipTest {

    @Test
    void deleteMustNotRemoveAnotherIntentsRecord() {
        ConcurrentIntentStore store = new ConcurrentIntentStore();
        Intent a = new Intent("owner-a");
        a.setIdempotencyKey("key-shared");
        store.save(a);

        // 同 key 第二个 intent（createIntent 不做幂等检查，记录按 key 覆盖写 → 现归 B）
        Intent b = new Intent("owner-b");
        b.setIdempotencyKey("key-shared");
        store.save(b);

        // 删除 A：不得误删 B 的记录（修复前按 key 无条件 remove → B 的幂等保证失效）
        store.delete("owner-a");

        IdempotencyResult r = store.checkIdempotency("key-shared");
        assertFalse(r.isAllowed(), "key-shared must still be recognized as a duplicate (owned by B)");
        assertTrue(r.isDuplicateActive(), "record must still be active for intent B");
        assertEquals("owner-b", r.getIntent().getIntentId(),
            "the surviving record must belong to B, not the deleted A");
    }

    @Test
    void deleteStillRemovesOwnRecordWhenNoUsurper() {
        ConcurrentIntentStore store = new ConcurrentIntentStore();
        Intent a = new Intent("owner-a");
        a.setIdempotencyKey("key-alone");
        store.save(a);

        store.delete("owner-a");

        IdempotencyResult r = store.checkIdempotency("key-alone");
        assertTrue(r.isAllowed(), "own record must be removed with its intent");
    }
}
