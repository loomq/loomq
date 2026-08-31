package com.loomq.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Intent 存储测试
 */
class IntentStoreTest {

    private IntentStore store;

    @BeforeEach
    void setUp() {
        store = new ConcurrentIntentStore();
    }

    @AfterEach
    void tearDown() {
        store.shutdown();
    }

    @Test
    void testSaveAndFind() {
        Intent intent = createTestIntent("test-1");
        store.save(intent);

        Intent found = store.findById("test-1");

        assertNotNull(found);
        assertEquals("test-1", found.getIntentId());
        assertEquals(IntentStatus.SCHEDULED, found.getStatus());
    }

    @Test
    void testFind_NotFound_ReturnsNull() {
        Intent found = store.findById("non-existent");
        assertNull(found);
    }

    @Test
    void testUpdate() {
        Intent intent = createTestIntent("test-2");
        store.save(intent);

        // 修改状态
        intent.transitionTo(IntentStatus.DUE);
        store.update(intent);

        Intent found = store.findById("test-2");
        assertEquals(IntentStatus.DUE, found.getStatus());
        assertEquals(0, store.countByStatus(IntentStatus.SCHEDULED));
        assertEquals(1, store.countByStatus(IntentStatus.DUE));
        assertEquals(1, store.getPendingCount());
    }

    @Test
    void testUpsert() {
        Intent intent = createTestIntent("test-upsert");
        store.upsert(intent);

        assertEquals(1, store.countByStatus(IntentStatus.SCHEDULED));
        assertEquals(1, store.getPendingCount());

        intent.transitionTo(IntentStatus.DUE);
        store.upsert(intent);

        assertEquals(0, store.countByStatus(IntentStatus.SCHEDULED));
        assertEquals(1, store.countByStatus(IntentStatus.DUE));
        assertEquals(1, store.getPendingCount());

        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        intent.transitionTo(IntentStatus.ACKED);
        store.upsert(intent);

        assertEquals(1, store.countByStatus(IntentStatus.ACKED));
        assertEquals(0, store.getPendingCount());
    }

    @Test
    void testDelete() {
        Intent intent = createTestIntent("test-3");
        store.save(intent);

        store.delete("test-3");

        assertNull(store.findById("test-3"));
        assertEquals(0, store.getPendingCount());
        assertEquals(0, store.countByStatus(IntentStatus.SCHEDULED));
    }

    @Test
    void testDelete_WithIdempotencyKey() {
        Intent intent = createTestIntent("test-4");
        intent.setIdempotencyKey("idem-key-1");
        store.save(intent);

        store.delete("test-4");

        // 幂等记录也应该被删除
        IdempotencyResult result = store.checkIdempotency("idem-key-1");
        assertTrue(result.isNotFound());
    }

    @Test
    void testIdempotency_NewRequest() {
        IdempotencyResult result = store.checkIdempotency("new-key");

        assertTrue(result.isNotFound());
        assertTrue(result.isAllowed());
        assertEquals(201, result.getHttpStatus());
    }

    @Test
    void testIdempotency_DuplicateActive() {
        Intent intent = createTestIntent("test-5");
        intent.setIdempotencyKey("idem-active");
        // createTestIntent 已经设置状态为 SCHEDULED
        store.save(intent);

        IdempotencyResult result = store.checkIdempotency("idem-active");

        assertTrue(result.isDuplicateActive());
        assertTrue(result.exists());
        assertFalse(result.isTerminal());
        assertEquals(200, result.getHttpStatus());
        assertEquals("test-5", result.getIntent().getIntentId());
    }

    @Test
    void testIdempotency_DuplicateTerminal() {
        Intent intent = createTestIntent("test-6");
        intent.setIdempotencyKey("idem-terminal");
        // createTestIntent 已经设置状态为 SCHEDULED
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        intent.transitionTo(IntentStatus.ACKED);
        store.save(intent);

        IdempotencyResult result = store.checkIdempotency("idem-terminal");

        assertTrue(result.isDuplicateTerminal());
        assertTrue(result.exists());
        assertTrue(result.isTerminal());
        assertEquals(409, result.getHttpStatus());
    }

    @Test
    void testIdempotency_AfterIntentDeleted() {
        Intent intent = createTestIntent("test-7");
        intent.setIdempotencyKey("idem-deleted");
        store.save(intent);

        // 删除 Intent
        store.delete("test-7");

        // 幂等记录应该也被清理
        IdempotencyResult result = store.checkIdempotency("idem-deleted");
        assertTrue(result.isNotFound());
    }

    @Test
    void testIdempotency_UpdatesWhenIntentBecomesTerminal() {
        Intent intent = createTestIntent("test-8");
        intent.setIdempotencyKey("idem-update");
        // createTestIntent 已经设置状态为 SCHEDULED
        store.save(intent);

        // 此时应该是 active
        IdempotencyResult result1 = store.checkIdempotency("idem-update");
        assertTrue(result1.isDuplicateActive());

        // 转换为终态
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        intent.transitionTo(IntentStatus.ACKED);
        store.update(intent);

        // 现在应该是 terminal
        IdempotencyResult result2 = store.checkIdempotency("idem-update");
        assertTrue(result2.isDuplicateTerminal());
    }

    @Test
    void testCountByStatus() {
        Intent intent1 = createTestIntent("c1");
        // createTestIntent 已经设置状态为 SCHEDULED
        store.save(intent1);

        Intent intent2 = createTestIntent("c2");
        store.save(intent2);

        Intent intent3 = createTestIntent("c3");
        intent3.transitionTo(IntentStatus.DUE);
        intent3.transitionTo(IntentStatus.DISPATCHING);
        intent3.transitionTo(IntentStatus.DELIVERED);
        intent3.transitionTo(IntentStatus.ACKED);
        store.save(intent3);

        assertEquals(2, store.countByStatus(IntentStatus.SCHEDULED));
        assertEquals(1, store.countByStatus(IntentStatus.ACKED));
        assertEquals(0, store.countByStatus(IntentStatus.DUE));
    }

    @Test
    void testGetPendingCount() {
        Intent intent1 = createTestIntent("p1");
        // createTestIntent 已经设置状态为 SCHEDULED
        store.save(intent1);

        Intent intent2 = createTestIntent("p2");
        intent2.transitionTo(IntentStatus.DUE);
        intent2.transitionTo(IntentStatus.DISPATCHING);
        intent2.transitionTo(IntentStatus.DELIVERED);
        intent2.transitionTo(IntentStatus.ACKED);
        store.save(intent2);

        Intent intent3 = createTestIntent("p3");
        // createTestIntent 已经设置状态为 SCHEDULED
        intent3.transitionTo(IntentStatus.CANCELED);
        store.save(intent3);

        // 只有 SCHEDULED 是非终态
        assertEquals(1, store.getPendingCount());
    }

    @Test
    void testGetAllIntents() {
        store.save(createTestIntent("a1"));
        store.save(createTestIntent("a2"));

        var all = store.getAllIntents();

        assertEquals(2, all.size());
        assertTrue(all.containsKey("a1"));
        assertTrue(all.containsKey("a2"));
    }

    @Test
    void testIdempotencyResult_Methods() {
        IdempotencyResult notFound = IdempotencyResult.newRequest();
        assertTrue(notFound.isNotFound());
        assertFalse(notFound.exists());

        IdempotencyResult newReq = IdempotencyResult.newRequest();
        assertTrue(newReq.isNotFound());
        assertTrue(newReq.isAllowed());

        Intent intent = createTestIntent("result-test");
        IdempotencyResult active = IdempotencyResult.duplicateActive(intent);
        assertTrue(active.isDuplicateActive());
        assertTrue(active.isActive());
        assertFalse(active.isTerminal());

        IdempotencyResult terminal = IdempotencyResult.duplicateTerminal(intent);
        assertTrue(terminal.isDuplicateTerminal());
        assertFalse(terminal.isActive());
        assertTrue(terminal.isTerminal());

        IdempotencyResult expired = IdempotencyResult.windowExpired();
        // WINDOW_EXPIRED 不是 NEW，所以 isNotFound() 返回 false
        assertFalse(expired.isNotFound());
        assertTrue(expired.isAllowed()); // 但 isAllowed() 应该返回 true
    }

    @Test
    void testIdempotencyResult_ErrorCode() {
        Intent intent = createTestIntent("err-test");

        IdempotencyResult terminal = IdempotencyResult.duplicateTerminal(intent);
        assertEquals("40901", terminal.getErrorCode());

        IdempotencyResult active = IdempotencyResult.duplicateActive(intent);
        assertNull(active.getErrorCode());
    }

    private Intent createTestIntent(String id) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(300));

        Callback callback = new Callback();
        callback.setUrl("http://example.com/webhook");
        callback.setMethod("POST");
        intent.setCallback(callback);

        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }
}
