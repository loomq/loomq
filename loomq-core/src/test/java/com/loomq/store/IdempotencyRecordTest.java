package com.loomq.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * 幂等性记录测试
 */
class IdempotencyRecordTest {

    @Test
    void testCreateFromIntent() {
        Intent intent = createIntent("test-1", "idem-key-1");
        intent.transitionTo(IntentStatus.SCHEDULED);

        IdempotencyRecord record = IdempotencyRecord.fromIntent(intent);

        assertEquals("idem-key-1", record.getIdempotencyKey());
        assertEquals("test-1", record.getIntentId());
        assertEquals(IntentStatus.SCHEDULED, record.getStatus());
        assertNotNull(record.getCreatedAt());
        assertNotNull(record.getWindowExpiry());
        assertTrue(record.isInWindow());
    }

    @Test
    void testWindowCalculation() {
        Instant now = Instant.now();
        Intent intent = createIntent("test-2", "idem-key-2");

        IdempotencyRecord record = IdempotencyRecord.fromIntent(intent);

        // 窗口期应该是24小时
        Duration window = Duration.between(record.getCreatedAt(), record.getWindowExpiry());
        assertEquals(Duration.ofHours(24), window);
    }

    @Test
    void testIsInWindow() {
        Instant now = Instant.now();
        IdempotencyRecord record = new IdempotencyRecord(
            "key", "intent",
            now,
            now.plusSeconds(60), // 60秒后过期
            IntentStatus.SCHEDULED
        );

        assertTrue(record.isInWindow());
        assertFalse(record.isExpired());
    }

    @Test
    void testIsExpired() throws InterruptedException {
        Instant now = Instant.now();
        IdempotencyRecord record = new IdempotencyRecord(
            "key", "intent",
            now,
            now.plusMillis(50), // 50ms后过期
            IntentStatus.SCHEDULED
        );

        assertFalse(record.isExpired());
        TimeUnit.MILLISECONDS.sleep(60);
        assertTrue(record.isExpired());
        assertFalse(record.isInWindow());
    }

    @Test
    void testUpdateStatus() {
        Intent intent = createIntent("test-3", "idem-key-3");
        intent.transitionTo(IntentStatus.SCHEDULED);

        IdempotencyRecord record = IdempotencyRecord.fromIntent(intent);
        assertEquals(IntentStatus.SCHEDULED, record.getStatus());

        record.updateStatus(IntentStatus.DUE);
        assertEquals(IntentStatus.DUE, record.getStatus());

        record.updateStatus(IntentStatus.ACKED);
        assertEquals(IntentStatus.ACKED, record.getStatus());
    }

    @Test
    void testIsTerminal() {
        IdempotencyRecord record = new IdempotencyRecord(
            "key", "intent",
            Instant.now(),
            Instant.now().plus(Duration.ofHours(24)),
            IntentStatus.SCHEDULED
        );

        assertFalse(record.isTerminal());

        record.updateStatus(IntentStatus.ACKED);
        assertTrue(record.isTerminal());

        record.updateStatus(IntentStatus.CANCELED);
        assertTrue(record.isTerminal());

        record.updateStatus(IntentStatus.EXPIRED);
        assertTrue(record.isTerminal());

        record.updateStatus(IntentStatus.DEAD_LETTERED);
        assertTrue(record.isTerminal());
    }

    @Test
    void testIsTerminal_NullStatus() {
        IdempotencyRecord record = new IdempotencyRecord(
            "key", "intent",
            Instant.now(),
            Instant.now().plus(Duration.ofHours(24)),
            null
        );

        assertFalse(record.isTerminal());
    }

    @Test
    void testEquals() {
        Instant now = Instant.now();
        IdempotencyRecord record1 = new IdempotencyRecord(
            "same-key", "intent-1", now, now.plus(Duration.ofHours(24)), IntentStatus.SCHEDULED
        );
        IdempotencyRecord record2 = new IdempotencyRecord(
            "same-key", "intent-2", now.plusSeconds(10), now.plus(Duration.ofHours(25)), IntentStatus.ACKED
        );
        IdempotencyRecord record3 = new IdempotencyRecord(
            "different-key", "intent-1", now, now.plus(Duration.ofHours(24)), IntentStatus.SCHEDULED
        );

        // 相同幂等键视为相等
        assertEquals(record1, record2);
        assertEquals(record1.hashCode(), record2.hashCode());

        // 不同幂等键不相等
        assertNotEquals(record1, record3);
    }

    @Test
    void testEquals_SameObject() {
        IdempotencyRecord record = IdempotencyRecord.fromIntent(
            createIntent("id", "key")
        );
        assertEquals(record, record);
    }

    @Test
    void testEquals_Null() {
        IdempotencyRecord record = IdempotencyRecord.fromIntent(
            createIntent("id", "key")
        );
        assertNotEquals(null, record);
    }

    @Test
    void testEquals_DifferentClass() {
        IdempotencyRecord record = IdempotencyRecord.fromIntent(
            createIntent("id", "key")
        );
        assertNotEquals(record, "not a record");
    }

    @Test
    void testToString() {
        IdempotencyRecord record = new IdempotencyRecord(
            "test-key", "test-intent",
            Instant.now(),
            Instant.now().plus(Duration.ofHours(24)),
            IntentStatus.SCHEDULED
        );

        String str = record.toString();

        assertTrue(str.contains("test-key"));
        assertTrue(str.contains("test-intent"));
        assertTrue(str.contains("SCHEDULED"));
    }

    @Test
    void testConstructor_WithCustomWindow() {
        Instant now = Instant.now();
        Instant customExpiry = now.plus(Duration.ofMinutes(30));

        IdempotencyRecord record = new IdempotencyRecord(
            "key", "intent", now, customExpiry, IntentStatus.SCHEDULED
        );

        assertEquals(customExpiry, record.getWindowExpiry());
    }

    @Test
    void testConstructor_NullChecks() {
        Instant now = Instant.now();

        assertThrows(NullPointerException.class, () ->
            new IdempotencyRecord(null, "intent", now, now.plus(Duration.ofHours(24)), IntentStatus.SCHEDULED)
        );

        assertThrows(NullPointerException.class, () ->
            new IdempotencyRecord("key", null, now, now.plus(Duration.ofHours(24)), IntentStatus.SCHEDULED)
        );

        assertThrows(NullPointerException.class, () ->
            new IdempotencyRecord("key", "intent", null, now.plus(Duration.ofHours(24)), IntentStatus.SCHEDULED)
        );

        assertThrows(NullPointerException.class, () ->
            new IdempotencyRecord("key", "intent", now, null, IntentStatus.SCHEDULED)
        );
    }

    @Test
    void testDefaultWindowConstant() {
        assertEquals(Duration.ofHours(24), IdempotencyRecord.DEFAULT_WINDOW);
    }

    @Test
    void testGetters() {
        Instant now = Instant.now();
        Instant expiry = now.plus(Duration.ofHours(24));

        IdempotencyRecord record = new IdempotencyRecord(
            "my-key", "my-intent", now, expiry, IntentStatus.DUE
        );

        assertEquals("my-key", record.getIdempotencyKey());
        assertEquals("my-intent", record.getIntentId());
        assertEquals(now, record.getCreatedAt());
        assertEquals(expiry, record.getWindowExpiry());
        assertEquals(IntentStatus.DUE, record.getStatus());
    }

    @Test
    void testFromIntent_NullIdempotencyKey() {
        Intent intent = createIntent("test", null);
        // 如果 idempotencyKey 为 null，会抛出 NPE
        assertThrows(NullPointerException.class, () ->
            IdempotencyRecord.fromIntent(intent)
        );
    }

    private Intent createIntent(String id, String idempotencyKey) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(300));
        intent.setIdempotencyKey(idempotencyKey);

        Callback callback = new Callback();
        callback.setUrl("http://example.com/webhook");
        intent.setCallback(callback);

        return intent;
    }
}
