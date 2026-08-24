package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ExpiryIndexTest {

    private final ExpiryIndex index = new ExpiryIndex();

    @Test
    void indexAndUnindexRoundTrip() {
        Intent intent = new Intent("intent_ei_1");
        intent.setExecuteAt(Instant.ofEpochMilli(1000));
        index.index(intent);

        assertEquals(Set.of("intent_ei_1"), index.expiredEntriesUpTo(1000).get(1000L));
        assertTrue(index.expiredEntriesUpTo(999).isEmpty());

        index.unindex("intent_ei_1", 1000);
        assertTrue(index.expiredEntriesUpTo(1000).isEmpty());
    }

    @Test
    void unindexRemovesEmptyBucketEntry() {
        Intent intent = new Intent("intent_ei_2");
        intent.setExecuteAt(Instant.ofEpochMilli(2000));
        index.index(intent);
        index.unindex("intent_ei_2", 2000);
        assertNull(index.expiredEntriesUpTo(2000).get(2000L));
    }

    @Test
    void nullExecuteAtIsIgnored() {
        Intent intent = new Intent("intent_ei_3");
        index.index(intent);
        assertTrue(index.expiredEntriesUpTo(Long.MAX_VALUE).isEmpty());
    }
}
