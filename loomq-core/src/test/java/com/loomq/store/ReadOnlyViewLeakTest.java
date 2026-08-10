package com.loomq.store;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ReadOnlyViewLeakTest {

    @Test
    void findByIdInternalReturnsDefensiveCopy() {
        ConcurrentIntentStore inner = new ConcurrentIntentStore();
        Intent intent = new Intent("intent_leak_test_01");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(120));
        intent.transitionTo(IntentStatus.SCHEDULED);
        inner.save(intent);

        ReadOnlyIntentStoreView view = new ReadOnlyIntentStoreView(inner);

        // findByIdInternal on the view should return a defensive copy (via findById fallback),
        // NOT the live internal reference.
        Intent viaView = view.findByIdInternal("intent_leak_test_01");
        Intent viaInner = inner.findByIdInternal("intent_leak_test_01");

        // If they're the same reference, mutating one would affect the other.
        // With defensive copy, they should be different objects.
        assertNotSame(viaInner, viaView,
            "ReadOnlyIntentStoreView.findByIdInternal must not return the live internal reference");
    }
}
