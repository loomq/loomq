package com.loomq.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

/**
 * ReadOnlyIntentStoreView 作为 AutoCloseable 不应在 close() 时抛异常。
 */
class BugReadOnlyViewCloseTest {

    @Test
    void closeMustBeNoOp() {
        IntentStore delegate = new ConcurrentIntentStore();
        ReadOnlyIntentStoreView view = new ReadOnlyIntentStoreView(delegate);
        assertDoesNotThrow(view::close);
    }
}
