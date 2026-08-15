package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.loomq.LoomqEngineFactory;
import org.junit.jupiter.api.Test;

/**
 * LoomqEngineFactory 应像 WheelConfig.fromProperties 一样容忍 null Properties。
 */
class BugFactoryNullPropertiesTest {

    @Test
    void createFromPropertiesMustHandleNull() {
        assertDoesNotThrow(() -> LoomqEngineFactory.createFromProperties(null));
    }
}
