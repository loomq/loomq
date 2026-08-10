package com.loomq.common;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class IntentValidatorTest {

    @Test
    void rejectsNullExecuteAt() {
        Intent intent = new Intent("valid-id-1234567890");
        intent.setDeadline(Instant.now().plusSeconds(60));
        // executeAt is null
        assertThrows(IllegalArgumentException.class, () -> IntentValidator.validate(intent));
    }

    @Test
    void acceptsNullDeadline() {
        // null deadline = never expires; this is a valid design choice (Intent.isExpired handles null)
        Intent intent = new Intent("valid-id-1234567890");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        // deadline is null
        assertDoesNotThrow(() -> IntentValidator.validate(intent));
    }

    @Test
    void rejectsDeadlineBeforeExecuteAt() {
        Intent intent = new Intent("valid-id-1234567890");
        Instant executeAt = Instant.now().plusSeconds(120);
        Instant deadline = Instant.now().plusSeconds(60);
        intent.setExecuteAt(executeAt);
        intent.setDeadline(deadline);
        assertThrows(IllegalArgumentException.class, () -> IntentValidator.validate(intent));
    }

    @Test
    void rejectsOversizedIntentId() {
        Intent intent = new Intent("this-id-is-way-too-long-for-slot");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(120));
        assertThrows(IllegalArgumentException.class, () -> IntentValidator.validate(intent));
    }

    @Test
    void rejectsBlankIntentId() {
        Intent intent = new Intent("   ");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(120));
        assertThrows(IllegalArgumentException.class, () -> IntentValidator.validate(intent));
    }

    @Test
    void acceptsValidIntent() {
        Intent intent = new Intent("valid-id-1234567890");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(120));
        assertDoesNotThrow(() -> IntentValidator.validate(intent));
    }

    @Test
    void acceptsNullIntentId() {
        Intent intent = new Intent();
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(120));
        assertDoesNotThrow(() -> IntentValidator.validate(intent));
    }
}
