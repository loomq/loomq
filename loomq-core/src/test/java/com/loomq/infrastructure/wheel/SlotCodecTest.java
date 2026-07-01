package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SlotCodecTest {
    @Test
    void shouldRoundTripTypicalIntent() {
        Intent intent = new Intent("intent_abcdef0123456789");
        intent.setExecuteAt(Instant.now().plusSeconds(5));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.incrementRevision();

        byte[] slot = SlotCodec.encode(intent);
        assertEquals(256, slot.length);
        assertTrue(SlotCodec.isOccupied(slot));
        assertFalse(SlotCodec.isTorn(slot));

        Intent decoded = SlotCodec.decode(slot);
        assertEquals(intent.getIntentId(), decoded.getIntentId());
        assertEquals(intent.getStatus(), decoded.getStatus());
        assertEquals(intent.getExecuteAt(), decoded.getExecuteAt());
        assertEquals(intent.getRevision(), decoded.getRevision());
    }

    @Test
    void shouldDetectTornSlot() {
        Intent intent = new Intent("intent_torn0000000001");
        intent.setExecuteAt(Instant.now().plusSeconds(5));
        intent.transitionTo(IntentStatus.SCHEDULED);
        byte[] slot = SlotCodec.encode(intent);
        slot[42] ^= 0xFF; // corrupt payload
        assertTrue(SlotCodec.isTorn(slot));
    }

    @Test
    void shouldRejectOversizedPayload() {
        Intent intent = new Intent("intent_big0000000000aa");
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 300; i++) huge.append('x');
        intent.setTags(java.util.Map.of("k", huge.toString()));
        assertThrows(SlotOverflowException.class, () -> SlotCodec.encode(intent));
    }

    @Test
    void emptySlotIsNotOccupied() {
        byte[] empty = new byte[256];
        assertFalse(SlotCodec.isOccupied(empty));
    }
}
