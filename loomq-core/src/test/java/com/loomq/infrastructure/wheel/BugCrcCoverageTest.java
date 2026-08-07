package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * FIX #4 (CRC now covers status/revision) - regression guard.
 *
 * <p>Previously SlotCodec.encode() computed the CRC only over bytes [OFF_EXECUTE_AT .. end] = [13 .. 256].
 * The STATUS byte (offset 0) and REVISION bytes (offset 1..9) were written BEFORE that region and
 * were NOT covered by the CRC. A torn write that corrupts those bytes was therefore undetectable:
 * isTorn() returned false and decode() returned a silently corrupted intent.
 *
 * <p>The fix extends CRC coverage to [0..8] (status+revision) + [13..255] (executeAt..end),
 * skipping only the CRC field itself [9..12]. This test confirms the fix: corruption of status
 * or revision bytes is now detected as a torn slot.
 */
class BugCrcCoverageTest {

    @Test
    void crcCoversStatusByteSoTornStatusIsDetected() {
        Intent intent = new Intent("intent_crc_status0001");
        intent.setExecuteAt(Instant.now().plusSeconds(5));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.incrementRevision();

        byte[] slot = SlotCodec.encode(intent);
        assertTrue(SlotCodec.isOccupied(slot));
        assertTrue(!SlotCodec.isTorn(slot)); // clean slot passes CRC

        // Corrupt the STATUS byte (offset 0). CRC now covers [0..8], so this is detected.
        slot[0] = (byte) (slot[0] == 1 ? 2 : 1); // flip to a different non-zero status ordinal+1

        assertTrue(SlotCodec.isTorn(slot),
            "FIX #4: corrupted status byte is now flagged as torn (CRC covers bytes 0..8)");

        // decode must throw on torn slot (CRC mismatch), not silently return corrupted data
        assertThrows(RuntimeException.class, () -> SlotCodec.decode(slot),
            "FIX #4: decode must reject torn slot instead of returning silently corrupted intent");
    }

    @Test
    void crcCoversRevisionSoTornRevisionIsDetected() {
        Intent intent = new Intent("intent_crc_rev000001");
        intent.setExecuteAt(Instant.now().plusSeconds(5));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.incrementRevision(); // revision = 1
        long rev = intent.getRevision();

        byte[] slot = SlotCodec.encode(intent);
        assertTrue(!SlotCodec.isTorn(slot)); // clean slot passes CRC

        // Corrupt the REVISION bytes (offset 1..9). CRC now covers [0..8], so this is detected.
        slot[1] ^= 0xFF;
        slot[2] ^= 0xFF;

        assertTrue(SlotCodec.isTorn(slot),
            "FIX #4: corrupted revision bytes are now flagged as torn (CRC covers bytes 0..8)");

        assertThrows(RuntimeException.class, () -> SlotCodec.decode(slot),
            "FIX #4: decode must reject torn slot instead of returning silently corrupted intent");

        // Sanity: the original intent's revision is unaffected by the slot corruption
        assertEquals(rev, intent.getRevision());
    }
}
