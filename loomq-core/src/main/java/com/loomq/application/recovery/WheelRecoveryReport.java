package com.loomq.application.recovery;

import java.util.Map;
import java.util.Set;

public record WheelRecoveryReport(int hotRestored, int coldRegistered, Set<String> multiSlotIntentIds,
                                  Map<String, Long> maxRevisions) {
}
