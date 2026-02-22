package hotspot.worker.consumer.usage.schema;

import java.time.Instant;

/**
 * Unified alert event schema published to user-alert-events.
 */
public record UsageAlertEvent(
        String alertId,
        String eventType,
        String alertType,
        String threshold,
        Instant occurredAt,
        Long subId,
        Long familyId,
        String giftId,
        long remainingBytes,
        int remainingPct,
        String sourceEventId
) {
}
