package hotspot.worker.consumer.usage.schema;

import java.time.Instant;

/**
 * usage-alert-events 발행 스키마
 */
public record UsageAlertEvent(
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
