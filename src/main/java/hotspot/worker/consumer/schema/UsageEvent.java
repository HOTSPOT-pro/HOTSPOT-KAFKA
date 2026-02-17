package hotspot.worker.consumer.schema;

import java.time.Instant;

/**
 * usage-events 수신 스키마
 */
public record UsageEvent(
        String eventId,
        long subId,
        long familyId,
        long bytes,
        Long appId,
        Instant occurredAt
) {
}
