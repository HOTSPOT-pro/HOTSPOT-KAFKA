package hotspot.worker.producer.schema;

import java.time.Instant;
import java.util.UUID;


public record UsageEvent(
        String eventId,
        long subId,
        long familyId,
        long bytes,
        String appId,
        Instant occurredAt
) {

    public static UsageEvent create(long subId, long familyId, int usageKb) {
        return new UsageEvent(
                UUID.randomUUID().toString(),
                subId,
                familyId,
                usageKb,
                String.valueOf(AppType.randomAppId()),
                Instant.now()
        );
    }
}
