package hotspot.worker.producer.schema;

import java.time.LocalDateTime;
import java.util.UUID;

public record UsageEvent(
        String eventId,
        long subId,
        long familyId,
        long usageKb,
        String appId,
        LocalDateTime createdTime
) {

    public static UsageEvent create(long subId, long familyId, int usageKb) {
        return new UsageEvent(
                UUID.randomUUID().toString(),
                subId,
                familyId,
                usageKb,
                randomApp(),
                LocalDateTime.now()
        );
    }

    public static String randomApp() {
        return AppType.randomCode();
    }
}
