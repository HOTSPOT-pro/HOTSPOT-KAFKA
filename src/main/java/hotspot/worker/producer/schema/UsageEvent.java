package hotspot.worker.producer.schema;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

public record UsageEvent(
        String eventId,
        long subId,
        long familyId,
        long dataUsage,
        Long appId,
        LocalDateTime createdTime
) {

    public static UsageEvent create(long subId, long familyId, int dataUsage) {
        return new UsageEvent(
                UUID.randomUUID().toString(),
                subId,
                familyId,
                dataUsage,
                randomApp(),
                LocalDateTime.now(ZoneId.of("Asia/Seoul"))
        );
    }

    public static Long randomApp() {
        return AppType.randomAppId();
    }
}
