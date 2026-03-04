package hotspot.worker.consumer.usage.schema;

import java.time.LocalDateTime;

public record UsageAlertEvent(
        String alertId,
        String eventType,
        String alertType,
        Long subId,
        Long familyId,
        String threshold,
        String giftId,
        String providedAmount,
        String usedPercent,
        String usedAmount,
        LocalDateTime createdTime
) {
}
