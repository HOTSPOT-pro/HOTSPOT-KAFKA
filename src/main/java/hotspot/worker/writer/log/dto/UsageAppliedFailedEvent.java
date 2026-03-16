package hotspot.worker.writer.log.dto;

import java.time.LocalDateTime;

public record UsageAppliedFailedEvent(
        String eventId,
        String payload,
        String exceptionType,
        String exceptionMessage,
        LocalDateTime failedAt
) {
}
