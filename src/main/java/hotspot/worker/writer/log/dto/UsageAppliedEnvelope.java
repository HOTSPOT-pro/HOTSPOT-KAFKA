package hotspot.worker.writer.log.dto;

import java.util.List;

import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;
import hotspot.worker.writer.infrastructure.entity.UsageAppliedEventLogEntity;
import hotspot.worker.writer.log.support.UsageBatchAcknowledgment;

public record UsageAppliedEnvelope(
        UsageEvent event,
        UsageLuaResult result,
        List<NotificationOutboxEventEntity> outboxEvents,
        UsageAppliedEventLogEntity usageAppliedLog,
        UsageBatchAcknowledgment batchAcknowledgment
) {
}
