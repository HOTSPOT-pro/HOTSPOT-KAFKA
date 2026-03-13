package hotspot.worker.outbox.service;

import java.util.List;

import org.springframework.kafka.support.Acknowledgment;

import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;
import hotspot.worker.outbox.infrastructure.entity.UsageAppliedEventLogEntity;

public record UsageAppliedEnvelope(
        UsageEvent event,
        UsageLuaResult result,
        List<NotificationOutboxEventEntity> outboxEvents,
        UsageAppliedEventLogEntity usageAppliedLog,
        Acknowledgment acknowledgment
) {
}
