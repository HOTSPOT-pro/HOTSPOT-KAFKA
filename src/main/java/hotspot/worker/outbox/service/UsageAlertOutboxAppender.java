package hotspot.worker.outbox.service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.consumer.usage.domain.GiftFire;
import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageAlertEvent;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.consumer.usage.support.TimeKey;
import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;
import hotspot.worker.writer.infrastructure.entity.UsageAppliedEventLogEntity;

@Service
public class UsageAlertOutboxAppender {

    private static final String AGGREGATE_TYPE = "user-alert";
    private static final String EVENT_TYPE = "USAGE_THRESHOLD";
    private static final ZoneId ZONE_KST = ZoneId.of("Asia/Seoul");

    private final ObjectMapper objectMapper;

    public UsageAlertOutboxAppender(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<NotificationOutboxEventEntity> buildOutboxEntities(UsageEvent source, UsageLuaResult result) {
        if (result.duplicate()) {
            return List.of();
        }
        List<OutboxEntry> entries = buildOutboxEntries(source, result);
        List<NotificationOutboxEventEntity> entities = new ArrayList<>(entries.size());
        for (OutboxEntry entry : entries) {
            entities.add(new NotificationOutboxEventEntity(
                    UUID.randomUUID(),
                    AGGREGATE_TYPE,
                    entry.aggregateId(),
                    entry.type(),
                    toJson(entry.payload())
            ));
        }
        return entities;
    }

    public UsageAppliedEventLogEntity buildAppliedLogEntity(UsageEvent source, UsageLuaResult result) {
        if (result.duplicate()) {
            return null;
        }
        return new UsageAppliedEventLogEntity(
                source.eventId(),
                source.subId(),
                source.familyId(),
                source.appId(),
                LocalDateTime.ofInstant(source.occurredAt(), ZONE_KST),
                TimeKey.yyyymm(source.occurredAt(), ZONE_KST),
                TimeKey.yyyymmdd(source.occurredAt(), ZONE_KST),
                result.bytes(),
                result.giftTake(),
                result.planTake(),
                result.familyTake(),
                toJson(result.giftAllocations())
        );
    }

    private List<OutboxEntry> buildOutboxEntries(UsageEvent source, UsageLuaResult result) {
        List<OutboxEntry> entries = new ArrayList<>();

        if (result.planFired()) {
            String aggregateId = "sub:" + source.subId();
            int usedPercent = usedPercentFromRemaining(result.planRemainingPct());
            UsageMetrics metrics = toMetrics(
                    result.planProvidedAmount(),
                    result.planUsedAmount(),
                    usedPercent
            );
            UsageAlertEvent event = buildEvent(
                    source,
                    "PLAN_REMAINING",
                    source.subId(),
                    null,
                    null,
                    result.planThreshold(),
                    metrics
            );
            entries.add(new OutboxEntry(aggregateId, "PLAN_REMAINING", event));
        }

        if (result.familyFired()) {
            String aggregateId = "family:" + source.familyId();
            int usedPercent = usedPercentFromRemaining(result.familyRemainingPct());
            UsageMetrics metrics = toMetrics(
                    result.familyProvidedAmount(),
                    result.familyUsedAmount(),
                    usedPercent
            );
            UsageAlertEvent event = buildEvent(
                    source,
                    "FAMILY_POOL_REMAINING",
                    null,
                    source.familyId(),
                    null,
                    result.familyThreshold(),
                    metrics
            );
            entries.add(new OutboxEntry(aggregateId, "FAMILY_POOL_REMAINING", event));
        }

        for (GiftFire giftFire : result.giftFires()) {
            String aggregateId = "gift:" + giftFire.giftId();
            int usedPercent = usedPercentFromRemaining(giftFire.pct());
            UsageMetrics metrics = toMetrics(
                    giftFire.providedAmount(),
                    giftFire.usedAmount(),
                    usedPercent
            );
            UsageAlertEvent event = buildEvent(
                    source,
                    "GIFT_REMAINING",
                    source.subId(),
                    null,
                    giftFire.giftId(),
                    giftFire.th(),
                    metrics
            );
            entries.add(new OutboxEntry(aggregateId, "GIFT_REMAINING", event));
        }

        return entries;
    }

    private UsageAlertEvent buildEvent(
            UsageEvent source,
            String alertType,
            Long subId,
            Long familyId,
            String giftId,
            int threshold,
            UsageMetrics metrics
    ) {
        String alertId = UUID.randomUUID().toString();
        return new UsageAlertEvent(
                alertId,
                EVENT_TYPE,
                alertType,
                subId,
                familyId,
                String.valueOf(threshold),
                giftId,
                metrics.providedAmount(),
                metrics.usedPercent(),
                metrics.usedAmount(),
                LocalDateTime.ofInstant(source.occurredAt(), ZONE_KST)
        );
    }

    private UsageMetrics toMetrics(long providedRaw, long usedRaw, int usedPercentRaw) {
        long provided = Math.max(0L, providedRaw);
        long used = Math.max(0L, usedRaw);
        if (provided > 0 && used > provided) {
            used = provided;
        }
        int usedPercent = Math.max(0, Math.min(100, usedPercentRaw));
        return new UsageMetrics(
                String.valueOf(provided),
                String.valueOf(usedPercent),
                String.valueOf(used)
        );
    }

    private int usedPercentFromRemaining(int remainingPctRaw) {
        int remainingPct = Math.max(0, Math.min(100, remainingPctRaw));
        return 100 - remainingPct;
    }

    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize payload", e);
        }
    }

    private record OutboxEntry(String aggregateId, String type, UsageAlertEvent payload) {
    }

    private record UsageMetrics(String providedAmount, String usedPercent, String usedAmount) {
    }
}
