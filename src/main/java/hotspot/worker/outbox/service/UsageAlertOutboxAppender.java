package hotspot.worker.outbox.service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.consumer.usage.domain.GiftFire;
import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageAlertEvent;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.outbox.infrastructure.NotificationOutboxEventJpaRepository;
import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;

@Service
@Transactional
public class UsageAlertOutboxAppender {

    private static final String AGGREGATE_TYPE = "user-alert";
    private static final String EVENT_TYPE = "USAGE_THRESHOLD";
    private static final ZoneId ALERT_TIME_ZONE = ZoneId.of("Asia/Seoul");

    private final NotificationOutboxEventJpaRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public UsageAlertOutboxAppender(
            NotificationOutboxEventJpaRepository outboxRepository,
            ObjectMapper objectMapper
    ) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    // 사용량 처리 결과를 기반으로 필요한 알림 이벤트들을 생성한 뒤 Outbox 테이블에 저장한다.
    public void append(UsageEvent source, UsageLuaResult result) {
        List<OutboxEntry> entries = buildOutboxEntries(source, result);
        for (OutboxEntry entry : entries) {
            outboxRepository.save(
                    new NotificationOutboxEventEntity(
                        UUID.randomUUID(),
                        AGGREGATE_TYPE,
                        entry.aggregateId(),
                        entry.type(),
                        toJson(entry.payload())
                    )
            );
        }
    }

    // 개인/가족/선물 임계치 도달 여부에 따라 Outbox에 적재할 엔트리 목록을 만든다.
    private List<OutboxEntry> buildOutboxEntries(UsageEvent source, UsageLuaResult result) {
        List<OutboxEntry> entries = new ArrayList<>();

        if (result.planFired()) {
            String aggregateId = "sub:" + source.subId();
            UsageAlertEvent event = buildEvent(
                    source,
                    "PLAN_REMAINING",
                    aggregateId,
                    source.subId(),
                    null,
                    null,
                    result.planThreshold()
            );
            entries.add(new OutboxEntry(aggregateId, "PLAN_REMAINING", event));
        }

        if (result.familyFired()) {
            String aggregateId = "family:" + source.familyId();
            UsageAlertEvent event = buildEvent(
                    source,
                    "FAMILY_POOL_REMAINING",
                    aggregateId,
                    null,
                    source.familyId(),
                    null,
                    result.familyThreshold()
            );
            entries.add(new OutboxEntry(aggregateId, "FAMILY_POOL_REMAINING", event));
        }

        for (GiftFire giftFire : result.giftFires()) {
            String aggregateId = "gift:" + giftFire.giftId();
            UsageAlertEvent event = buildEvent(
                    source,
                    "GIFT_REMAINING",
                    aggregateId,
                    source.subId(),
                    null,
                    giftFire.giftId(),
                    giftFire.th()
            );
            entries.add(new OutboxEntry(aggregateId, "GIFT_REMAINING", event));
        }

        return entries;
    }

    // 알림 타입과 대상 식별자에 맞는 UsageAlertEvent payload를 생성한다.
    private UsageAlertEvent buildEvent(
            UsageEvent source,
            String alertType,
            String targetKey,
            Long subId,
            Long familyId,
            String giftId,
            int threshold
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
                LocalDateTime.ofInstant(source.occurredAt(), ALERT_TIME_ZONE)
        );
    }

    // Outbox에 저장할 payload 객체를 JSON 문자열로 직렬화한다.
    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox payload", e);
        }
    }

    private record OutboxEntry(String aggregateId, String type, UsageAlertEvent payload) {
    }
}
