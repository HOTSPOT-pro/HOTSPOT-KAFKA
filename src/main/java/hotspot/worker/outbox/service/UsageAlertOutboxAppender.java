package hotspot.worker.outbox.service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.consumer.usage.domain.GiftFire;
import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageAlertEvent;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.consumer.usage.support.TimeKey;
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
    private final StringRedisTemplate redis;

    public UsageAlertOutboxAppender(
            NotificationOutboxEventJpaRepository outboxRepository,
            ObjectMapper objectMapper,
            StringRedisTemplate redis
    ) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.redis = redis;
    }

    // 사용량 처리 결과를 알림 이벤트로 변환해 Outbox에 저장한다.
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

    // 임계치 발화 결과(개인/가족/선물)별 Outbox 엔트리를 만든다.
    private List<OutboxEntry> buildOutboxEntries(UsageEvent source, UsageLuaResult result) {
        List<OutboxEntry> entries = new ArrayList<>();

        if (result.planFired()) {
            String aggregateId = "sub:" + source.subId();
            UsageMetrics metrics = readPlanMetrics(source);
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
            UsageMetrics metrics = readFamilyMetrics(source);
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
            UsageMetrics metrics = readGiftMetrics(source, giftFire.giftId());
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

    // 공통 메타 정보와 계산된 사용량 지표를 담아 알림 페이로드를 만든다.
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
                LocalDateTime.ofInstant(source.occurredAt(), ALERT_TIME_ZONE)
        );
    }

    // 개인 요금제 기준 제공량/사용률/사용량을 읽어온다.
    private UsageMetrics readPlanMetrics(UsageEvent source) {
        String yyyymm = TimeKey.yyyymm(source.occurredAt(), ALERT_TIME_ZONE);
        long provided = readHashLong("limit:sub:" + source.subId(), "plan_limit");
        long used = readHashLong("usage:sub:" + source.subId() + ":" + yyyymm, "plan_used");
        return toMetrics(provided, used);
    }

    // 가족 공유풀 기준 제공량/사용률/사용량을 읽어온다.
    private UsageMetrics readFamilyMetrics(UsageEvent source) {
        String yyyymm = TimeKey.yyyymm(source.occurredAt(), ALERT_TIME_ZONE);
        long provided = readHashLong("limit:family:" + source.familyId(), "family_limit");
        long used = readHashLong("usage:family:" + source.familyId() + ":" + yyyymm, "family_used");
        return toMetrics(provided, used);
    }

    // 선물 데이터 기준 제공량/사용률/사용량을 읽어온다.
    private UsageMetrics readGiftMetrics(UsageEvent source, String giftId) {
        String yyyymm = TimeKey.yyyymm(source.occurredAt(), ALERT_TIME_ZONE);
        String limitKey = "limit:gift:" + source.subId() + ":" + giftId + ":" + yyyymm;
        String usageKey = "usage:gift:" + source.subId() + ":" + giftId + ":" + yyyymm;
        long provided = readHashLong(limitKey, "gift_limit");
        long used = readHashLong(usageKey, "gift_used");
        return toMetrics(provided, used);
    }

    // 원시 byte 값을 알림용 문자열 포맷으로 변환한다.
    private UsageMetrics toMetrics(long providedRaw, long usedRaw) {
        long provided = Math.max(0L, providedRaw);
        long used = Math.max(0L, usedRaw);
        if (provided > 0 && used > provided) {
            used = provided;
        }
        long usedPercent = provided == 0 ? 0 : (used * 100) / provided;
        return new UsageMetrics(
                String.valueOf(provided),
                String.valueOf(usedPercent),
                String.valueOf(used)
        );
    }

    // Redis Hash 필드를 long으로 읽고, 값이 없으면 0을 반환한다.
    private long readHashLong(String key, String field) {
        Object raw = redis.opsForHash().get(key, field);
        if (raw == null) {
            return 0L;
        }
        String value = String.valueOf(raw);
        if (value.isBlank()) {
            return 0L;
        }
        return Long.parseLong(value);
    }

    // Outbox payload 객체를 JSON 문자열로 직렬화한다.
    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox payload", e);
        }
    }

    private record OutboxEntry(String aggregateId, String type, UsageAlertEvent payload) {
    }

    private record UsageMetrics(String providedAmount, String usedPercent, String usedAmount) {
    }
}
