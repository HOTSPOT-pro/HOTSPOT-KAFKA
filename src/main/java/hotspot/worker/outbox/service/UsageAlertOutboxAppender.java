package hotspot.worker.outbox.service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.consumer.usage.domain.GiftFire;
import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageAlertEvent;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.consumer.usage.support.TimeKey;
import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;
import hotspot.worker.outbox.infrastructure.entity.UsageAppliedEventLogEntity;

@Service
public class UsageAlertOutboxAppender {

    private static final String AGGREGATE_TYPE = "user-alert";
    private static final String EVENT_TYPE = "USAGE_THRESHOLD";
    private static final ZoneId ZONE_KST = ZoneId.of("Asia/Seoul");

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redis;

    // 알림 페이로드 직렬화와 지표 조회용 의존성을 주입받는다.
    public UsageAlertOutboxAppender(
            ObjectMapper objectMapper,
            StringRedisTemplate redis
    ) {
        this.objectMapper = objectMapper;
        this.redis = redis;
    }

    // Lua 결과를 기반으로 알림 outbox 엔티티 목록을 생성한다.
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

    // 사용량 반영 로그 엔티티를 생성한다.
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

    // 요금제/가족풀/선물 임계치 발화 결과를 outbox 엔트리로 변환한다.
    private List<OutboxEntry> buildOutboxEntries(UsageEvent source, UsageLuaResult result) {
        List<OutboxEntry> entries = new ArrayList<>();

        if (result.planFired()) {
            String aggregateId = "sub:" + source.subId();
            int usedPercent = usedPercentFromRemaining(result.planRemainingPct());
            UsageMetrics metrics = readPlanMetrics(source, usedPercent);
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
            UsageMetrics metrics = readFamilyMetrics(source, usedPercent);
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
            UsageMetrics metrics = readGiftMetrics(source, giftFire.giftId(), usedPercent);
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

    // 단일 알림 이벤트 페이로드를 구성한다.
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

    // 요금제 한도/사용량 지표를 Redis에서 조회한다.
    private UsageMetrics readPlanMetrics(UsageEvent source, int usedPercent) {
        String yyyymm = TimeKey.yyyymm(source.occurredAt(), ZONE_KST);
        long provided = readHashLong("limit:sub:" + source.subId(), "plan_limit");
        long used = readHashLong("usage:sub:" + source.subId() + ":" + yyyymm, "plan_used");
        return toMetrics(provided, used, usedPercent);
    }

    // 가족풀 한도/사용량 지표를 Redis에서 조회한다.
    private UsageMetrics readFamilyMetrics(UsageEvent source, int usedPercent) {
        String yyyymm = TimeKey.yyyymm(source.occurredAt(), ZONE_KST);
        long provided = readHashLong("limit:family:" + source.familyId(), "family_limit");
        long used = readHashLong("usage:family:" + source.familyId() + ":" + yyyymm, "family_used");
        return toMetrics(provided, used, usedPercent);
    }

    // 선물 한도/사용량 지표를 Redis에서 조회한다.
    private UsageMetrics readGiftMetrics(UsageEvent source, String giftId, int usedPercent) {
        String yyyymm = TimeKey.yyyymm(source.occurredAt(), ZONE_KST);
        String limitKey = "limit:gift:" + source.subId() + ":" + giftId + ":" + yyyymm;
        String usageKey = "usage:gift:" + source.subId() + ":" + giftId + ":" + yyyymm;
        long provided = readHashLong(limitKey, "gift_limit");
        long used = readHashLong(usageKey, "gift_used");
        return toMetrics(provided, used, usedPercent);
    }

    // 원시 지표를 알림용 출력 포맷으로 정규화한다.
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

    // 잔여율을 사용률로 변환한다.
    private int usedPercentFromRemaining(int remainingPctRaw) {
        int remainingPct = Math.max(0, Math.min(100, remainingPctRaw));
        return 100 - remainingPct;
    }

    // Redis HASH 필드를 long 값으로 조회한다.
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

    // 페이로드 객체를 JSON 문자열로 직렬화한다.
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
