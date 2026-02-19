package hotspot.worker.consumer.usage.service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.consumer.usage.schema.UsageAlertEvent;

/**
 * Redis Stream outbox를 읽어 usage-alert-events로 발행하는 퍼블리셔
 */
@Service
public class OutboxAlertPublisher {

    private static final String FIELD_ATTEMPTS = "attempts";
    private static final String FIELD_LAST_ERROR = "last_error";
    private static final String FIELD_LAST_FAILED_TIME = "last_failed_time";
    private static final String FIELD_STREAM_ID = "stream_id";
    private static final String FIELD_ERROR = "error";

    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, UsageAlertEvent> kafka;
    private final ObjectMapper om;

    private final String topic;
    private final String streamKey;
    private final String dlqStreamKey;
    private final String metaKeyPrefix;
    private final String group;
    private final String consumerName;
    private final long readCount;
    private final long blockMs;
    private final int maxAttempts;
    private final long metaTtlSeconds;
    private final long reclaimMinIdleMs;
    private final long reclaimBatchSize;

    public OutboxAlertPublisher(
            StringRedisTemplate redis,
            KafkaTemplate<String, UsageAlertEvent> kafka,
            ObjectMapper om,
            @Value("${app.topics.usage-alert-events}") String topic,
            @Value("${app.outbox.usage-alerts.stream-key:outbox:usage-alerts:v1}") String streamKey,
            @Value("${app.outbox.usage-alerts.dlq-stream-key:outbox:usage-alerts:dlq:v1}") String dlqStreamKey,
            @Value("${app.outbox.usage-alerts.meta-key-prefix:outbox:meta:usage-alerts:v1:}") String metaKeyPrefix,
            @Value("${app.outbox.usage-alerts.group:usage-alerts-pub-g1}") String group,
            @Value("${app.outbox.usage-alerts.consumer-name}") String configuredConsumerName,
            @Value("${app.outbox.usage-alerts.read-count:100}") long readCount,
            @Value("${app.outbox.usage-alerts.block-ms:2000}") long blockMs,
            @Value("${app.outbox.usage-alerts.max-attempts:20}") int maxAttempts,
            @Value("${app.outbox.usage-alerts.meta-ttl-seconds:1209600}") long metaTtlSeconds,
            @Value("${app.outbox.usage-alerts.reclaim-min-idle-ms:60000}") long reclaimMinIdleMs,
            @Value("${app.outbox.usage-alerts.reclaim-batch-size:100}") long reclaimBatchSize
    ) {
        this.redis = redis;
        this.kafka = kafka;
        this.om = om;
        this.topic = topic;
        this.streamKey = streamKey;
        this.dlqStreamKey = dlqStreamKey;
        this.metaKeyPrefix = metaKeyPrefix;
        this.group = group;
        this.consumerName = configuredConsumerName;
        this.readCount = readCount;
        this.blockMs = blockMs;
        this.maxAttempts = maxAttempts;
        this.metaTtlSeconds = metaTtlSeconds;
        this.reclaimMinIdleMs = reclaimMinIdleMs;
        this.reclaimBatchSize = reclaimBatchSize;
    }

    @PostConstruct
    public void initializeConsumerGroup() {
        // 애플리케이션 기동 시 consumer group 보장
        ensureGroup();
    }

    // 신규 outbox 엔트리 발행 + reclaim 루프
    @Scheduled(fixedDelayString = "${app.outbox.usage-alerts.poll-delay-ms:500}")
    public void publishFromOutbox() {
        List<MapRecord<String, Object, Object>> newRecords = redis.opsForStream().read(
                Consumer.from(group, consumerName),
                StreamReadOptions.empty()
                        .count(readCount)
                        .block(Duration.ofMillis(blockMs)),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed())
        );
        processRecords(newRecords);

        reclaimPending();
    }

    // 죽은 consumer가 남긴 pending 엔트리를 재할당
    private void reclaimPending() {
        PendingMessages pending = redis.opsForStream().pending(streamKey, group, Range.unbounded(), reclaimBatchSize);
        if (pending == null || pending.isEmpty()) {
            return;
        }

        List<RecordId> staleIds = new ArrayList<>();
        for (PendingMessage msg : pending) {
            Duration elapsed = msg.getElapsedTimeSinceLastDelivery();
            if (elapsed != null && elapsed.toMillis() >= reclaimMinIdleMs) {
                staleIds.add(msg.getId());
            }
        }
        if (staleIds.isEmpty()) {
            return;
        }

        RedisStreamCommands.XClaimOptions options = RedisStreamCommands.XClaimOptions
                .minIdle(Duration.ofMillis(reclaimMinIdleMs))
                .ids(staleIds.toArray(new RecordId[0]));

        List<MapRecord<String, Object, Object>> claimed =
                redis.opsForStream().claim(streamKey, group, consumerName, options);

        processRecords(claimed);
    }

    // 읽어온 레코드를 순차 처리
    private void processRecords(List<MapRecord<String, Object, Object>> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        for (MapRecord<String, Object, Object> record : records) {
            publishSingle(record);
        }
    }

    // Kafka 발행 성공 시에만 XACK
    private void publishSingle(MapRecord<String, Object, Object> record) {
        String streamId = record.getId().getValue();
        Map<Object, Object> value = record.getValue();

        String payload = asString(value.get("payload"));
        String kafkaKey = asString(value.get("kafka_key"));

        if (payload == null || payload.isBlank() || kafkaKey == null || kafkaKey.isBlank()) {
            redis.opsForStream().acknowledge(streamKey, group, record.getId());
            return;
        }

        try {
            UsageAlertEvent event = om.readValue(payload, UsageAlertEvent.class);
            kafka.send(topic, kafkaKey, event).join();

            redis.opsForStream().acknowledge(streamKey, group, record.getId());
            redis.delete(metaKey(streamId));
        } catch (Exception e) {
            handleFailure(record, e);
        }
    }

    // 실패 메타를 누적하고 임계 초과 시 DLQ 이관
    private void handleFailure(MapRecord<String, Object, Object> record, Exception e) {
        String streamId = record.getId().getValue();
        String metaKey = metaKey(streamId);

        Long attempts = redis.opsForHash().increment(metaKey, FIELD_ATTEMPTS, 1L);
        redis.opsForHash().put(metaKey, FIELD_LAST_ERROR, safeErrorMessage(e));
        redis.opsForHash().put(metaKey, FIELD_LAST_FAILED_TIME, Instant.now().toString());
        redis.expire(metaKey, Duration.ofSeconds(metaTtlSeconds));

        long attemptsValue = attempts == null ? 0L : attempts;
        if (attemptsValue > maxAttempts) {
            moveToDlq(record, attemptsValue, e);
            redis.opsForStream().acknowledge(streamKey, group, record.getId());
        }
    }

    // 원본 필드 + 실패 메타를 DLQ stream에 적재
    private void moveToDlq(MapRecord<String, Object, Object> record, long attempts, Exception e) {
        Map<String, String> dlqEntry = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : record.getValue().entrySet()) {
            dlqEntry.put(String.valueOf(entry.getKey()), asString(entry.getValue()));
        }

        dlqEntry.put(FIELD_STREAM_ID, record.getId().getValue());
        dlqEntry.put(FIELD_ATTEMPTS, String.valueOf(attempts));
        dlqEntry.put(FIELD_ERROR, safeErrorMessage(e));
        dlqEntry.put(FIELD_LAST_FAILED_TIME, Instant.now().toString());

        redis.opsForStream().add(dlqStreamKey, dlqEntry);
    }

    // stream/group이 없을 때 seed 레코드로 group 생성
    private void ensureGroup() {
        try {
            redis.opsForStream().createGroup(streamKey, ReadOffset.latest(), group);
            return;
        } catch (Exception e) {
            if (isBusyGroup(e)) {
                return;
            }
            if (!isNoSuchKey(e)) {
                throw e;
            }
        }

        RecordId seedId = redis.opsForStream().add(streamKey, Map.of("_seed", "1"));
        try {
            redis.opsForStream().createGroup(streamKey, ReadOffset.latest(), group);
        } catch (Exception e) {
            if (!isBusyGroup(e)) {
                throw e;
            }
        } finally {
            if (seedId != null) {
                redis.opsForStream().delete(streamKey, seedId);
            }
        }
    }

    private String metaKey(String streamId) {
        return metaKeyPrefix + streamId;
    }

    private String safeErrorMessage(Exception e) {
        String msg = e.getClass().getSimpleName();
        if (e.getMessage() != null && !e.getMessage().isBlank()) {
            msg = msg + ":" + e.getMessage();
        }
        if (msg.length() > 1000) {
            return msg.substring(0, 1000);
        }
        return msg;
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private boolean isBusyGroup(Exception e) {
        return containsInExceptionChain(e, "BUSYGROUP")
                || containsInExceptionChain(e, "RedisBusyException");
    }

    private boolean isNoSuchKey(Exception e) {
        return containsInExceptionChain(e, "requires the key to exist")
                || containsInExceptionChain(e, "NOGROUP")
                || containsInExceptionChain(e, "no such key");
    }

    private boolean containsInExceptionChain(Throwable t, String token) {
        Throwable curr = t;
        while (curr != null) {
            String msg = curr.getMessage();
            if (msg != null && msg.contains(token)) {
                return true;
            }
            String name = curr.getClass().getSimpleName();
            if (name != null && name.contains(token)) {
                return true;
            }
            curr = curr.getCause();
        }
        return false;
    }

}
