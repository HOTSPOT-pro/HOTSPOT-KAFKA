package hotspot.worker.consumer.usage.service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.consumer.usage.domain.GiftAllocation;
import hotspot.worker.consumer.usage.domain.GiftFire;
import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.consumer.usage.support.RedisKeyBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class UsageLuaExecutor {

    private static final Logger log = LoggerFactory.getLogger(UsageLuaExecutor.class);
    private static final long PERF_LOG_EVERY = 10_000L;
    private static final long TTL_MON_SECONDS = 15_552_000L;
    private static final long TTL_DAY_SECONDS = 15_552_000L;
    private static final long TTL_NOTIFY_SECONDS = 2_678_400L;
    private static final long TTL_DEDUP_SECONDS = 86_400L;
    private static final long TTL_RESULT_SECONDS = 172_800L;

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<List> usageAtomicScript;
    private final RedisKeyBuilder keyBuilder;
    private final ObjectMapper om;
    private final Timer luaTotalTimer;
    private final Timer luaRedisExecuteTimer;
    private final Timer luaParseTimer;
    private final Counter luaStatusOkCounter;
    private final Counter luaStatusDupCounter;
    private final Counter luaStatusInvalidBytesCounter;
    private final AtomicLong executedCount = new AtomicLong();
    private final AtomicLong totalExecuteNanos = new AtomicLong();
    private final AtomicLong totalRedisExecuteNanos = new AtomicLong();
    private final AtomicLong totalParseNanos = new AtomicLong();

    public UsageLuaExecutor(
            StringRedisTemplate redis,
            DefaultRedisScript<List> usageAtomicScript,
            RedisKeyBuilder keyBuilder,
            ObjectMapper om,
            MeterRegistry meterRegistry
    ) {
        this.redis = redis;
        this.usageAtomicScript = usageAtomicScript;
        this.keyBuilder = keyBuilder;
        this.om = om;
        this.luaTotalTimer = Timer.builder("hotspot.usage.lua.total")
                .description("Total Lua executor time per event")
                .register(meterRegistry);
        this.luaRedisExecuteTimer = Timer.builder("hotspot.usage.lua.redis.execute")
                .description("Redis EVAL roundtrip time per event")
                .register(meterRegistry);
        this.luaParseTimer = Timer.builder("hotspot.usage.lua.parse")
                .description("Lua response parsing time per event")
                .register(meterRegistry);
        this.luaStatusOkCounter = Counter.builder("hotspot.usage.lua.status")
                .tag("status", "ok")
                .register(meterRegistry);
        this.luaStatusDupCounter = Counter.builder("hotspot.usage.lua.status")
                .tag("status", "dup")
                .register(meterRegistry);
        this.luaStatusInvalidBytesCounter = Counter.builder("hotspot.usage.lua.status")
                .tag("status", "invalid_bytes")
                .register(meterRegistry);
    }

    public UsageLuaResult execute(UsageEvent ev) {
        long start = System.nanoTime();
        RedisKeyBuilder.Keys keysPack =
                keyBuilder.build(ev.subId(), ev.familyId(), ev.eventId(), ev.occurredAt());

        List<String> argv = List.of(
                String.valueOf(ev.bytes()),
                String.valueOf(ev.appId()),
                keysPack.yyyymm(),
                keysPack.giftLimitPrefix(),
                keysPack.giftUsagePrefix(),
                keysPack.giftNotifyPrefix(),
                String.valueOf(TTL_MON_SECONDS),
                String.valueOf(TTL_DAY_SECONDS),
                String.valueOf(TTL_NOTIFY_SECONDS),
                String.valueOf(TTL_DEDUP_SECONDS),
                ev.eventId(),
                ev.occurredAt().toString(),
                String.valueOf(ev.subId()),
                String.valueOf(ev.familyId()),
                keysPack.daily3HourlyUsedField(),
                String.valueOf(TTL_RESULT_SECONDS)
        );

        Object[] argvArray = argv.toArray(new Object[0]);
        long redisExecuteStart = System.nanoTime();
        Object raw = redis.execute(usageAtomicScript, keysPack.keys(), argvArray);
        long redisExecuteElapsedNanos = System.nanoTime() - redisExecuteStart;
        @SuppressWarnings("unchecked")
        List<Object> arr = (List<Object>) raw;

        long parseStart = System.nanoTime();
        String status = toStr(arr.get(0));
        UsageLuaResult result;
        if ("INVALID_BYTES".equals(status)) {
            result = ignoredResult();
            luaStatusInvalidBytesCounter.increment();
        } else if ("DUP".equals(status)) {
            if (arr.size() < 2 || toStr(arr.get(1)) == null || toStr(arr.get(1)).isBlank()) {
                throw new IllegalStateException("Lua returned DUP without cached result payload");
            }
            result = parseCachedResult(toStr(arr.get(1)));
            luaStatusDupCounter.increment();
        } else if ("DUP_MISSING_RESULT".equals(status)) {
            throw new IllegalStateException("Lua returned DUP but result cache key is missing");
        } else if (!"OK".equals(status)) {
            throw new IllegalStateException("Lua returned unexpected status: " + status);
        } else {
            long bytes = toLong(arr.get(1));
            long giftTake = toLong(arr.get(2));
            long planTake = toLong(arr.get(3));
            long familyTake = toLong(arr.get(4));
            long overflow = toLong(arr.get(5));
            long planUsedAmount = toLong(arr.get(6));
            long planProvidedAmount = toLong(arr.get(7));
            long familyUsedAmount = toLong(arr.get(8));
            long familyProvidedAmount = toLong(arr.get(9));
            boolean planFire = toLong(arr.get(10)) == 1;
            int planTh = (int) toLong(arr.get(11));
            long planRem = toLong(arr.get(12));
            int planPct = (int) toLong(arr.get(13));

            boolean famFire = toLong(arr.get(15)) == 1;
            int famTh = (int) toLong(arr.get(16));
            long famRem = toLong(arr.get(17));
            int famPct = (int) toLong(arr.get(18));

            List<GiftFire> giftFires = parseGiftFires(toStr(arr.get(20)));
            List<GiftAllocation> giftAllocations = parseGiftAllocations(toStr(arr.get(21)));

            result = new UsageLuaResult(
                    false,
                    bytes, giftTake, planTake, familyTake, overflow,
                    planProvidedAmount, planUsedAmount,
                    familyProvidedAmount, familyUsedAmount,
                    planFire, planTh, planRem, planPct,
                    famFire, famTh, famRem, famPct,
                    giftFires,
                    giftAllocations
            );
            luaStatusOkCounter.increment();
        }
        long parseElapsedNanos = System.nanoTime() - parseStart;

        long elapsedNanos = System.nanoTime() - start;
        luaTotalTimer.record(elapsedNanos, TimeUnit.NANOSECONDS);
        luaRedisExecuteTimer.record(redisExecuteElapsedNanos, TimeUnit.NANOSECONDS);
        luaParseTimer.record(parseElapsedNanos, TimeUnit.NANOSECONDS);
        long count = executedCount.incrementAndGet();
        long totalNanos = totalExecuteNanos.addAndGet(elapsedNanos);
        long totalRedisNanos = totalRedisExecuteNanos.addAndGet(redisExecuteElapsedNanos);
        long totalParseNanosValue = totalParseNanos.addAndGet(parseElapsedNanos);
        if (count % PERF_LOG_EVERY == 0) {
            long avgMicros = (totalNanos / count) / 1000L;
            long avgRedisMicros = (totalRedisNanos / count) / 1000L;
            long avgParseMicros = (totalParseNanosValue / count) / 1000L;
            log.info(
                    "Lua 성능 지표: 처리건수={}, 평균처리={}µs, Redis실행평균={}µs, 파싱평균={}µs",
                    count,
                    avgMicros,
                    avgRedisMicros,
                    avgParseMicros
            );
        }
        return result;
    }

    private UsageLuaResult parseCachedResult(String json) {
        try {
            JsonNode node = om.readTree(json);
            List<GiftAllocation> giftAllocations = List.of();
            if (node.has("giftAllocations")) {
                JsonNode giftAllocationsNode = node.get("giftAllocations");
                if (giftAllocationsNode != null && giftAllocationsNode.isArray()) {
                    giftAllocations = om.convertValue(
                            giftAllocationsNode,
                            new TypeReference<List<GiftAllocation>>() {
                            }
                    );
                }
            }
            return new UsageLuaResult(
                    true,
                    node.path("bytes").asLong(0),
                    node.path("giftTake").asLong(0),
                    node.path("planTake").asLong(0),
                    node.path("familyTake").asLong(0),
                    node.path("overflow").asLong(0),
                    0, 0, 0, 0,
                    false, 101, 0, 0,
                    false, 101, 0, 0,
                    List.of(),
                    giftAllocations
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse cached result payload", e);
        }
    }

    private UsageLuaResult ignoredResult() {
        return new UsageLuaResult(
                true,
                0, 0, 0, 0, 0,
                0, 0, 0, 0,
                false, 101, 0, 0,
                false, 101, 0, 0,
                List.of(),
                List.of()
        );
    }

    private List<GiftFire> parseGiftFires(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            JsonNode node = om.readTree(json);
            if (node == null || node.isNull() || node.isMissingNode()) {
                return List.of();
            }
            if (node.isObject() && node.isEmpty()) {
                return List.of();
            }
            if (node.isArray()) {
                return om.convertValue(node, new TypeReference<List<GiftFire>>() {
                });
            }
            throw new IllegalStateException("Unexpected fired_gifts JSON shape: " + json);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse fired_gifts JSON: " + json, e);
        }
    }

    private List<GiftAllocation> parseGiftAllocations(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            JsonNode node = om.readTree(json);
            if (node == null || node.isNull() || node.isMissingNode()) {
                return List.of();
            }
            if (node.isObject() && node.isEmpty()) {
                return List.of();
            }
            if (node.isArray()) {
                return om.convertValue(node, new TypeReference<List<GiftAllocation>>() {
                });
            }
            throw new IllegalStateException("Unexpected gift_allocations JSON shape: " + json);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse gift_allocations JSON: " + json, e);
        }
    }

    private String toStr(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return o.toString();
    }

    private long toLong(Object o) {
        String s = toStr(o);
        if (s == null || s.isBlank()) {
            return 0L;
        }
        return Long.parseLong(s);
    }
}
