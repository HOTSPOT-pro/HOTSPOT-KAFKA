package hotspot.worker.consumer.usage.service;

import java.nio.charset.StandardCharsets;
import java.util.List;

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

@Service
public class UsageLuaExecutor {

    private static final long TTL_MON_SECONDS = 15_552_000L;
    private static final long TTL_DAY_SECONDS = 15_552_000L;
    private static final long TTL_NOTIFY_SECONDS = 2_678_400L;
    private static final long TTL_DEDUP_SECONDS = 86_400L;
    private static final long TTL_RESULT_SECONDS = 172_800L;

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<List> usageAtomicScript;
    private final RedisKeyBuilder keyBuilder;
    private final ObjectMapper om;

    // Lua 실행과 결과 파싱에 필요한 의존성을 주입받는다.
    public UsageLuaExecutor(
            StringRedisTemplate redis,
            DefaultRedisScript<List> usageAtomicScript,
            RedisKeyBuilder keyBuilder,
            ObjectMapper om
    ) {
        this.redis = redis;
        this.usageAtomicScript = usageAtomicScript;
        this.keyBuilder = keyBuilder;
        this.om = om;
    }

    // usage 이벤트를 Lua 스크립트로 원자 처리하고 결과를 DTO로 변환한다.
    public UsageLuaResult execute(UsageEvent ev) {
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
        Object raw = redis.execute(usageAtomicScript, keysPack.keys(), argvArray);
        @SuppressWarnings("unchecked")
        List<Object> arr = (List<Object>) raw;

        String status = toStr(arr.get(0));
        if ("INVALID_BYTES".equals(status)) {
            return ignoredResult();
        }
        if ("DUP".equals(status)) {
            if (arr.size() < 2 || toStr(arr.get(1)) == null || toStr(arr.get(1)).isBlank()) {
                throw new IllegalStateException("Lua returned DUP without cached result payload");
            }
            return parseCachedResult(toStr(arr.get(1)));
        }
        if ("DUP_MISSING_RESULT".equals(status)) {
            throw new IllegalStateException("Lua returned DUP but result cache key is missing");
        }
        if (!"OK".equals(status)) {
            throw new IllegalStateException("Lua returned unexpected status: " + status);
        }

        long bytes = toLong(arr.get(1));
        long giftTake = toLong(arr.get(2));
        long planTake = toLong(arr.get(3));
        long familyTake = toLong(arr.get(4));
        long overflow = toLong(arr.get(5));
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

        return new UsageLuaResult(
                false,
                bytes, giftTake, planTake, familyTake, overflow,
                planFire, planTh, planRem, planPct,
                famFire, famTh, famRem, famPct,
                giftFires,
                giftAllocations
        );
    }

    // DUP 결과에 포함된 캐시 JSON을 UsageLuaResult로 복원한다.
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
                    false,
                    node.path("bytes").asLong(0),
                    node.path("giftTake").asLong(0),
                    node.path("planTake").asLong(0),
                    node.path("familyTake").asLong(0),
                    node.path("overflow").asLong(0),
                    false, 101, 0, 0,
                    false, 101, 0, 0,
                    List.of(),
                    giftAllocations
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse cached result payload", e);
        }
    }

    // INVALID_BYTES 응답을 무시 결과 DTO로 변환한다.
    private UsageLuaResult ignoredResult() {
        return new UsageLuaResult(
                true,
                0, 0, 0, 0, 0,
                false, 101, 0, 0,
                false, 101, 0, 0,
                List.of(),
                List.of()
        );
    }

    // Lua의 fired_gifts JSON 배열을 도메인 리스트로 파싱한다.
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

    // Lua의 gift_allocations JSON 배열을 도메인 리스트로 파싱한다.
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

    // Lua 반환값(Object/byte[])을 문자열로 정규화한다.
    private String toStr(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return o.toString();
    }

    // Lua 반환값을 long 값으로 변환한다.
    private long toLong(Object o) {
        String s = toStr(o);
        if (s == null || s.isBlank()) {
            return 0L;
        }
        return Long.parseLong(s);
    }
}
