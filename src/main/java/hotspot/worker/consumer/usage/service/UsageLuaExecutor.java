package hotspot.worker.consumer.usage.service;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.consumer.usage.domain.GiftFire;
import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.consumer.usage.support.RedisKeyBuilder;

/**
 * usage_atomic.lua 실행과 결과 파싱을 담당하는 서비스
 */
@Service
public class UsageLuaExecutor {

    private static final long TTL_MON_SECONDS = 2_678_400L;
    private static final long TTL_DAY_SECONDS = 172_800L;
    private static final long TTL_NOTIFY_SECONDS = 2_678_400L;
    private static final long TTL_DEDUP_SECONDS = 86_400L;
    private static final long OUTBOX_STREAM_MAXLEN_APPROX = 200_000L;

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<List> usageAtomicScript;
    private final RedisKeyBuilder keyBuilder;
    private final ObjectMapper om;

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
                String.valueOf(OUTBOX_STREAM_MAXLEN_APPROX)
        );

        Object[] argvArray = argv.toArray(new Object[0]);
        Object raw = redis.execute(usageAtomicScript, keysPack.keys(), argvArray);
        @SuppressWarnings("unchecked")
        List<Object> arr = (List<Object>) raw;

        // dedup 키가 이미 존재하면 DUP 상태로 반환됨
        if (arr.size() == 1 && "DUP".equals(toStr(arr.get(0)))) {
            return new UsageLuaResult(
                    true,
                    0, 0, 0, 0, 0,
                    false, 101, 0, 0,
                    false, 101, 0, 0,
                    List.of()
            );
        }

        String status = toStr(arr.get(0));
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

        String firedJson = toStr(arr.get(20));
        List<GiftFire> giftFires = parseGiftFires(firedJson);

        return new UsageLuaResult(
                false,
                bytes, giftTake, planTake, familyTake, overflow,
                planFire, planTh, planRem, planPct,
                famFire, famTh, famRem, famPct,
                giftFires
        );
    }

    // fired_gifts JSON을 GiftFire 리스트로 변환
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

    // Redis 반환값을 문자열로 변환
    private String toStr(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return o.toString();
    }

    // Redis 반환값을 숫자로 변환
    private long toLong(Object o) {
        String s = toStr(o);
        if (s == null || s.isBlank()) {
            return 0L;
        }
        return Long.parseLong(s);
    }
}
