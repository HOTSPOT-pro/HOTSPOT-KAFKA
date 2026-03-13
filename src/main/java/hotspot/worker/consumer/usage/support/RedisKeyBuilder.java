package hotspot.worker.consumer.usage.support;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

// usage Lua 실행에 필요한 Redis 키/접두어를 생성하는 빌더다.
public final class RedisKeyBuilder {
    private final ZoneId zone;

    // 키 생성 시 사용할 기준 시간대를 주입받는다.
    public RedisKeyBuilder(ZoneId zone) {
        this.zone = zone;
    }

    // Lua KEYS와 ARGV 조합에 필요한 파생 값을 묶는다.
    public record Keys(
            List<String> keys,
            String yyyymm,
            String daily3HourlyUsedField,
            String giftLimitPrefix,
            String giftUsagePrefix,
            String giftNotifyPrefix
    ) {
    }

    // 이벤트 정보를 기준으로 Lua 실행에 필요한 키/접두어를 생성한다.
    public Keys build(long subId, long familyId, String eventId, Instant occurredAt) {
        String yyyymm = TimeKey.yyyymm(occurredAt, zone);
        String yyyymmdd = TimeKey.yyyymmdd(occurredAt, zone);

        String limitSubKey = "limit:sub:" + subId;
        String limitFamilyKey = "limit:family:" + familyId;
        String limitFamilySubKey = "limit:family_sub:" + familyId + ":" + subId;
        String giftIdxKey = "idx:gift:" + subId + ":" + yyyymm;

        String usageSubMonKey = "usage:sub:" + subId + ":" + yyyymm;
        String usageSubDayKey = "usage:sub:" + subId + ":" + yyyymmdd;
        String usageFamilyMonKey = "usage:family:" + familyId + ":" + yyyymm;
        String usageFamilyDayKey = "usage:family:" + familyId + ":" + yyyymmdd;
        String usageAppMonKey = "usage:app:" + subId + ":" + yyyymm;
        String usageAppDayKey = "usage:app:" + subId + ":" + yyyymmdd;
        String usage3HourlyDayKey = "usage:3hourly:" + subId + ":" + yyyymmdd;

        String notifyPlanMonKey = "notify:sub:" + subId + ":" + yyyymm;
        String notifyPlanDayKey = "notify:sub:" + subId + ":" + yyyymmdd;
        String notifyFamilyMonKey = "notify:family:" + familyId + ":" + yyyymm;
        String dedupKey = "dedup:evt:" + eventId;
        String resultKey = "result:evt:" + eventId;

        List<String> keys = List.of(
                limitSubKey,
                limitFamilyKey,
                limitFamilySubKey,
                giftIdxKey,
                usageSubMonKey,
                usageSubDayKey,
                usageFamilyMonKey,
                usageFamilyDayKey,
                usageAppMonKey,
                usageAppDayKey,
                usage3HourlyDayKey,
                notifyPlanMonKey,
                notifyPlanDayKey,
                notifyFamilyMonKey,
                dedupKey,
                resultKey
        );

        String giftLimitPrefix = "limit:gift:" + subId + ":";
        String giftUsagePrefix = "usage:gift:" + subId + ":";
        String giftNotifyPrefix = "notify:gift:" + subId + ":";

        return new Keys(
                keys,
                yyyymm,
                TimeKey.daily3HourlyUsedField(occurredAt, zone),
                giftLimitPrefix,
                giftUsagePrefix,
                giftNotifyPrefix
        );
    }
}
