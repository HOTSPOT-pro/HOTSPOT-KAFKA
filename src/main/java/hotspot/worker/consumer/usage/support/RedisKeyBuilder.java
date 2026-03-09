package hotspot.worker.consumer.usage.support;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

/**
 * usage Lua 실행용 Redis 키를 규약대로 생성하는 빌더
 */
public final class RedisKeyBuilder {
    private final ZoneId zone;

    public RedisKeyBuilder(ZoneId zone) {
        this.zone = zone;
    }

    // Lua KEYS와 접두사 파라미터 묶음
    public record Keys(
            List<String> keys,
            String yyyymm,
            String daily3HourlyUsedField,
            String giftLimitPrefix,
            String giftUsagePrefix,
            String giftNotifyPrefix
    ) {
    }

    // 이벤트 기준으로 Lua KEYS/ARGV 구성값을 만듦
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
        String usageAppDay3HourlyKey = "usage:app:" + subId + ":" + yyyymmdd + ":3hourly";

        String notifyPlanMonKey = "notify:sub:" + subId + ":" + yyyymm;
        String notifyPlanDayKey = "notify:sub:" + subId + ":" + yyyymmdd;
        String notifyFamilyMonKey = "notify:family:" + familyId + ":" + yyyymm;
        String dedupKey = "dedup:evt:" + eventId;

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
                usageAppDay3HourlyKey,
                notifyPlanMonKey,
                notifyPlanDayKey,
                notifyFamilyMonKey,
                dedupKey
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
