package hotspot.worker.consumer.consistency.subscription.executor;

import static hotspot.worker.common.util.JsonNodeUtils.requireLong;
import static hotspot.worker.common.util.JsonNodeUtils.requireText;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SubscriptionLuaExecutor {

    private final StringRedisTemplate redisTemplate;

    private final DefaultRedisScript<Long> subscriptionLockScript;
    private final DefaultRedisScript<Long> subscriptionPlanChangedScript;
    private final DefaultRedisScript<Long> subscriptionPolicySnapshotScript;
    private final DefaultRedisScript<Long> subscriptionGiftScript;
    private final DefaultRedisScript<Long> subscriptionAppScript;

    public void executeLock(String eventId, long subId, String action) {
        redisTemplate.execute(
                subscriptionLockScript,
                List.of(
                        "idem:sub:" + eventId,
                        "block:immediate:" + subId
                ),
                action
        );
    }

    public void executePlanChanged(String eventId, long subId, long limit) {
        redisTemplate.execute(
                subscriptionPlanChangedScript,
                List.of(
                        "idem:sub:" + eventId,
                        "limit:sub:" + subId
                ),
                String.valueOf(limit)
        );
    }

    public void replacePolicies(String eventId, long subId, JsonNode policiesNode) {

        List<String> keys = List.of(
                "idem:sub:" + eventId,
                "block:repeat:" + subId,
                "block:time:" + subId
        );

        // ARGV는 4개 단위로 평탄화:
        // [policyId, policyType, encoded, expireEpoch] 반복
        List<String> argv = new ArrayList<>();

        for (JsonNode p : policiesNode) {
            long policyId = requireLong(p, "policyId");
            String policyType = requireText(p, "policyType");

            if (!"SCHEDULED".equals(policyType) && !"ONCE".equals(policyType)) {
                throw new IllegalArgumentException("Invalid policyType: " + policyType + ", policy=" + p);
            }

            String encoded = "";
            String expireEpoch = "0";

            if ("SCHEDULED".equals(policyType)) {
                encoded = requireText(p, "encoded");
            } else {
                // expireEpoch는 number로 오는 걸 권장
                expireEpoch = String.valueOf(requireLong(p, "expireEpoch"));
            }

            argv.add(String.valueOf(policyId));
            argv.add(policyType);
            argv.add(encoded);
            argv.add(expireEpoch);
        }

        redisTemplate.execute(
                subscriptionPolicySnapshotScript,
                keys,
                argv.toArray()
        );
    }

    public void executeGift(
            String eventId,
            long receiverSubId,
            long giftId,
            String yyyyMM,
            long giftLimitBytes,
            long giverSubId,
            String yyyyMMDD,
            long giftAmountBytes,
            long appId
    ) {

        List<String> keys = List.of(
                "idem:sub:" + eventId,
                "limit:gift:" + receiverSubId + ":" + giftId + ":" + yyyyMM,
                "idx:gift:" + receiverSubId + ":" + yyyyMM,
                "usage:sub:" + giverSubId + ":" + yyyyMM,
                "usage:sub:" + giverSubId + ":" + yyyyMMDD,
                "usage:app:" + giverSubId + ":" + yyyyMM,
                "usage:app:" + giverSubId + ":" + yyyyMMDD
        );

        redisTemplate.execute(
                subscriptionGiftScript,
                keys,
                String.valueOf(giftId),
                String.valueOf(giftLimitBytes),
                String.valueOf(giftAmountBytes),
                "plan_used",
                yyyyMM,
                yyyyMMDD,
                String.valueOf(appId)
        );
    }

    public void executeAppPolicySnapshot(
            String eventId,
            long subId,
            List<String> appIds
    ) {

        List<String> keys = List.of(
                "idem:sub:" + eventId,
                "block:app:" + subId
        );

        redisTemplate.execute(
                subscriptionAppScript,
                keys,
                appIds.toArray()
        );
    }
}
