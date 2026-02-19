package hotspot.worker.consumer.consistency.subscription.executor;

import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SubscriptionLuaExecutor {

    private final StringRedisTemplate redisTemplate;

    private final DefaultRedisScript<Long> subscriptionLockScript;
    private final DefaultRedisScript<Long> subscriptionPlanChangedScript;
    private final DefaultRedisScript<Long> subscriptionPolicyScript;
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

    public void executePolicy(
            String eventId,
            long subId,
            String action,
            String policyType,
            long policyId,
            String encoded,
            Long expireEpoch
    ) {
        redisTemplate.execute(
                subscriptionPolicyScript,
                List.of(
                        "idem:sub:" + eventId,
                        "block:repeat:" + subId,
                        "block:time:" + subId
                ),
                action,
                policyType,
                String.valueOf(policyId),
                encoded == null ? "" : encoded,
                expireEpoch == null ? "0" : String.valueOf(expireEpoch)
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
            long giftAmountBytes
    ) {

        List<String> keys = List.of(
                "idem:sub:" + eventId,
                "limit:gift:" + receiverSubId + ":" + giftId + ":" + yyyyMM,
                "idx:gift:" + receiverSubId + ":" + yyyyMM,
                "usage:sub:" + giverSubId + ":" + yyyyMM,
                "usage:sub:" + giverSubId + ":" + yyyyMMDD
        );

        redisTemplate.execute(
                subscriptionGiftScript,
                keys,
                String.valueOf(giftId),
                String.valueOf(giftLimitBytes),
                String.valueOf(giftAmountBytes),
                "plan_used",
                yyyyMM,
                yyyyMMDD
        );
    }

    public void executeAppPolicy(
            String eventId,
            long subId,
            String action,
            long appId
    ) {
        redisTemplate.execute(
                subscriptionAppScript,
                List.of(
                        "idem:sub:" + eventId,
                        "block:app:" + subId
                ),
                action,
                String.valueOf(appId)
        );
    }
}
