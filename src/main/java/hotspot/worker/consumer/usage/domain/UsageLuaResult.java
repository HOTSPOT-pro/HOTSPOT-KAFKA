package hotspot.worker.consumer.usage.domain;

import java.util.List;

/**
 * usage_atomic.lua 실행 결과 DTO
 */
public record UsageLuaResult(
        boolean duplicate,
        long bytes,
        long giftTake,
        long planTake,
        long familyTake,
        long overflow,
        boolean planFired,
        int planThreshold,
        long planRemainingBytes,
        int planRemainingPct,
        boolean familyFired,
        int familyThreshold,
        long familyRemainingBytes,
        int familyRemainingPct,
        List<GiftFire> giftFires
) {
}
