package hotspot.worker.consumer.usage.domain;

import java.util.List;

// usage_atomic.lua 실행 결과를 담는 DTO다.
public record UsageLuaResult(
        boolean duplicate,
        long bytes,
        long giftTake,
        long planTake,
        long familyTake,
        long overflow,
        long planProvidedAmount,
        long planUsedAmount,
        long familyProvidedAmount,
        long familyUsedAmount,
        boolean planFired,
        int planThreshold,
        long planRemainingBytes,
        int planRemainingPct,
        boolean familyFired,
        int familyThreshold,
        long familyRemainingBytes,
        int familyRemainingPct,
        List<GiftFire> giftFires,
        List<GiftAllocation> giftAllocations
) {
}
