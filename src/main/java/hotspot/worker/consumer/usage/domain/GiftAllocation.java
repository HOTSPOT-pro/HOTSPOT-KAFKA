package hotspot.worker.consumer.usage.domain;

public record GiftAllocation(
        String giftId,
        long usedAmount
) {
}
