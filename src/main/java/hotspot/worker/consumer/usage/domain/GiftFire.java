package hotspot.worker.consumer.usage.domain;

public record GiftFire(
        String giftId,
        int th,
        long rem,
        int pct,
        int last
) {
}
