package hotspot.worker.consumer.domain;

/**
 * fired_gifts 항목 1건을 담는 DTO.
 */
public record GiftFire(
        String giftId,
        int th,
        long rem,
        int pct,
        int last
) {
}