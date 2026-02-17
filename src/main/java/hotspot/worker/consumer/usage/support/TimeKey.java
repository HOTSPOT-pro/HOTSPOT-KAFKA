package hotspot.worker.consumer.usage.support;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * 이벤트 시각을 Redis 키 날짜 포맷으로 변환하는 유틸
 */
public final class TimeKey {

    // 유틸 클래스 인스턴스화 방지
    private TimeKey() {
    }

    // YYYYMM 문자열을 반환
    public static String yyyymm(Instant ts, ZoneId zone) {
        ZonedDateTime z = ts.atZone(zone);
        return String.format("%04d%02d", z.getYear(), z.getMonthValue());
    }

    // YYYYMMDD 문자열을 반환
    public static String yyyymmdd(Instant ts, ZoneId zone) {
        ZonedDateTime z = ts.atZone(zone);
        return String.format("%04d%02d%02d", z.getYear(), z.getMonthValue(), z.getDayOfMonth());
    }
}
