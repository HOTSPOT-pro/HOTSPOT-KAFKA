package hotspot.worker.writer.infrastructure;

import java.sql.Timestamp;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import hotspot.worker.writer.infrastructure.entity.UsageAppliedEventLogEntity;

@Component
public class UsageAppliedEventLogJdbcWriter {

    private static final String INSERT_SQL = """
            INSERT INTO usage_applied_event_log
                (
                    event_id, sub_id, family_id, app_id, occurred_at, yyyymm,
                    yyyymmdd, usage_amount, gift_used, plan_used, family_used, gift_detail_json
                )
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))
            ON CONFLICT (event_id) DO NOTHING
            """;

    private final JdbcTemplate usageDbJdbcTemplate;

    // 사용량 전용 DB JdbcTemplate을 주입받는다.
    public UsageAppliedEventLogJdbcWriter(
            @Qualifier("usageDbJdbcTemplate") JdbcTemplate usageDbJdbcTemplate
    ) {
        this.usageDbJdbcTemplate = usageDbJdbcTemplate;
    }

    // 사용량 적용 로그를 usage DB에 배치 INSERT한다.
    public void saveAll(List<UsageAppliedEventLogEntity> logs) {
        if (logs == null || logs.isEmpty()) {
            return;
        }

        usageDbJdbcTemplate.batchUpdate(
                INSERT_SQL,
                logs,
                logs.size(),
                (ps, log) -> {
                    ps.setString(1, log.getEventId());
                    ps.setLong(2, log.getSubId());
                    ps.setLong(3, log.getFamilyId());
                    if (log.getAppId() == null) {
                        ps.setNull(4, java.sql.Types.BIGINT);
                    } else {
                        ps.setLong(4, log.getAppId());
                    }
                    ps.setTimestamp(5, Timestamp.valueOf(log.getOccurredAt()));
                    ps.setString(6, log.getYyyymm());
                    ps.setString(7, log.getYyyymmdd());
                    ps.setLong(8, log.getUsageAmount());
                    ps.setLong(9, log.getGiftUsed());
                    ps.setLong(10, log.getPlanUsed());
                    ps.setLong(11, log.getFamilyUsed());
                    ps.setString(12, log.getGiftDetailJson());
                }
        );
    }
}
