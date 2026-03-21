package hotspot.worker.writer.infrastructure;

import java.sql.Timestamp;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import hotspot.worker.writer.log.dto.UsageAppliedFailedEvent;

@Component
public class UsageAppliedFailedEventJdbcWriter {

    private static final String INSERT_SQL = """
            INSERT INTO usage_applied_failed_event
                (event_id, payload, exception_type, exception_message, failed_at)
            VALUES
                (?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    private final JdbcTemplate usageDbJdbcTemplate;

    public UsageAppliedFailedEventJdbcWriter(
            @Qualifier("usageDbJdbcTemplate") JdbcTemplate usageDbJdbcTemplate
    ) {
        this.usageDbJdbcTemplate = usageDbJdbcTemplate;
    }

    public void saveAll(List<UsageAppliedFailedEvent> failures) {
        if (failures == null || failures.isEmpty()) {
            return;
        }
        usageDbJdbcTemplate.batchUpdate(
                INSERT_SQL,
                failures,
                failures.size(),
                (ps, item) -> {
                    ps.setString(1, item.eventId());
                    ps.setString(2, item.payload());
                    ps.setString(3, item.exceptionType());
                    ps.setString(4, item.exceptionMessage());
                    ps.setTimestamp(5, Timestamp.valueOf(item.failedAt()));
                }
        );
    }
}
