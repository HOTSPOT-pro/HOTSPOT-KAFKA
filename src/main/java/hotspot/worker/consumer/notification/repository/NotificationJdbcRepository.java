package hotspot.worker.consumer.notification.repository;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class NotificationJdbcRepository {

    private final JdbcTemplate jdbcTemplate;

    public List<Long> findFamilyMembers(Long familyId) {
        String sql = """
            SELECT sub_id
            FROM family_sub
            WHERE family_id = ?
        """;

        return jdbcTemplate.queryForList(sql, Long.class, familyId);
    }

    public void insert(
            Long subId,
            String type,
            String content,
            String eventId
    ) {
            String sql = """
            INSERT INTO notification
            (sub_id, notification_type, notification_content, event_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (event_id, sub_id)
            DO NOTHING
        """;

        jdbcTemplate.update(
                sql,
                subId,
                type,
                content,
                eventId
        );
    }
}
