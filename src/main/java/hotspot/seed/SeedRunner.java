package hotspot.seed;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

/**
 * 로컬 Redis에 정책/한도 시드 데이터를 적재하는 전용 실행기
 */
@SpringBootApplication
public class SeedRunner {

    private static final int PIPELINE_BATCH_SIZE = 1000;

    public static void main(String[] args) {
        // SeedRunner 단독 실행 진입점
        SpringApplication.run(SeedRunner.class, args);
    }

    @Bean
        // 애플리케이션 시작 시 시드 전체를 순서대로 수행
    CommandLineRunner run(JdbcTemplate jdbc, StringRedisTemplate redis) {
        return ignored -> {
            deleteByPattern(redis, "limit:sub:*");
            deleteByPattern(redis, "limit:family:*");
            deleteByPattern(redis, "limit:family_sub:*");
            deleteByPattern(redis, "limit:gift:*");
            deleteByPattern(redis, "idx:gift:*");

            deleteByPattern(redis, "block:repeat:*");
            deleteByPattern(redis, "block:time:*");
            deleteByPattern(redis, "block:immediate:*");
            deleteByPattern(redis, "block:app:*");
            deleteByPattern(redis, "priority:family:*");

            redis.delete("idx:sub:family");
            redis.delete("idx:family:subs");

            seedPlanLimit(jdbc, redis);
            seedFamilyLimit(jdbc, redis);
            seedFamilySubLimitPriorityAndIndexes(jdbc, redis);
            seedPresentsAndDonorUsage(jdbc, redis);

            seedBlockRepeat(jdbc, redis);
            seedBlockTime(jdbc, redis);
            seedBlockImmediate(jdbc, redis);
            seedBlockApp(jdbc, redis);

            System.out.println("Seed done.");
        };
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // 패턴에 해당하는 Redis 키를 일괄 삭제
    private static void deleteByPattern(StringRedisTemplate redis, String pattern) {
        var keys = redis.keys(pattern);
        if (keys != null && !keys.isEmpty()) {
            redis.delete(keys);
        }
    }

    // 구독 플랜 한도(limit:sub)를 적재
    private void seedPlanLimit(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
                SELECT s.sub_id AS sub_id,
                       p.plan_data_amount AS plan_limit_kb
                FROM subscription s
                JOIN plan p
                  ON p.plan_id = s.plan_id
                WHERE s.is_deleted = false
                  AND p.is_deleted = false
                """;

        List<PlanLimitRow> rows = jdbc.query(sql, (rs, rowNum) ->
                new PlanLimitRow(rs.getLong("sub_id"), rs.getLong("plan_limit_kb"))
        );

        forEachBatch(rows, PIPELINE_BATCH_SIZE, batch -> {
            redis.executePipelined((RedisCallback<Object>) conn -> {
                for (PlanLimitRow row : batch) {
                    String key = "limit:sub:" + row.subId();
                    conn.hSet(b(key), b("plan_limit"), b(Long.toString(row.planLimitKb())));
                }
                return null;
            });
        });
    }

    private record PlanLimitRow(long subId, long planLimitKb) {
    }

    private static <T> void forEachBatch(List<T> items, int batchSize, Consumer<List<T>> batchConsumer) {
        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            batchConsumer.accept(items.subList(i, end));
        }
    }

    private static <T> void writeInBatches(
            StringRedisTemplate redis,
            List<T> items,
            int batchSize,
            BiConsumer<RedisConnection, T> writer
    ) {
        forEachBatch(items, batchSize, currentBatch -> {
            redis.executePipelined((RedisCallback<Object>) conn -> {
                for (T item : currentBatch) {
                    writer.accept(conn, item);
                }
                return null;
            });
        });
    }

    // 가족 풀 한도(limit:family)를 적재
    private void seedFamilyLimit(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
                SELECT DISTINCT fs.family_id,
                                f.family_data_amount AS family_limit_kb
                FROM family_sub fs
                JOIN family f
                  ON f.family_id = fs.family_id
                WHERE f.is_deleted = false
                """;

        List<FamilyLimitRow> rows = jdbc.query(sql, (rs, rowNum) ->
                new FamilyLimitRow(rs.getLong("family_id"), rs.getLong("family_limit_kb"))
        );

        writeInBatches(redis, rows, PIPELINE_BATCH_SIZE, (conn, row) -> {
            String key = "limit:family:" + row.familyId();
            conn.hSet(b(key), b("family_limit"), b(Long.toString(row.familyLimitKb())));
        });
    }

    private record FamilyLimitRow(long familyId, long familyLimitKb) {
    }

    // 가족 구성원별 한도/우선순위 및 인덱스를 적재
    private void seedFamilySubLimitPriorityAndIndexes(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
                SELECT family_id,
                       sub_id,
                       priority,
                       data_limit
                FROM family_sub
                """;

        List<FamilySubRow> rows = jdbc.query(sql, (rs, rowNum) ->
                new FamilySubRow(
                        rs.getLong("family_id"),
                        rs.getLong("sub_id"),
                        rs.getInt("priority"),
                        rs.getLong("data_limit")
                )
        );

        writeInBatches(redis, rows, PIPELINE_BATCH_SIZE, (conn, row) -> {
            conn.hSet(b("idx:sub:family"), b(Long.toString(row.subId())), b(Long.toString(row.familyId())));

            // 🔥 모든 family 소속 subId를 하나의 SET에 저장
            conn.sAdd(
                    b("idx:family:subs"),
                    b(Long.toString(row.subId()))
            );

            if (row.priority() >= 0) {
                String prioKey = "priority:family:" + row.familyId();
                conn.zAdd(b(prioKey), (double) row.priority(), b(Long.toString(row.subId())));
            }

            String limitKey = "limit:family_sub:" + row.familyId() + ":" + row.subId();
            String familyLimitValue = (row.dataLimitKb() < 0) ? "-1" : Long.toString(row.dataLimitKb());
            conn.hSet(b(limitKey), b("family_limit"), b(familyLimitValue));
            conn.hSet(b(limitKey), b("priority"), b(Integer.toString(row.priority())));
        });
    }

    private record FamilySubRow(long familyId, long subId, int priority, long dataLimitKb) {
    }

    // 선물 데이터 한도와 기부자 사용량을 초기화
    private void seedPresentsAndDonorUsage(
            JdbcTemplate jdbc,
            StringRedisTemplate redis
    ) {
        String giftAppId = resolveGiftAppId(jdbc);
        String sql = """
                SELECT present_data_id AS present_data_id,
                       target_sub_id,
                       provide_sub_id,
                       data_amount,
                       created_time
                FROM present_data
                """;

        List<PresentRow> rows = jdbc.query(sql, (rs, rowNum) ->
                new PresentRow(
                        rs.getLong("present_data_id"),
                        rs.getLong("target_sub_id"),
                        rs.getLong("provide_sub_id"),
                        rs.getLong("data_amount"),
                        rs.getTimestamp("created_time").toLocalDateTime()
                )
        );

        writeInBatches(redis, rows, PIPELINE_BATCH_SIZE, (conn, row) -> {
            long prioEpoch = row.createdTime().atZone(ZoneId.of("Asia/Seoul")).toEpochSecond();
            String yyyymm = row.createdTime().format(DateTimeFormatter.ofPattern("yyyyMM"));
            String yyyymmdd = row.createdTime().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            String giftId = Long.toString(row.presentId());

            String limitGiftKey = "limit:gift:" + row.targetSubId() + ":" + giftId + ":" + yyyymm;
            conn.hSet(b(limitGiftKey), b("gift_limit"), b(Long.toString(row.dataAmountKb())));

            String idxGiftKey = "idx:gift:" + row.targetSubId() + ":" + yyyymm;
            conn.zAdd(b(idxGiftKey), (double) prioEpoch, b(giftId));

            String donorMonKey = "usage:sub:" + row.provideSubId() + ":" + yyyymm;
            conn.hIncrBy(b(donorMonKey), b("plan_used"), row.dataAmountKb());

            String donorDayKey = "usage:sub:" + row.provideSubId() + ":" + yyyymmdd;
            conn.hIncrBy(b(donorDayKey), b("plan_used"), row.dataAmountKb());

            String donorMonAppKey = "usage:app:" + row.provideSubId() + ":" + yyyymm;
            conn.zIncrBy(b(donorMonAppKey), row.dataAmountKb(), b(giftAppId));

            String donorDayAppKey = "usage:app:" + row.provideSubId() + ":" + yyyymmdd;
            conn.zIncrBy(b(donorDayAppKey), row.dataAmountKb(), b(giftAppId));

            String usageGiftKey = "usage:gift:" + row.targetSubId() + ":" + giftId + ":" + yyyymm;
            conn.hSetNX(b(usageGiftKey), b("gift_used"), b("0"));
        });
    }

    private record PresentRow(
            long presentId,
            long targetSubId,
            long provideSubId,
            long dataAmountKb,
            LocalDateTime createdTime
    ) {
    }

    private String resolveGiftAppId(JdbcTemplate jdbc) {
        String sql = """
                SELECT abs.app_blocked_service_id
                FROM app_blocked_service abs
                WHERE abs.is_deleted = false
                ORDER BY abs.app_blocked_service_id
                LIMIT 1
                """;
        String appId = jdbc.query(sql, rs -> rs.next() ? rs.getString("app_blocked_service_id") : null);
        if (!StringUtils.hasText(appId)) {
            throw new IllegalStateException("Gift app id not found in app_blocked_service");
        }
        return appId;
    }

    // 반복 차단 정책(block:repeat) 적재
    private void seedBlockRepeat(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
                SELECT ps.sub_id,
                       ps.block_policy_id AS policy_id,
                       COALESCE(
                         NULLIF(s.snap ->> 'days_csv', ''),
                         NULLIF(array_to_string(ARRAY(
                           SELECT jsonb_array_elements_text(s.snap -> 'data' -> 'days')
                         ), ','), ''),
                         NULLIF(array_to_string(ARRAY(
                           SELECT jsonb_array_elements_text(s.snap -> 'days')
                         ), ','), ''),
                         ''
                       ) AS days_csv,
                       COALESCE(
                         NULLIF(s.snap ->> 'start_hhmm', ''),
                         NULLIF(s.snap -> 'data' ->> 'startTime', ''),
                         NULLIF(s.snap ->> 'startTime', ''),
                         ''
                       ) AS start_hhmm,
                       COALESCE(
                         NULLIF(s.snap ->> 'end_hhmm', ''),
                         NULLIF(s.snap -> 'data' ->> 'endTime', ''),
                        NULLIF(s.snap ->> 'endTime', ''),
                         ''
                        ) AS end_hhmm
                FROM policy_sub ps
                JOIN block_policy bp
                  ON bp.block_policy_id = ps.block_policy_id
                CROSS JOIN LATERAL (
                    SELECT COALESCE(bp.policy_snapshot::jsonb, '{}'::jsonb) AS snap
                ) s
                WHERE ps.is_active = true
                  AND bp.is_deleted = false
                  AND bp.is_active = true
                  AND UPPER(COALESCE(bp.policy_type::text, s.snap ->> 'policyType', s.snap ->> 'policy_type', '')) IN ('SCHEDULED')
                """;

        List<BlockRepeatRow> rows = jdbc.query(sql, (rs, rowNum) ->
                new BlockRepeatRow(
                        rs.getLong("sub_id"),
                        rs.getString("policy_id"),
                        rs.getString("days_csv"),
                        rs.getString("start_hhmm"),
                        rs.getString("end_hhmm")
                )
        );

        writeInBatches(redis, rows, PIPELINE_BATCH_SIZE, (conn, row) -> {
            String value = normalizeDaysCsv(row.daysCsv())
                    + "|"
                    + row.startHhmm()
                    + "|"
                    + row.endHhmm();
            conn.hSet(b("block:repeat:" + row.subId()), b(row.policyId()), b(value));
        });
    }

    private record BlockRepeatRow(
            long subId,
            String policyId,
            String daysCsv,
            String startHhmm,
            String endHhmm
    ) {
    }

    private static String normalizeDaysCsv(String daysCsv) {
        if (!StringUtils.hasText(daysCsv)) {
            return "";
        }
        String[] tokens = daysCsv.split(",");
        List<String> normalized = new ArrayList<>(tokens.length);
        for (String token : tokens) {
            String t = token.trim();
            if (t.isEmpty()) {
                continue;
            }
            normalized.add(mapDayTokenToNumber(t));
        }
        return String.join(",", normalized);
    }

    private static String mapDayTokenToNumber(String token) {
        String upper = token.toUpperCase();
        return switch (upper) {
            case "MON" -> "1";
            case "TUE" -> "2";
            case "WED" -> "3";
            case "THU" -> "4";
            case "FRI" -> "5";
            case "SAT" -> "6";
            case "SUN" -> "7";
            default -> token;
        };
    }

    // 기간 차단 정책(block:time) 적재
    private void seedBlockTime(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
                SELECT ps.sub_id,
                       ps.block_policy_id AS policy_id,
                       COALESCE(
                         NULLIF(s.snap ->> 'expire_epoch', ''),
                         NULLIF(s.snap -> 'data' ->> 'endTime', ''),
                         NULLIF(s.snap ->> 'endTime', '')
                       ) AS once_end_value
                FROM policy_sub ps
                JOIN block_policy bp
                  ON bp.block_policy_id = ps.block_policy_id
                CROSS JOIN LATERAL (
                    SELECT COALESCE(bp.policy_snapshot::jsonb, '{}'::jsonb) AS snap
                ) s
                WHERE ps.is_active = true
                  AND bp.is_deleted = false
                  AND bp.is_active = true
                  AND UPPER(COALESCE(bp.policy_type::text, s.snap ->> 'policyType', s.snap ->> 'policy_type', '')) = 'ONCE'
                """;

        List<BlockTimeRow> rows = jdbc.query(sql, (rs, rowNum) ->
                new BlockTimeRow(
                        rs.getLong("sub_id"),
                        rs.getString("policy_id"),
                        rs.getString("once_end_value")
                )
        );

        writeInBatches(redis, rows, PIPELINE_BATCH_SIZE, (conn, row) -> {
            long expireEpoch = parseOnceEndToEpoch(row.onceEndValue());
            if (expireEpoch > 0) {
                conn.zAdd(b("block:time:" + row.subId()), (double) expireEpoch, b(row.policyId()));
            }
        });
    }

    private record BlockTimeRow(long subId, String policyId, String onceEndValue) {
    }

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter ONCE_DT_DOT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy.MM.dd'T'HH:mm")
            .optionalStart()
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .optionalEnd()
            .toFormatter();
    private static final DateTimeFormatter ONCE_DT_DASH = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm")
            .optionalStart()
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .optionalEnd()
            .toFormatter();

    private static long parseOnceEndToEpoch(String value) {
        if (value == null) {
            return 0L;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return 0L;
        }
        if (trimmed.chars().allMatch(Character::isDigit)) {
            return Long.parseLong(trimmed);
        }
        try {
            return LocalDateTime.parse(trimmed, ONCE_DT_DOT).atZone(KST).toEpochSecond();
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDateTime.parse(trimmed, ONCE_DT_DASH).atZone(KST).toEpochSecond();
        } catch (DateTimeParseException ignored) {
            return 0L;
        }
    }

    // 즉시 차단 정책(block:immediate) 적재
    private void seedBlockImmediate(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
                SELECT s.sub_id
                FROM subscription s
                WHERE s.is_deleted = false
                  AND s.is_locked = true
                """;

        List<Long> subIds = jdbc.query(sql, (rs, rowNum) -> rs.getLong("sub_id"));

        writeInBatches(redis, subIds, PIPELINE_BATCH_SIZE, (conn, subId) -> {
            conn.set(b("block:immediate:" + subId), b("1"));
        });
    }

    // 앱 차단 목록(block:app)을 적재
    private void seedBlockApp(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
                SELECT bss.sub_id,
                       abs.app_blocked_service_id AS app_id
                FROM blocked_service_sub bss
                JOIN app_blocked_service abs
                  ON abs.app_blocked_service_id = bss.blocked_service_id
                WHERE bss.is_deleted = false
                  AND abs.is_deleted = false
                """;

        List<BlockAppRow> rows = jdbc.query(sql, (rs, rowNum) ->
                new BlockAppRow(rs.getLong("sub_id"), rs.getString("app_id"))
        );

        writeInBatches(redis, rows, PIPELINE_BATCH_SIZE, (conn, row) -> {
            conn.sAdd(b("block:app:" + row.subId()), b(row.appId()));
        });
    }

    private record BlockAppRow(long subId, String appId) {
    }
}
