package hotspot.worker.apps;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

@SpringBootApplication
public class SeedRunner {

    // 시드 전용 애플리케이션 진입점
    public static void main(String[] args) {
        SpringApplication.run(SeedRunner.class, args);
    }

    // 실행 인자를 읽고 전체 시드 작업을 순서대로 수행
    @Bean
    CommandLineRunner run(JdbcTemplate jdbc, StringRedisTemplate redis, ApplicationArguments args) {
        return ignored -> {
            boolean flush = args.containsOption("flush");
            if (flush) {
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
            }

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

    // Redis 저수준 API 호출용 UTF-8 바이트 변환
    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // 패턴에 맞는 키를 조회해 일괄 삭제한다(운영 대용량 사용 주의)
    private static void deleteByPattern(StringRedisTemplate redis, String pattern) {
        var keys = redis.keys(pattern);
        if (keys != null && !keys.isEmpty()) {
            redis.delete(keys);
        }
    }

    // 구독-요금제 정보를 읽어 개인 한도(limit:sub:{subId})를 채움
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

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                long subId = rs.getLong("sub_id");
                long planKb = rs.getLong("plan_limit_kb");
                String key = "limit:sub:" + subId;
                conn.hSet(b(key), b("plan_limit"), b(Long.toString(planKb)));
            });
            return null;
        });
    }

    // 가족 정보를 읽어 가족 한도(limit:family:{familyId})를 채움
    private void seedFamilyLimit(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT DISTINCT fs.family_id,
                            f.family_data_amount AS family_limit_kb
            FROM family_sub fs
            JOIN family f
              ON f.family_id = fs.family_id
            WHERE f.is_deleted = false
            """;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                long familyId = rs.getLong("family_id");
                long famKb = rs.getLong("family_limit_kb");
                String key = "limit:family:" + familyId;
                conn.hSet(b(key), b("family_limit"), b(Long.toString(famKb)));
            });
            return null;
        });
    }

    // 가족 구성원 인덱스/우선순위/개별 가족 한도 키를 생성
    private void seedFamilySubLimitPriorityAndIndexes(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT family_id,
                   sub_id,
                   priority,
                   data_limit
            FROM family_sub
            """;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                long familyId = rs.getLong("family_id");
                long subId = rs.getLong("sub_id");
                int priority = rs.getInt("priority");
                long dataLimitKb = rs.getLong("data_limit");

                conn.hSet(b("idx:sub:family"), b(Long.toString(subId)), b(Long.toString(familyId)));

                if (priority >= 0) {
                    String prioKey = "priority:family:" + familyId;
                    conn.zAdd(b(prioKey), (double) priority, b(Long.toString(subId)));
                }

                String limitKey = "limit:family_sub:" + familyId + ":" + subId;
                String familyLimitValue = (dataLimitKb < 0) ? "-1" : Long.toString(dataLimitKb);
                conn.hSet(b(limitKey), b("family_limit"), b(familyLimitValue));
                conn.hSet(b(limitKey), b("priority"), b(Integer.toString(priority)));
            });
            return null;
        });
    }

    // 선물 데이터로 gift 한도/인덱스와 제공자 사용량을 적재
    private void seedPresentsAndDonorUsage(
        JdbcTemplate jdbc,
        StringRedisTemplate redis
    ) {
        String giftAppId = resolveGiftAppId(jdbc);
        String sql = """
            SELECT id AS present_data_id,
                   target_sub_id,
                   provide_sub_id,
                   data_amount,
                   created_time
            FROM present_data
            """;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                long presentId = rs.getLong("present_data_id");
                long targetSub = rs.getLong("target_sub_id");
                long provideSub = rs.getLong("provide_sub_id");
                long giftKb = rs.getLong("data_amount");

                LocalDateTime created = rs.getTimestamp("created_time").toLocalDateTime();
                long prioEpoch = created.atZone(ZoneId.of("Asia/Seoul")).toEpochSecond();
                String yyyymm = created.format(DateTimeFormatter.ofPattern("yyyyMM"));
                String yyyymmdd = created.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                String giftId = Long.toString(presentId);

                String limitGiftKey = "limit:gift:" + targetSub + ":" + giftId + ":" + yyyymm;
                conn.hSet(b(limitGiftKey), b("gift_limit"), b(Long.toString(giftKb)));

                String idxGiftKey = "idx:gift:" + targetSub + ":" + yyyymm;
                conn.zAdd(b(idxGiftKey), (double) prioEpoch, b(giftId));

                String donorMonKey = "usage:sub:" + provideSub + ":" + yyyymm;
                conn.hIncrBy(b(donorMonKey), b("plan_used"), giftKb);

                String donorDayKey = "usage:sub:" + provideSub + ":" + yyyymmdd;
                conn.hIncrBy(b(donorDayKey), b("plan_used"), giftKb);

                String donorMonAppKey = "usage:app:" + provideSub + ":" + yyyymm;
                conn.zIncrBy(b(donorMonAppKey), giftKb, b(giftAppId));

                String donorDayAppKey = "usage:app:" + provideSub + ":" + yyyymmdd;
                conn.zIncrBy(b(donorDayAppKey), giftKb, b(giftAppId));

                String usageGiftKey = "usage:gift:" + targetSub + ":" + giftId + ":" + yyyymm;
                conn.hSetNX(b(usageGiftKey), b("gift_used"), b("0"));
            });
            return null;
        });
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

    // 반복형 차단 정책(SCHEDULED)을 block:repeat에 적재
    private void seedBlockRepeat(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT ps.sub_id,
                   ps.policy_sub_id AS policy_id,
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
            CROSS JOIN LATERAL (
                SELECT ps.date_snapshot::jsonb AS snap
            ) s
            WHERE ps.is_deleted = false
              AND UPPER(COALESCE(s.snap ->> 'policyType', s.snap ->> 'policy_type', '')) IN ('SCHEDULED')
            """;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                long subId = rs.getLong("sub_id");
                String policyId = rs.getString("policy_id");
                String value = normalizeDaysCsv(rs.getString("days_csv"))
                    + "|"
                    + rs.getString("start_hhmm")
                    + "|"
                    + rs.getString("end_hhmm");
                conn.hSet(b("block:repeat:" + subId), b(policyId), b(value));
            });
            return null;
        });
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

    // 기간형 차단 정책(ONCE)을 block:time에 적재
    private void seedBlockTime(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT ps.sub_id,
                   ps.policy_sub_id AS policy_id,
                   COALESCE(
                     NULLIF(s.snap ->> 'expire_epoch', ''),
                     NULLIF(s.snap -> 'data' ->> 'endTime', ''),
                     NULLIF(s.snap ->> 'endTime', '')
                   ) AS once_end_value
            FROM policy_sub ps
            CROSS JOIN LATERAL (
                SELECT ps.date_snapshot::jsonb AS snap
            ) s
            WHERE ps.is_deleted = false
              AND UPPER(COALESCE(s.snap ->> 'policyType', s.snap ->> 'policy_type', '')) = 'ONCE'
            """;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                long subId = rs.getLong("sub_id");
                String policyId = rs.getString("policy_id");
                long expireEpoch = parseOnceEndToEpoch(rs.getString("once_end_value"));
                if (expireEpoch > 0) {
                    conn.zAdd(b("block:time:" + subId), (double) expireEpoch, b(policyId));
                }
            });
            return null;
        });
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

    // 즉시 차단 정책(IMMEDIATE)을 block:immediate에 적재
    private void seedBlockImmediate(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT s.sub_id
            FROM subscription s
            WHERE s.is_deleted = false
              AND s.is_locked = true
            """;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                conn.set(b("block:immediate:" + rs.getLong("sub_id")), b("1"));
            });
            return null;
        });
    }

    // 앱 차단 목록을 block:app:{subId} 세트로 적재
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

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                long subId = rs.getLong("sub_id");
                String appId = rs.getString("app_id");
                conn.sAdd(b("block:app:" + subId), b(appId));
            });
            return null;
        });
    }
}
