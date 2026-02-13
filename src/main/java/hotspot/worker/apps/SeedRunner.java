package hotspot.worker.apps;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

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
            String yyyymm = args.containsOption("month")
                ? args.getOptionValues("month").get(0)
                : YearMonth.now(ZoneId.of("Asia/Seoul")).format(DateTimeFormatter.ofPattern("yyyyMM"));

            String giftAppId = args.containsOption("gift-app-id")
                ? args.getOptionValues("gift-app-id").get(0)
                : "DATA_GIFT";

            boolean flush = args.containsOption("flush");
            boolean fcfs = isTrue(args, "fcfs");

            if (flush) {
                deleteByPattern(redis, "limit:sub:*");
                deleteByPattern(redis, "limit:family:*");
                deleteByPattern(redis, "limit:family_sub:*");
                deleteByPattern(redis, "limit:gift:*:*:" + yyyymm);
                deleteByPattern(redis, "idx:gift:*:" + yyyymm);

                deleteByPattern(redis, "block:repeat:*");
                deleteByPattern(redis, "block:time:*");
                deleteByPattern(redis, "block:immediate:*");
                deleteByPattern(redis, "block:app:*");
                deleteByPattern(redis, "priority:family:*");

                redis.delete("idx:sub:family");
            }

            seedPlanLimit(jdbc, redis);
            seedFamilyLimit(jdbc, redis);
            seedFamilySubLimitPriorityAndIndexes(jdbc, redis, fcfs);
            seedPresentsAndDonorUsage(jdbc, redis, yyyymm, giftAppId);

            seedBlockRepeat(jdbc, redis);
            seedBlockTime(jdbc, redis);
            seedBlockImmediate(jdbc, redis);
            seedBlockApp(jdbc, redis);

            System.out.println("Seed done. month=" + yyyymm + ", giftAppId=" + giftAppId + ", fcfs=" + fcfs);
        };
    }

    // Redis 저수준 API 호출용 UTF-8 바이트 변환.
    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // 패턴에 맞는 키를 조회해 일괄 삭제한다(운영 대용량 사용 주의).
    private static void deleteByPattern(StringRedisTemplate redis, String pattern) {
        var keys = redis.keys(pattern);
        if (keys != null && !keys.isEmpty()) {
            redis.delete(keys);
        }
    }

    // --option=true 또는 --option 형태를 모두 true로 해석한다.
    private static boolean isTrue(ApplicationArguments args, String option) {
        if (!args.containsOption(option)) {
            return false;
        }
        var values = args.getOptionValues(option);
        if (values == null || values.isEmpty()) {
            return true;
        }
        return Boolean.parseBoolean(values.get(0));
    }

    // 구독-요금제 정보를 읽어 개인 한도(limit:sub:{subId})를 채운다.
    private void seedPlanLimit(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT s.id AS sub_id,
                   p.plan_data_amount AS plan_limit_kb
            FROM subscription s
            JOIN plan p
              ON p.id = s.plan_id
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

    // 가족 정보를 읽어 가족 한도(limit:family:{familyId})를 채운다.
    private void seedFamilyLimit(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT DISTINCT fs.family_id,
                            f.family_data_amount AS family_limit_kb
            FROM family_sub fs
            JOIN family f
              ON f.id = fs.family_id
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

    // 가족 구성원 인덱스/우선순위/개별 가족 한도 키를 생성한다.
    private void seedFamilySubLimitPriorityAndIndexes(JdbcTemplate jdbc, StringRedisTemplate redis, boolean fcfs) {
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

                if (!fcfs) {
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

    // 선물 데이터로 gift 한도/인덱스와 제공자 사용량을 적재한다.
    private void seedPresentsAndDonorUsage(
        JdbcTemplate jdbc,
        StringRedisTemplate redis,
        String yyyymm,
        String giftAppId
    ) {
        String sql = """
            SELECT id AS present_data_id,
                   target_sub_id,
                   provide_sub_id,
                   data_amount,
                   created_time
            FROM present_data
            WHERE to_char(created_time, 'YYYYMM') = ?
            """;

        long ttlMonSec = 60L * 60 * 24 * 90;
        long ttlDaySec = 60L * 60 * 24 * 14;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, ps -> ps.setString(1, yyyymm), rs -> {
                long presentId = rs.getLong("present_data_id");
                long targetSub = rs.getLong("target_sub_id");
                long provideSub = rs.getLong("provide_sub_id");
                long giftKb = rs.getLong("data_amount");

                LocalDateTime created = rs.getTimestamp("created_time").toLocalDateTime();
                long prioEpoch = created.atZone(ZoneId.of("Asia/Seoul")).toEpochSecond();
                String yyyymmdd = created.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                String giftId = Long.toString(presentId);

                String limitGiftKey = "limit:gift:" + targetSub + ":" + giftId + ":" + yyyymm;
                conn.hSet(b(limitGiftKey), b("gift_limit"), b(Long.toString(giftKb)));
                conn.expire(b(limitGiftKey), ttlMonSec);

                String idxGiftKey = "idx:gift:" + targetSub + ":" + yyyymm;
                conn.zAdd(b(idxGiftKey), (double) prioEpoch, b(giftId));
                conn.expire(b(idxGiftKey), ttlMonSec);

                String donorMonKey = "usage:sub:" + provideSub + ":" + yyyymm;
                conn.hIncrBy(b(donorMonKey), b("plan_used"), giftKb);
                conn.expire(b(donorMonKey), ttlMonSec);

                String donorDayKey = "usage:sub:" + provideSub + ":" + yyyymmdd;
                conn.hIncrBy(b(donorDayKey), b("plan_used"), giftKb);
                conn.expire(b(donorDayKey), ttlDaySec);

                String donorMonAppKey = "usage:app:" + provideSub + ":" + yyyymm;
                conn.zIncrBy(b(donorMonAppKey), giftKb, b(giftAppId));
                conn.expire(b(donorMonAppKey), ttlMonSec);

                String donorDayAppKey = "usage:app:" + provideSub + ":" + yyyymmdd;
                conn.zIncrBy(b(donorDayAppKey), giftKb, b(giftAppId));
                conn.expire(b(donorDayAppKey), ttlDaySec);

                String usageGiftKey = "usage:gift:" + targetSub + ":" + giftId + ":" + yyyymm;
                conn.hSetNX(b(usageGiftKey), b("gift_used"), b("0"));
                conn.expire(b(usageGiftKey), ttlMonSec);
            });
            return null;
        });
    }

    // 반복형 차단 정책(SCHEDULED)을 block:repeat에 적재한다.
    private void seedBlockRepeat(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT ps.sub_id,
                   bp.id AS policy_id,
                   COALESCE(bp.policy_snapshot ->> 'days_csv', '') AS days_csv,
                   COALESCE(bp.policy_snapshot ->> 'start_hhmm', '') AS start_hhmm,
                   COALESCE(bp.policy_snapshot ->> 'end_hhmm', '') AS end_hhmm
            FROM policy_sub ps
            JOIN block_policy bp
              ON bp.id = ps.policy_id
            WHERE ps.is_deleted = false
              AND bp.is_deleted = false
              AND bp.policy_type = 'SCHEDULED'
            """;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                long subId = rs.getLong("sub_id");
                String policyId = rs.getString("policy_id");
                String value = rs.getString("days_csv")
                    + "|"
                    + rs.getString("start_hhmm")
                    + "|"
                    + rs.getString("end_hhmm");
                conn.hSet(b("block:repeat:" + subId), b(policyId), b(value));
            });
            return null;
        });
    }

    // 기간형 차단 정책(ONCE)을 block:time에 적재한다.
    private void seedBlockTime(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT ps.sub_id,
                   bp.id AS policy_id,
                   COALESCE(NULLIF(bp.policy_snapshot ->> 'expire_epoch', '')::bigint, 0) AS expire_epoch
            FROM policy_sub ps
            JOIN block_policy bp
              ON bp.id = ps.policy_id
            WHERE ps.is_deleted = false
              AND bp.is_deleted = false
              AND bp.policy_type = 'ONCE'
            """;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                long subId = rs.getLong("sub_id");
                String policyId = rs.getString("policy_id");
                double expire = rs.getLong("expire_epoch");
                conn.zAdd(b("block:time:" + subId), expire, b(policyId));
            });
            return null;
        });
    }

    // 즉시 차단 정책(IMMEDIATE)을 block:immediate에 적재한다.
    private void seedBlockImmediate(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT ps.sub_id
            FROM policy_sub ps
            JOIN block_policy bp
              ON bp.id = ps.policy_id
            WHERE ps.is_deleted = false
              AND bp.is_deleted = false
              AND bp.policy_type = 'IMMEDIATE'
            """;

        redis.executePipelined((RedisCallback<Object>) conn -> {
            jdbc.query(sql, rs -> {
                conn.set(b("block:immediate:" + rs.getLong("sub_id")), b("1"));
            });
            return null;
        });
    }

    // 앱 차단 목록을 block:app:{subId} 세트로 적재한다.
    private void seedBlockApp(JdbcTemplate jdbc, StringRedisTemplate redis) {
        String sql = """
            SELECT bss.sub_id,
                   abs.service_code AS app_id
            FROM blocked_service_sub bss
            JOIN app_blocked_service abs
              ON abs.id = bss.blocked_service_id
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
