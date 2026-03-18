package hotspot.seed;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

import hotspot.worker.consumer.usage.support.TimeKey;

/**
 * 로컬 Redis에 정책/한도 시드 데이터를 적재하는 전용 실행기
 */
@SpringBootApplication
public class SeedRunner {

    private static final int PIPELINE_BATCH_SIZE = 1000;
    private static final List<Long> HISTORICAL_USAGE_SUB_IDS =
            List.of(1000001L, 1000002L, 1000003L, 1000004L, 1000005L, 1000006L);
    private static final DateTimeFormatter YYYYMM = DateTimeFormatter.ofPattern("yyyyMM");
    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

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

            deleteByPattern(redis, "usage:sub:*");
            deleteByPattern(redis, "usage:family:*");
            deleteByPattern(redis, "usage:gift:*");
            deleteByPattern(redis, "usage:app:*");
            deleteByPattern(redis, "usage:3hourly:*");
            deleteByPattern(redis, "notify:sub:*");
            deleteByPattern(redis, "notify:family:*");
            deleteByPattern(redis, "notify:gift:*");
            deleteByPattern(redis, "dedup:evt:*");
            deleteByPattern(redis, "idx:family:subs:*");
            deleteByPattern(redis, "idx:family:subs");

            redis.delete("idx:sub:family");

            System.out.println("Seed Initialization done.");


            seedPlanLimit(jdbc, redis);
            seedFamilyLimit(jdbc, redis);
            seedFamilySubLimitPriorityAndIndexes(jdbc, redis);
            seedPresentsAndDonorUsage(jdbc, redis);

            System.out.println("Limit Seed done.");

            seedHistoricalUsage(jdbc, redis);

            System.out.println("Usage History Seed done.");

            seedBlockRepeat(jdbc, redis);
            seedBlockTime(jdbc, redis);
            seedBlockImmediate(jdbc, redis);
            seedBlockApp(jdbc, redis);

            System.out.println("Block Policy Seed done.");
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

            // 모든 family 소속 subId를 하나의 SET에 저장
            conn.sAdd(
                    b("idx:family:subs:" + row.familyId()),
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

            String donorDay3HourlyAppKey = "usage:3hourly:" + row.provideSubId() + ":" + yyyymmdd;
            conn.hIncrBy(
                    b(donorDay3HourlyAppKey),
                    b(TimeKey.daily3HourlyUsedField(row.createdTime().atZone(KST).toInstant(), KST)),
                    row.dataAmountKb()
            );

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

    // 과거 6개월(현재 달 포함) 범위의 일별/월별 사용량 키를 랜덤으로 적재한다.
    private void seedHistoricalUsage(JdbcTemplate jdbc, StringRedisTemplate redis) {
        Map<Long, SubPlanProfile> planProfileBySubId = resolvePlanProfileBySubId(jdbc);
        Map<Long, Long> familyBySubId = resolveFamilyBySubId(jdbc);
        List<String> appIds = resolveUsageAppIds(jdbc);

        LocalDate today = LocalDate.now(KST);
        LocalDate startDate = today.withDayOfMonth(1).minusMonths(5);
        LocalDate endDate = today.minusDays(1);
        if (endDate.isBefore(startDate)) {
            return;
        }

        Map<YearMonth, List<LocalDate>> datesByMonth = new LinkedHashMap<>();
        for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
            YearMonth yearMonth = YearMonth.from(date);
            datesByMonth.computeIfAbsent(yearMonth, ignored -> new ArrayList<>()).add(date);
        }

        List<HistoricalUsageRow> rows = new ArrayList<>();
        for (long subId : HISTORICAL_USAGE_SUB_IDS) {
            Long familyId = familyBySubId.get(subId);
            SubPlanProfile planProfile = requirePlanProfile(planProfileBySubId, subId);
            Random random = new Random(20260314L + subId);

            for (Map.Entry<YearMonth, List<LocalDate>> entry : datesByMonth.entrySet()) {
                List<LocalDate> monthDates = entry.getValue();
                YearMonth yearMonth = entry.getKey();
                List<Long> dailyTotals = buildDailyTotalsForPlan(
                        planProfile.planId(),
                        planProfile.planLimitKb(),
                        yearMonth,
                        monthDates.size(),
                        random
                );

                for (int i = 0; i < monthDates.size(); i++) {
                    LocalDate date = monthDates.get(i);
                    long totalUsed = dailyTotals.get(i);
                    long giftUsed = 0L;
                    long familyUsed = 0L;
                    if (familyId != null) {
                        long familyLimit = Math.max(0L, totalUsed - giftUsed);
                        familyUsed = random.nextInt(100) < 40 ? randomBetween(random, 0L, familyLimit / 4) : 0L;
                    }
                    long overflowUsed = 0L;
                    long overflowLimit = Math.max(0L, totalUsed - giftUsed - familyUsed);
                    if (random.nextInt(100) < 3) {
                        overflowUsed = randomBetween(random, 0L, overflowLimit / 25);
                    }
                    long planUsed = totalUsed - giftUsed - familyUsed - overflowUsed;

                    rows.add(new HistoricalUsageRow(
                            subId,
                            familyId,
                            date,
                            totalUsed,
                            planUsed,
                            familyUsed,
                            giftUsed,
                            overflowUsed,
                            buildAppUsage(appIds, totalUsed, random),
                            build3HourlyUsage(totalUsed, random)
                    ));
                }
            }
        }

        writeInBatches(redis, rows, PIPELINE_BATCH_SIZE, (conn, row) -> {
            String yyyymm = row.date().format(YYYYMM);
            String yyyymmdd = row.date().format(YYYYMMDD);

            String subMonKey = "usage:sub:" + row.subId() + ":" + yyyymm;
            String subDayKey = "usage:sub:" + row.subId() + ":" + yyyymmdd;

            incrSubUsage(conn, subMonKey, row);
            incrSubUsage(conn, subDayKey, row);

            if (row.familyId() != null && row.familyUsed() > 0) {
                String familyMonKey = "usage:family:" + row.familyId() + ":" + yyyymm;
                String familyDayKey = "usage:family:" + row.familyId() + ":" + yyyymmdd;
                conn.hIncrBy(b(familyMonKey), b("family_used"), row.familyUsed());
                conn.hIncrBy(b(familyDayKey), b("family_used"), row.familyUsed());
            }

            String appMonKey = "usage:app:" + row.subId() + ":" + yyyymm;
            String appDayKey = "usage:app:" + row.subId() + ":" + yyyymmdd;
            for (Map.Entry<String, Long> appUsage : row.appUsages().entrySet()) {
                conn.zIncrBy(b(appMonKey), appUsage.getValue(), b(appUsage.getKey()));
                conn.zIncrBy(b(appDayKey), appUsage.getValue(), b(appUsage.getKey()));
            }

            String hourlyDayKey = "usage:3hourly:" + row.subId() + ":" + yyyymmdd;
            for (Map.Entry<String, Long> bucket : row.hourlyUsages().entrySet()) {
                conn.hIncrBy(b(hourlyDayKey), b(bucket.getKey()), bucket.getValue());
            }
        });
    }

    private void incrSubUsage(RedisConnection conn, String key, HistoricalUsageRow row) {
        conn.hIncrBy(b(key), b("total_used"), row.totalUsed());
        conn.hIncrBy(b(key), b("plan_used"), row.planUsed());
        conn.hIncrBy(b(key), b("member_family_used"), row.familyUsed());
        conn.hIncrBy(b(key), b("gift_used"), row.giftUsed());
        if (row.overflowUsed() > 0) {
            conn.hIncrBy(b(key), b("overflow_used"), row.overflowUsed());
        }
    }

    private Map<Long, Long> resolveFamilyBySubId(JdbcTemplate jdbc) {
        String sql = """
                SELECT sub_id, family_id
                FROM family_sub
                WHERE sub_id IN (1000001,1000002,1000003,1000004,1000005,1000006)
                ORDER BY family_id
                """;
        List<FamilyMappingRow> rows = jdbc.query(sql, (rs, rowNum) ->
                new FamilyMappingRow(rs.getLong("sub_id"), rs.getLong("family_id"))
        );

        Map<Long, Long> bySubId = new HashMap<>();
        for (FamilyMappingRow row : rows) {
            bySubId.putIfAbsent(row.subId(), row.familyId());
        }
        return bySubId;
    }

    private Map<Long, SubPlanProfile> resolvePlanProfileBySubId(JdbcTemplate jdbc) {
        String sql = """
                SELECT s.sub_id AS sub_id,
                       s.plan_id AS plan_id,
                       p.plan_data_amount AS plan_limit_kb
                FROM subscription s
                JOIN plan p
                  ON p.plan_id = s.plan_id
                WHERE s.sub_id IN (1000001,1000002,1000003,1000004,1000005,1000006)
                """;
        List<SubPlanProfileRow> rows = jdbc.query(sql, (rs, rowNum) ->
                new SubPlanProfileRow(
                        rs.getLong("sub_id"),
                        rs.getLong("plan_id"),
                        rs.getLong("plan_limit_kb")
                )
        );
        Map<Long, SubPlanProfile> bySubId = new HashMap<>();
        for (SubPlanProfileRow row : rows) {
            bySubId.putIfAbsent(row.subId(), new SubPlanProfile(row.planId(), row.planLimitKb()));
        }
        return bySubId;
    }

    private SubPlanProfile requirePlanProfile(Map<Long, SubPlanProfile> planProfileBySubId, long subId) {
        SubPlanProfile profile = planProfileBySubId.get(subId);
        if (profile == null) {
            throw new IllegalStateException("Plan limit not found for sub_id=" + subId);
        }
        if (profile.planId() == 1L) {
            if (profile.planLimitKb() != -1L) {
                throw new IllegalStateException("Unlimited plan expected for sub_id=" + subId + ", plan_id=1");
            }
            return profile;
        }
        if (profile.planLimitKb() <= 0L) {
            throw new IllegalStateException("Invalid plan limit for sub_id=" + subId + ", plan_id=" + profile.planId());
        }
        return profile;
    }

    private List<Long> buildDailyTotalsForPlan(
            long planId,
            long planLimitKb,
            YearMonth yearMonth,
            int dayCount,
            Random random
    ) {
        if (dayCount <= 0) {
            return List.of();
        }
        if (planId == 1L || planLimitKb == -1L) {
            return buildUnlimitedDailyTotals(dayCount, random);
        }
        if (planId == 5L) {
            return buildDailyPlanTotals(planLimitKb, dayCount, random);
        }
        if (planId == 2L || planId == 3L || planId == 4L) {
            return buildMonthlyPlanTotals(planLimitKb, yearMonth, dayCount, random);
        }
        return buildMonthlyPlanTotals(planLimitKb, yearMonth, dayCount, random);
    }

    private List<Long> buildUnlimitedDailyTotals(int dayCount, Random random) {
        List<Long> totals = new ArrayList<>(dayCount);
        long monthBase = randomBetween(random, 400_000L, 2_000_000L);
        for (int i = 0; i < dayCount; i++) {
            long dayUsed = jitterAround(monthBase, 0.20d, random);
            totals.add(Math.max(50_000L, dayUsed));
        }
        return totals;
    }

    private List<Long> buildDailyPlanTotals(long dailyLimitKb, int dayCount, Random random) {
        long minDaily = Math.max(1L, Math.round(dailyLimitKb * 0.5d));
        long maxDaily = Math.max(minDaily, dailyLimitKb);
        long baseMin = Math.max(minDaily, Math.round(dailyLimitKb * 0.65d));
        long baseMax = Math.max(baseMin, Math.round(dailyLimitKb * 0.85d));
        long monthBase = randomBetween(random, baseMin, baseMax);
        List<Long> totals = new ArrayList<>(dayCount);
        for (int i = 0; i < dayCount; i++) {
            long dayUsed = jitterAround(monthBase, 0.10d, random);
            totals.add(clamp(dayUsed, minDaily, maxDaily));
        }
        return totals;
    }

    private List<Long> buildMonthlyPlanTotals(long monthlyLimitKb, YearMonth yearMonth, int dayCount, Random random) {
        long proratedLimit = proratedMonthlyLimit(monthlyLimitKb, yearMonth, dayCount);
        long monthlyTarget = randomBetween(
                random,
                Math.max(1L, Math.round(proratedLimit * 0.5d)),
                Math.max(1L, proratedLimit)
        );

        List<String> dayKeys = new ArrayList<>(dayCount);
        List<Double> weights = new ArrayList<>(dayCount);
        for (int i = 0; i < dayCount; i++) {
            dayKeys.add(Integer.toString(i));
            weights.add(0.9d + random.nextDouble() * 0.2d);
        }
        Map<String, Long> distributed = distributeByWeights(dayKeys, weights, monthlyTarget, 0L);

        List<Long> totals = new ArrayList<>(dayCount);
        for (int i = 0; i < dayCount; i++) {
            totals.add(distributed.get(Integer.toString(i)));
        }
        return totals;
    }

    private long proratedMonthlyLimit(long monthlyLimitKb, YearMonth yearMonth, int coveredDays) {
        int daysInMonth = yearMonth.lengthOfMonth();
        if (daysInMonth <= 0 || coveredDays <= 0) {
            return 1L;
        }
        return Math.max(1L, Math.round((double) monthlyLimitKb * coveredDays / daysInMonth));
    }

    private long jitterAround(long base, double ratio, Random random) {
        double factor = 1.0d - ratio + (random.nextDouble() * ratio * 2.0d);
        return Math.max(0L, Math.round(base * factor));
    }

    private long clamp(long value, long min, long max) {
        return Math.max(min, Math.min(max, value));
    }

    private List<String> resolveUsageAppIds(JdbcTemplate jdbc) {
        String sql = """
                SELECT app_blocked_service_id
                FROM app_blocked_service
                WHERE is_deleted = false
                ORDER BY app_blocked_service_id
                """;
        List<String> appIds = jdbc.query(sql, (rs, rowNum) -> rs.getString("app_blocked_service_id"));
        if (appIds.isEmpty()) {
            throw new IllegalStateException("No app ids found in app_blocked_service");
        }
        return appIds;
    }

    private Map<String, Long> buildAppUsage(List<String> appIds, long totalUsed, Random random) {
        List<String> orderedApps = new ArrayList<>(appIds);
        java.util.Collections.shuffle(orderedApps, random);

        List<Double> weights = new ArrayList<>(orderedApps.size());
        for (int i = 0; i < orderedApps.size(); i++) {
            double baseWeight;
            if (i == 0) {
                baseWeight = 24.0;
            } else if (i == 1) {
                baseWeight = 16.0;
            } else if (i < 5) {
                baseWeight = 8.0;
            } else {
                baseWeight = 3.0;
            }
            double jitter = 0.8 + random.nextDouble() * 0.5;
            weights.add(baseWeight * jitter);
        }

        return distributeByWeights(orderedApps, weights, totalUsed, 1L);
    }

    private Map<String, Long> build3HourlyUsage(long totalUsed, Random random) {
        List<String> fields = List.of(
                "00_used", "03_used", "06_used", "09_used",
                "12_used", "15_used", "18_used", "21_used"
        );

        // 새벽은 낮고, 점심~저녁에 피크가 오도록 기본 가중치를 준다.
        List<Double> baseWeights = List.of(7.0, 3.0, 6.0, 14.0, 16.0, 17.0, 23.0, 14.0);
        List<Double> jitteredWeights = new ArrayList<>(baseWeights.size());
        for (double baseWeight : baseWeights) {
            double jitter = 0.85 + random.nextDouble() * 0.3;
            jitteredWeights.add(baseWeight * jitter);
        }

        return distributeByWeights(fields, jitteredWeights, totalUsed, 1L);
    }

    private Map<String, Long> distributeByWeights(
            List<String> keys,
            List<Double> weights,
            long total,
            long minPerKey
    ) {
        if (keys.isEmpty()) {
            return Map.of();
        }

        long safeTotal = Math.max(0L, total);
        long baseEach = (minPerKey > 0 && safeTotal >= minPerKey * keys.size()) ? minPerKey : 0L;
        long minTotal = baseEach * keys.size();
        long remaining = safeTotal - minTotal;

        Map<String, Long> distributed = new LinkedHashMap<>();
        for (String key : keys) {
            distributed.put(key, baseEach);
        }

        double weightSum = 0.0;
        for (double weight : weights) {
            weightSum += Math.max(0.0001, weight);
        }

        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            double weight = Math.max(0.0001, weights.get(i));
            long additional;
            if (i == keys.size() - 1 || weightSum <= 0.0) {
                additional = remaining;
            } else {
                additional = Math.round((double) remaining * weight / weightSum);
                if (additional > remaining) {
                    additional = remaining;
                }
            }
            distributed.put(key, distributed.get(key) + additional);
            remaining -= additional;
            weightSum -= weight;
        }

        return distributed;
    }

    private long randomBetween(Random random, long minInclusive, long maxInclusive) {
        if (maxInclusive <= minInclusive) {
            return minInclusive;
        }
        return minInclusive + (long) (random.nextDouble() * (maxInclusive - minInclusive + 1));
    }

    private record SubPlanProfileRow(long subId, long planId, long planLimitKb) {
    }

    private record SubPlanProfile(long planId, long planLimitKb) {
    }

    private record FamilyMappingRow(long subId, long familyId) {
    }

    private record HistoricalUsageRow(
            long subId,
            Long familyId,
            LocalDate date,
            long totalUsed,
            long planUsed,
            long familyUsed,
            long giftUsed,
            long overflowUsed,
            Map<String, Long> appUsages,
            Map<String, Long> hourlyUsages
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
                  AND UPPER(
                        COALESCE(bp.policy_type::text, s.snap ->> 'policyType', s.snap ->> 'policy_type', '')
                      ) IN ('SCHEDULED')
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
                  AND UPPER(
                        COALESCE(bp.policy_type::text, s.snap ->> 'policyType', s.snap ->> 'policy_type', '')
                      ) = 'ONCE'
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
                WHERE bss.is_active = true
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
