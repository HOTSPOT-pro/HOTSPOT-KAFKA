package hotspot.worker.producer.repository;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;

import hotspot.worker.producer.schema.UsageEvent;
import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class UsageValidationRepository {

    private final RedisTemplate<String, String> redisTemplate;
    private final DefaultRedisScript<List> usageValidBatchScript;

    private static final DateTimeFormatter HHMM_FORMATTER = DateTimeFormatter.ofPattern("HH:mm");
    private static final DateTimeFormatter YYYYMM_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");
    private static final DateTimeFormatter YYYYMMDD_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final ZoneId ZONE_ID = ZoneId.of("Asia/Seoul");

    public Set<String> validateAndCheck(List<UsageEvent> events) {

        if (events.isEmpty()) {
            return Set.of();
        }

        ZonedDateTime now = ZonedDateTime.now(ZONE_ID);

        List<String> args = new ArrayList<>();

        args.add(String.valueOf(now.toEpochSecond()));
        args.add(String.valueOf(now.getDayOfWeek().getValue()));
        args.add(now.format(HHMM_FORMATTER));
        args.add(now.format(YYYYMM_FORMATTER));
        args.add(now.format(YYYYMMDD_FORMATTER));

        for (UsageEvent event : events) {
            args.add(event.eventId());
            args.add(String.valueOf(event.subId()));
            args.add(String.valueOf(event.familyId()));
            args.add(String.valueOf(event.bytes()));
            args.add(String.valueOf(event.appId()));
        }

        List<?> result = redisTemplate.execute(
                usageValidBatchScript,
                Collections.emptyList(),
                args.toArray()
        );

        if (result == null) {
            return Collections.emptySet();
        }

        return result.stream()
                .map(Object::toString)
                .collect(Collectors.toSet());
    }
}
