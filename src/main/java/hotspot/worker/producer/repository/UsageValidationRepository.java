package hotspot.worker.producer.repository;

import hotspot.worker.producer.schema.UsageEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Repository
@RequiredArgsConstructor
public class UsageValidationRepository {

    private final RedisTemplate<String, String> redisTemplate;
    private final DefaultRedisScript<List> masterBatchScript;

    private static final ZoneId ZONE_ID = ZoneId.of("Asia/Seoul");

    public Set<Long> validateAndCheck(List<UsageEvent> events) {

        if (events.isEmpty()) {
            return Collections.emptySet();
        }

        ZonedDateTime now = ZonedDateTime.now(ZONE_ID);

        List<String> args = new ArrayList<>();

        args.add(String.valueOf(now.toEpochSecond()));
        args.add(events.get(0).appId());
        args.add(String.valueOf(now.getDayOfWeek().getValue()));
        args.add(now.format(DateTimeFormatter.ofPattern("HH:mm")));
        args.add(now.format(DateTimeFormatter.ofPattern("yyyyMM")));

        for (UsageEvent event : events) {
            args.add(String.valueOf(event.subId()));
        }

        List<?> result = redisTemplate.execute(
                masterBatchScript,
                Collections.emptyList(),
                args.toArray()
        );

        if (result == null) {
            return Collections.emptySet();
        }

        return result.stream()
                .map(o -> Long.parseLong(o.toString()))
                .collect(Collectors.toSet());
    }
}