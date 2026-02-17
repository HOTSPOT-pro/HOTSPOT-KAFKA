package hotspot.worker.producer.generator;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import hotspot.worker.producer.schema.AppType;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import hotspot.worker.producer.orchestrator.UsageOrchestrator;
import hotspot.worker.producer.schema.UsageEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static java.time.LocalDateTime.now;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageGenerator {

    private final UsageOrchestrator orchestrator;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String SUB_FAMILY_IDX_KEY = "idx:sub:family";
    private static final String FAMILY_SUB_SET_KEY = "idx:family:subs";

    private static final int MIN_USAGE_KB = 50;
    private static final int MAX_USAGE_KB = 500;
    private static final int EVENT_SIZE = 1000;


    public void produceEvent() {

        List<UsageEvent> events = generateRandomEvents();

        log.info("Generated events: {}", events.size());

        orchestrator.process(events);
    }

    private List<UsageEvent> generateRandomEvents() {

        List<String> subIds =
                redisTemplate.opsForSet()
                        .randomMembers(FAMILY_SUB_SET_KEY, EVENT_SIZE);

        if (subIds == null || subIds.isEmpty()) {
            throw new IllegalStateException("No family subs found");
        }

        List<UsageEvent> events = new ArrayList<>(subIds.size());

        for (String subIdStr : subIds) {

            Object familyObj =
                    redisTemplate.opsForHash()
                            .get(SUB_FAMILY_IDX_KEY, subIdStr);

            if (familyObj == null) {
                continue;
            }

            long subId = Long.parseLong(subIdStr);
            long familyId = Long.parseLong(familyObj.toString());

            int usageKb =
                    ThreadLocalRandom.current()
                            .nextInt(MIN_USAGE_KB, MAX_USAGE_KB + 1);

            events.add(
                    new UsageEvent(
                            UUID.randomUUID().toString(),
                            subId,
                            familyId,
                            usageKb,
                            AppType.randomAppId(),
                            now(ZoneId.of("Asia/Seoul"))
                    )
            );
        }

        return events;
    }
}
