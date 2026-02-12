package hotspot.worker.producer.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import hotspot.worker.producer.orchestrator.UsageOrchestrator;
import hotspot.worker.producer.schema.UsageEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageGenerator {

    private final UsageOrchestrator orchestrator;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String SUB_FAMILY_IDX_KEY = "idx:sub:family";
    private static final int MIN_USAGE_KB = 50;
    private static final int MAX_USAGE_KB = 500;
    private static final int EVENT_SIZE = 1000;

    public void produceEvent() {

        List<UsageEvent> events = generateRandomEvents();

        log.info("Generated events: {}", events.size());

        orchestrator.process(events);
    }

    private List<UsageEvent> generateRandomEvents() {

        Map<Object, Object> subFamilyMap =
                redisTemplate.opsForHash().entries("SUB_FAMILY_IDX_KEY");

        if (subFamilyMap.isEmpty()) {
            throw new IllegalStateException(SUB_FAMILY_IDX_KEY + " is empty");
        }

        List<String> subIds =
                subFamilyMap.keySet().stream()
                        .map(Object::toString)
                        .toList();

        List<UsageEvent> events = new ArrayList<>(EVENT_SIZE);

        int total = subIds.size();

        for (int i = 0; i < EVENT_SIZE; i++) {

            String subIdStr = subIds.get(ThreadLocalRandom.current().nextInt(total));
            String familyIdStr =
                    subFamilyMap.get(subIdStr).toString();

            long subId = Long.parseLong(subIdStr);
            long familyId = Long.parseLong(familyIdStr);

            int usageKb =
                    ThreadLocalRandom.current()
                            .nextInt(MIN_USAGE_KB, MAX_USAGE_KB + 1);

            events.add(
                    UsageEvent.create(subId, familyId, usageKb)
            );
        }

        return events;
    }
}
