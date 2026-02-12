package hotspot.worker.producer.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
                redisTemplate.opsForHash().entries("idx:sub:family");

        if (subFamilyMap.isEmpty()) {
            throw new IllegalStateException("idx:sub:family is empty");
        }

        List<String> subIds =
                subFamilyMap.keySet().stream()
                        .map(Object::toString)
                        .toList();

        Random random = new Random();
        List<UsageEvent> events = new ArrayList<>(EVENT_SIZE);

        int total = subIds.size();

        for (int i = 0; i < EVENT_SIZE; i++) {

            String subIdStr = subIds.get(random.nextInt(total));
            String familyIdStr =
                    subFamilyMap.get(subIdStr).toString();

            long subId = Long.parseLong(subIdStr);
            long familyId = Long.parseLong(familyIdStr);

            int usageKb =
                    random.nextInt(MAX_USAGE_KB - MIN_USAGE_KB + 1)
                            + MIN_USAGE_KB;

            events.add(
                    UsageEvent.create(subId, familyId, usageKb)
            );
        }

        return events;
    }
}
