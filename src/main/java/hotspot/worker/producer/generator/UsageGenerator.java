package hotspot.worker.producer.generator;

import java.util.ArrayList;
import java.util.List;
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

    private static final long[] FIXED_SUB_IDS = {
            1000001L,
            1000005L,
            1000006L
    };

    private static final String SUB_FAMILY_IDX_KEY = "idx:sub:family";
    private static final String FAMILY_SUB_SET_KEY = "idx:family:subs";

    private static final int MIN_USAGE_KB = 50;
    private static final int MAX_USAGE_KB = 500;
    private static final int EVENT_SIZE = 7500;

    public void produceEvent() {
        List<UsageEvent> events = generateRandomEvents();
        orchestrator.process(events);
    }

//    private List<UsageEvent> generateFixedEvents() {
//
//        List<UsageEvent> events = new ArrayList<>();
//
//        for (long subId : FIXED_SUB_IDS) {
//
//            // Redis에서 familyId 조회
//            Object familyObj =
//                    redisTemplate.opsForHash()
//                            .get(SUB_FAMILY_IDX_KEY, String.valueOf(subId));
//
//            if (familyObj == null) {
//                continue;
//            }
//
//            long familyId = Long.parseLong(familyObj.toString());
//
//            events.add(
//                    UsageEvent.create(subId, familyId, FIXED_USAGE_KB)
//            );
//        }
//
//        return events;
//    }

    private List<UsageEvent> generateRandomEvents() {
        List<Object> sampledSubIds =
                redisTemplate.opsForHash()
                        .randomKeys(SUB_FAMILY_IDX_KEY, EVENT_SIZE);

        if (sampledSubIds == null || sampledSubIds.isEmpty()) {
            throw new IllegalStateException("No family subs found");
        }

        List<String> subIds = sampledSubIds.stream()
                .map(Object::toString)
                .toList();

        List<Object> familyObjs =
                redisTemplate.opsForHash()
                        .multiGet(SUB_FAMILY_IDX_KEY, new ArrayList<>(subIds));

        List<UsageEvent> events = new ArrayList<>(subIds.size());

        for (int i = 0; i < subIds.size(); i++) {
            String subIdStr = subIds.get(i);
            Object familyObj = familyObjs.get(i);

            if (familyObj == null) {
                continue;
            }

            long subId = Long.parseLong(subIdStr);
            long familyId = Long.parseLong(familyObj.toString());

            int bytes =
                    ThreadLocalRandom.current()
                            .nextInt(MIN_USAGE_KB, MAX_USAGE_KB + 1);

            events.add(
                    UsageEvent.create(subId, familyId, bytes)
            );
        }

        return events;
    }

//    private List<UsageEvent> generateRandomEventsLegacy() {
//
//        List<String> subIds =
//                redisTemplate.opsForSet()
//                        .randomMembers("idx:family:subs", EVENT_SIZE);
//
//        if (subIds == null || subIds.isEmpty()) {
//            throw new IllegalStateException("No family subs found");
//        }
//
//        List<Object> familyObjs =
//                redisTemplate.opsForHash()
//                        .multiGet(SUB_FAMILY_IDX_KEY, new ArrayList<>(subIds));
//
//        List<UsageEvent> events = new ArrayList<>(subIds.size());
//
//        for (int i = 0; i < subIds.size(); i++) {
//            String subIdStr = subIds.get(i);
//            Object familyObj = familyObjs.get(i);
//
//            if (familyObj == null) {
//                continue;
//            }
//
//            long subId = Long.parseLong(subIdStr);
//            long familyId = Long.parseLong(familyObj.toString());
//            int bytes = ThreadLocalRandom.current().nextInt(MIN_USAGE_KB, MAX_USAGE_KB + 1);
//
//            events.add(UsageEvent.create(subId, familyId, bytes));
//        }
//
//        return events;
//    }
}
