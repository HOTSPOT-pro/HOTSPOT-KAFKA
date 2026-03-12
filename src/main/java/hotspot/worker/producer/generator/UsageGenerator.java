package hotspot.worker.producer.generator;

import java.util.ArrayList;
import java.util.List;

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
            1000002L,
            1000003L,
            1000004L,
            1000005L,
            1000006L
    };

    private static final String SUB_FAMILY_IDX_KEY = "idx:sub:family";
    private static final String FAMILY_SUB_SET_KEY = "idx:family:subs";

    private static final int MIN_USAGE_KB = 50;
    private static final int MAX_USAGE_KB = 1024;
    private static final int EVENT_SIZE = 5000;


    public void produceEvent() {

        List<UsageEvent> events = generateFixedEvents();

        log.info("Generated events: {}", events.size());

        orchestrator.process(events);
    }

    private List<UsageEvent> generateFixedEvents() {

        List<UsageEvent> events = new ArrayList<>();

        for (long subId : FIXED_SUB_IDS) {

            // Redis에서 familyId 조회
            Object familyObj =
                    redisTemplate.opsForHash()
                            .get(SUB_FAMILY_IDX_KEY, String.valueOf(subId));

            if (familyObj == null) {
                log.warn("Family mapping not found for subId={}", subId);
                continue;
            }

            long familyId = Long.parseLong(familyObj.toString());

            events.add(
                    UsageEvent.create(subId, familyId, MAX_USAGE_KB)
            );
        }

        return events;
    }

//    private List<UsageEvent> generateRandomEvents() {
//
//        List<String> subIds =
//                redisTemplate.opsForSet()
//                        .randomMembers(FAMILY_SUB_SET_KEY, EVENT_SIZE);
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
//
//            String subIdStr = subIds.get(i);
//            Object familyObj = familyObjs.get(i);
//
//            if (familyObj == null) {
//                continue;
//            }
//
//            long subId = Long.parseLong(subIdStr);
//            long familyId = Long.parseLong(familyObj.toString());
//
//            int bytes =
//                    ThreadLocalRandom.current()
//                            .nextInt(MIN_USAGE_KB, MAX_USAGE_KB + 1);
//
//            events.add(
//                    UsageEvent.create(subId, familyId, bytes)
//            );
//        }
//
//        return events;
//    }
}
