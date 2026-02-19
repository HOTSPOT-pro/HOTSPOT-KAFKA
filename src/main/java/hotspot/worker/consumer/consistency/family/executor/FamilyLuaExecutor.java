package hotspot.worker.consumer.consistency.family.executor;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import hotspot.worker.consumer.consistency.family.dto.PriorityDto;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class FamilyLuaExecutor {

    private final StringRedisTemplate redisTemplate;

    private final DefaultRedisScript<Long> familyMemberScript;
    private final DefaultRedisScript<Long> familyModeScript;
    private final DefaultRedisScript<Long> familySubLimitScript;

    public void executeMember(String eventId, long familyId, long subId, String type) {

        redisTemplate.execute(
                familyMemberScript,
                List.of(
                        "idem:family:" + eventId,
                        "limit:family:" + familyId,
                        "idx:sub:family",
                        "idx:family:subs:" + familyId,
                        "limit:family_sub:" + familyId + ":" + subId,
                        "priority:family:" + familyId
                ),
                String.valueOf(subId),
                String.valueOf(familyId),
                type
        );
    }

    public void executeMode(
            String eventId,
            long familyId,
            String mode,
            List<PriorityDto> priorities
    ) {

        List<String> keys = List.of(
                "idem:family:" + eventId,
                "priority:family:" + familyId
        );

        List<String> args = new ArrayList<>();
        args.add(mode);

        if ("PRIORITY".equals(mode)) {

            args.add(String.valueOf(priorities.size()));

            for (PriorityDto dto : priorities) {
                args.add(String.valueOf(dto.subId()));
                args.add(String.valueOf(dto.priority()));
            }

        } else {
            args.add("0");
        }

        redisTemplate.execute(
                familyModeScript,
                keys,
                args.toArray()
        );
    }

    public void executeSubLimit(String eventId, long familyId, long subId, long newLimit) {
        redisTemplate.execute(
                familySubLimitScript,
                List.of(
                        "idem:family:" + eventId,
                        "limit:family_sub:" + familyId + ":" + subId
                ),
                String.valueOf(newLimit)
        );
    }
}
