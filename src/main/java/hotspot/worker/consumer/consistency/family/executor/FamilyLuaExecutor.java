package hotspot.worker.consumer.consistency.family.executor;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.family.dto.PriorityDto;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class FamilyLuaExecutor {

    private final StringRedisTemplate redisTemplate;

    private final DefaultRedisScript<Long> familyMemberScript;
    private final DefaultRedisScript<Long> familyModeScript;
    private final DefaultRedisScript<Long> familySubLimitScript;
    private final DefaultRedisScript<Long> familyCreateScript;
    private final DefaultRedisScript<Long> familyPolicyChangedScript;
    private final DefaultRedisScript<Long> familyPolicyDeletedScript;
    private final DefaultRedisScript<Long> familyPolicyDeactivateScript;

    public void executeCreate(
            String eventId,
            long familyId,
            List<Long> members
    ) {

        List<String> keys = List.of(
                "idem:family:" + eventId,
                "limit:family:" + familyId,
                "idx:sub:family",
                "idx:family:subs:" + familyId
        );

        List<String> args = new ArrayList<>();

        args.add(String.valueOf(familyId));
        args.add(String.valueOf(members.size()));

        for (Long subId : members) {
            args.add(String.valueOf(subId));
        }

        redisTemplate.execute(
                familyCreateScript,
                keys,
                args.toArray()
        );
    }

    public void removePolicy(JsonNode event) {

        String eventId = requireText(event, "eventId");
        long policyId = requireLong(event, "policyId");

        JsonNode subIdsNode = event.get("subIds");

        List<String> keys = List.of(
                "idem:policy:" + eventId
        );

        List<String> argv = new ArrayList<>();

        argv.add(String.valueOf(policyId));

        for (JsonNode sub : subIdsNode) {
            argv.add(String.valueOf(sub.asLong()));
        }

        redisTemplate.execute(
                familyPolicyDeletedScript,
                keys,
                argv.toArray()
        );
    }

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

    public void applyPolicy(JsonNode event) {

        String eventId = requireText(event, "eventId");

        JsonNode policyNode = event.get("policy");
        if (policyNode == null || !policyNode.isObject()) {
            throw new IllegalArgumentException("Missing policy object");
        }

        long policyId = requireLong(policyNode, "policyId");
        String policyType = requireText(policyNode, "policyType");

        JsonNode subIdsNode = event.get("subIds");

        if (subIdsNode == null || !subIdsNode.isArray()) {
            throw new IllegalArgumentException("Missing subIds array");
        }

        List<String> keys = List.of(
                "idem:policy:" + eventId
        );

        List<String> argv = new ArrayList<>();

        argv.add(String.valueOf(policyId));
        argv.add(policyType);

        if ("SCHEDULED".equals(policyType)) {

            String encoded = requireText(policyNode, "encoded");

            argv.add(encoded);
            argv.add("0");

        } else {

            long expireEpoch = requireLong(policyNode, "expireEpoch");

            argv.add("");
            argv.add(String.valueOf(expireEpoch));
        }

        for (JsonNode sub : subIdsNode) {
            argv.add(String.valueOf(sub.asLong()));
        }

        redisTemplate.execute(
                familyPolicyChangedScript,
                keys,
                argv.toArray()
        );
    }

    public void deactivatePolicy(JsonNode event) {

        String eventId = requireText(event, "eventId");

        JsonNode policiesNode = event.get("policies");

        if (policiesNode == null || !policiesNode.isArray()) {
            throw new IllegalArgumentException("Missing policies array");
        }

        List<String> keys = List.of(
                "idem:policy:" + eventId
        );

        List<String> argv = new ArrayList<>();

        for (JsonNode policy : policiesNode) {

            JsonNode subIdsNode = policy.get("subIds");

            if (subIdsNode == null || !subIdsNode.isArray()) {
                continue;
            }

            long policyId = requireLong(policy, "policyId");
            argv.add(String.valueOf(policyId));

            for (JsonNode sub : subIdsNode) {
                argv.add(String.valueOf(sub.asLong()));
            }

            argv.add("END");
        }

        if (argv.isEmpty()) {
            return;
        }

        redisTemplate.execute(
                familyPolicyDeactivateScript,
                keys,
                argv.toArray()
        );
    }

    private String requireText(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || !v.isTextual()) {
            throw new IllegalArgumentException("Missing or invalid '" + field + "'");
        }
        return v.asText();
    }

    private long requireLong(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || !v.isNumber()) {
            throw new IllegalArgumentException("Missing or invalid '" + field + "'");
        }
        return v.asLong();
    }
}
