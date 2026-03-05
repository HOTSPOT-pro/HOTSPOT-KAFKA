package hotspot.worker.consumer.consistency.family.handler;

import static hotspot.worker.common.util.JsonNodeUtils.requireLong;
import static hotspot.worker.common.util.JsonNodeUtils.requireText;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.family.executor.FamilyLuaExecutor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class FamilyCreateHandler implements FamilyEventHandler {

    private final FamilyLuaExecutor luaExecutor;

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("FAMILY_CREATE");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = requireText(event, "eventId");
        long familyId = requireLong(event, "familyId");

        JsonNode membersNode = event.get("members");

        if (membersNode == null || !membersNode.isArray()) {
            throw new IllegalArgumentException("Missing or invalid 'members' array");
        }

        List<Long> members = new ArrayList<>();

        for (JsonNode node : membersNode) {
            members.add(node.asLong());
        }

        luaExecutor.executeCreate(
                eventId,
                familyId,
                members
        );
    }
}
