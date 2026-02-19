package hotspot.worker.consumer.consistency.family.handler;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.family.executor.FamilyLuaExecutor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class FamilyMemberHandler implements FamilyEventHandler {

    private final FamilyLuaExecutor luaExecutor;

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("FAMILY_MEMBER_ADDED")
                || eventType.equals("FAMILY_MEMBER_REMOVED");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = event.get("eventId").asText();
        long familyId = event.get("familyId").asLong();
        long subId = event.get("subId").asLong();

        String type = event.get("type").asText()
                .equals("FAMILY_MEMBER_ADDED") ? "ADD" : "REMOVE";

        luaExecutor.executeMember(eventId, familyId, subId, type);
    }
}
