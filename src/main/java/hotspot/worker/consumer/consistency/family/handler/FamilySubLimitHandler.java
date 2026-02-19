package hotspot.worker.consumer.consistency.family.handler;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.family.executor.FamilyLuaExecutor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class FamilySubLimitHandler implements FamilyEventHandler {

    private final FamilyLuaExecutor luaExecutor;

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("FAMILY_SUB_LIMIT_CHANGED");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = event.get("eventId").asText();
        long familyId = event.get("familyId").asLong();
        long subId = event.get("subId").asLong();
        long newLimit = event.get("newLimit").asLong();

        luaExecutor.executeSubLimit(eventId, familyId, subId, newLimit);
    }
}
