package hotspot.worker.consumer.consistency.family.handler;

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

        String eventId = event.get("eventId").asText();
        long familyId = event.get("familyId").asLong();

        List<Long> members = new ArrayList<>();

        for (JsonNode node : event.get("members")) {
            members.add(node.asLong());
        }

        luaExecutor.executeCreate(
                eventId,
                familyId,
                members
        );
    }
}
