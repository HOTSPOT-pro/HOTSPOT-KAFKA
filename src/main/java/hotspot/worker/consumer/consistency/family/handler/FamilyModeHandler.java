package hotspot.worker.consumer.consistency.family.handler;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.family.dto.PriorityDto;
import hotspot.worker.consumer.consistency.family.executor.FamilyLuaExecutor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class FamilyModeHandler implements FamilyEventHandler {

    private final FamilyLuaExecutor executor;

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("FAMILY_MODE_CHANGED");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = event.get("eventId").asText();
        long familyId = event.get("familyId").asLong();
        String mode = event.get("mode").asText();

        List<PriorityDto> priorities = new ArrayList<>();

        if (mode.equals("PRIORITY")) {

            for (JsonNode node : event.get("priorities")) {
                priorities.add(
                        new PriorityDto(
                                node.get("subId").asLong(),
                                node.get("priority").asLong()
                        )
                );
            }
        }

        executor.executeMode(eventId, familyId, mode, priorities);
    }
}
