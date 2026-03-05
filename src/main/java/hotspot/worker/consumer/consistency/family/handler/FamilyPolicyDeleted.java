package hotspot.worker.consumer.consistency.family.handler;

import com.fasterxml.jackson.databind.JsonNode;
import hotspot.worker.consumer.consistency.family.executor.FamilyLuaExecutor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class FamilyPolicyDeleted implements FamilyEventHandler {

    private final FamilyLuaExecutor executor;

    @Override
    public boolean supports(String eventType) {
        return "POLICY_DELETED".equals(eventType);
    }

    @Override
    public void handle(JsonNode event) {
        executor.removePolicy(event);
    }
}
