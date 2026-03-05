package hotspot.worker.consumer.consistency.subscription.handler;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.subscription.executor.SubscriptionLuaExecutor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class PolicyHandler implements SubscriptionEventHandler {

    private final SubscriptionLuaExecutor executor;

    @Override
    public boolean supports(String eventType) {
        return "POLICY_SNAPSHOT".equals(eventType);
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = requireText(event, "eventId");
        long subId = requireLong(event, "subId");

        JsonNode policiesNode = event.get("policies");
        if (policiesNode == null || !policiesNode.isArray()) {
            throw new IllegalArgumentException("Missing or invalid 'policies' array: " + event);
        }

        executor.replacePolicies(eventId, subId, policiesNode);
    }

    private String requireText(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || !v.isTextual()) {
            throw new IllegalArgumentException("Missing or invalid '" + field + "': " + node);
        }
        return v.asText();
    }

    private long requireLong(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || !v.isNumber()) {
            throw new IllegalArgumentException("Missing or invalid '" + field + "': " + node);
        }
        return v.asLong();
    }
}
