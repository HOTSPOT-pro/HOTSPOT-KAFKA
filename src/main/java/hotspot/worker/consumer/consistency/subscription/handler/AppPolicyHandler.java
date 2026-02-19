package hotspot.worker.consumer.consistency.subscription.handler;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.subscription.executor.SubscriptionLuaExecutor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class AppPolicyHandler implements SubscriptionEventHandler {

    private final SubscriptionLuaExecutor executor;

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("APP_BLOCKED")
                || eventType.equals("APP_UNBLOCKED");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = event.get("eventId").asText();
        long subId = event.get("subId").asLong();
        long appId = event.get("appId").asLong();
        String type = event.get("type").asText();

        String action = type.equals("APP_BLOCKED") ? "APPLY" : "REMOVE";

        executor.executeAppPolicy(eventId, subId, action, appId);
    }
}
