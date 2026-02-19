package hotspot.worker.consumer.consistency.subscription.handler;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.subscription.executor.SubscriptionLuaExecutor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SubscriptionLockHandler implements SubscriptionEventHandler {

    private final SubscriptionLuaExecutor executor;

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("SUBSCRIPTION_LOCKED")
                || eventType.equals("SUBSCRIPTION_UNLOCKED");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = event.get("eventId").asText();
        long subId = event.get("subId").asLong();
        String type = event.get("type").asText();

        String action = type.equals("SUBSCRIPTION_LOCKED") ? "LOCK" : "UNLOCK";
        executor.executeLock(eventId, subId, action);
    }
}
