package hotspot.worker.consumer.consistency.subscription.handler;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.subscription.executor.SubscriptionLuaExecutor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class PlanChangedHandler implements SubscriptionEventHandler {

    private final SubscriptionLuaExecutor executor;

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("PLAN_CHANGED");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = event.get("eventId").asText();
        long subId = event.get("subId").asLong();
        long limit = event.get("planLimit").asLong();

        executor.executePlanChanged(eventId, subId, limit);
    }
}
