package hotspot.worker.consumer.consistency.subscription.handler;

import java.util.ArrayList;
import java.util.List;

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
        return eventType.equals("APP_BLOCK_LIST_UPDATED");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = event.get("eventId").asText();
        long subId = event.get("subId").asLong();

        JsonNode appIdsNode = event.get("appIds");

        List<String> appIds = new ArrayList<>();

        if (appIdsNode != null && appIdsNode.isArray()) {
            for (JsonNode node : appIdsNode) {
                appIds.add(node.asText());
            }
        }

        executor.executeAppPolicySnapshot(eventId, subId, appIds);
    }
}
