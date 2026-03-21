package hotspot.worker.consumer.consistency.subscription.handler;

import com.fasterxml.jackson.databind.JsonNode;

public interface SubscriptionEventHandler {

    boolean supports(String eventType);

    void handle(JsonNode event);
}
