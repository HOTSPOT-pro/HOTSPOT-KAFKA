package hotspot.worker.consumer.consistency.subscription.router;

import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.subscription.handler.SubscriptionEventHandler;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SubscriptionEventRouter {

    private final List<SubscriptionEventHandler> handlers;

    public void route(JsonNode event) {

        JsonNode typeNode = event.get("type");
        if (typeNode == null || !typeNode.isTextual()) {
            throw new IllegalArgumentException("Missing or invalid 'type' field: " + event);
        }

        String type = typeNode.asText();

        handlers.stream()
                .filter(h -> h.supports(type))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No handler for " + type))
                .handle(event);
    }
}
