package hotspot.worker.consumer.consistency.family.router;

import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.family.handler.FamilyEventHandler;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class FamilyEventRouter {

    private final List<FamilyEventHandler> handlers;

    public void route(JsonNode event) {

        String type = event.get("type").asText();

        handlers.stream()
                .filter(h -> h.supports(type))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No handler for " + type))
                .handle(event);
    }
}
