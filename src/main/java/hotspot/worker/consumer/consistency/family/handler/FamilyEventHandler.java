package hotspot.worker.consumer.consistency.family.handler;

import com.fasterxml.jackson.databind.JsonNode;

public interface FamilyEventHandler {

    boolean supports(String eventType);

    void handle(JsonNode event);
}
