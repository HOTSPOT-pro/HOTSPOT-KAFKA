package hotspot.worker.consumer.consistency.family.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.consumer.consistency.family.router.FamilyEventRouter;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class FamilyEventConsumer {


    private final FamilyEventRouter router;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.topics.family-events}",
            groupId = "${app.consumer-groups.family}"
    )
    public void consume(String message) throws Exception {

        JsonNode node = objectMapper.readTree(message);

        if (node.isTextual()) {
            node = objectMapper.readTree(node.asText());
        }

        router.route(node);
    }
}
