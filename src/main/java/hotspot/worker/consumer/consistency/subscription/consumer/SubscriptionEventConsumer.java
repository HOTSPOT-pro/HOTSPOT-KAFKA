package hotspot.worker.consumer.consistency.subscription.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.consumer.consistency.subscription.router.SubscriptionEventRouter;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SubscriptionEventConsumer {

    private final SubscriptionEventRouter router;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.topics.subscription-events}",
            groupId = "${app.consumer-groups.subscription}"
    )
    public void consume(String message) throws Exception {

        JsonNode node = objectMapper.readTree(message);

        while (node.isTextual()) {
            node = objectMapper.readTree(node.asText());
        }

        router.route(node);
    }
}
