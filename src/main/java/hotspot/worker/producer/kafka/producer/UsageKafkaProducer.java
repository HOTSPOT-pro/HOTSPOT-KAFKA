package hotspot.worker.producer.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import hotspot.worker.producer.schema.UsageEvent;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UsageKafkaProducer {

    private final KafkaTemplate<String, UsageEvent> kafkaTemplate;

    private static final String TOPIC = "usage-events";

    public void sendUsage(UsageEvent event) {
        kafkaTemplate.send(TOPIC, event.eventId(), event);
    }
}
