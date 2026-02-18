package hotspot.worker.common.config.kafka.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import hotspot.worker.producer.schema.UsageEvent;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UsageKafkaProducer {

    private final KafkaTemplate<String, UsageEvent> kafkaTemplate;

    @Value("${app.topics.usage-events}")
    private String topic;

    public void sendUsage(UsageEvent event) {
        kafkaTemplate.send(topic, event.eventId(), event);
    }
}
