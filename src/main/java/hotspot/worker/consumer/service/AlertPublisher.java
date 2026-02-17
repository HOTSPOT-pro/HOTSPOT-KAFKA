package hotspot.worker.consumer.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import hotspot.worker.consumer.schema.UsageAlertEvent;

/**
 * 알림 이벤트를 Kafka로 발행하는 서비스
 */
@Service
public class AlertPublisher {

    private final KafkaTemplate<String, UsageAlertEvent> kafka;
    private final String topic;

    public AlertPublisher(
            KafkaTemplate<String, UsageAlertEvent> kafka,
            @Value("${app.topics.usage-alert-events}") String topic
    ) {
        this.kafka = kafka;
        this.topic = topic;
    }

    // 발행 성공까지 대기하고 실패 시 예외를 발생
    public void publish(String key, UsageAlertEvent event) {
        kafka.send(topic, key, event).join();
    }
}
