package hotspot.worker.consumer.usage.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import hotspot.worker.consumer.usage.schema.UsageEvent;

/**
 * usage-events 수신 진입점
 */
@Component
public class UsageEventsConsumer {

    private final UsageEventHandler handler;

    public UsageEventsConsumer(UsageEventHandler handler) {
        this.handler = handler;
    }

    // 처리 성공 시에만 수동 ACK를 수행
    @KafkaListener(
            topics = "${app.topics.usage-events}",
            groupId = "${app.consumer-groups.usage}",
            containerFactory = "usageKafkaListenerContainerFactory"
    )
    public void onMessage(UsageEvent ev, Acknowledgment ack) {
        handler.handle(ev);
        ack.acknowledge();
    }
}
