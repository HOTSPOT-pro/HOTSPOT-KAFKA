package hotspot.worker.consumer.usage.service;

import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import hotspot.worker.consumer.usage.schema.UsageEvent;

// usage-events 토픽 수신 진입점이다.
@Component
public class UsageEventsConsumer {

    private final UsageEventHandler handler;

    // usage 이벤트 처리 핸들러를 주입받는다.
    public UsageEventsConsumer(UsageEventHandler handler) {
        this.handler = handler;
    }

    // 수신한 usage 이벤트를 처리하고 성공 시에만 수동 ACK를 수행한다.
    @KafkaListener(
            id = "usage-events-listener",
            topics = "${app.topics.usage-events}",
            groupId = "${app.consumer-groups.usage}",
            containerFactory = "usageKafkaListenerContainerFactory"
    )
    public void onMessage(List<UsageEvent> events, Acknowledgment ack) {
        handler.handle(events, ack);
    }
}
