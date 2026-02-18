package hotspot.worker.consumer.notification;

import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import hotspot.worker.common.config.kafka.mapper.UsageAlertMapper;
import hotspot.worker.consumer.notification.repository.NotificationJdbcRepository;
import hotspot.worker.consumer.usage.schema.UsageAlertEvent;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UsageAlertConsumer {

    private final NotificationJdbcRepository repository;
    private final UsageAlertMapper usageAlertMapper;

    @KafkaListener(
            topics = "${app.topics.usage-alert-events}",
            groupId = "${app.consumer-groups.alert}",
            containerFactory = "alertKafkaListenerContainerFactory"
    )
    public void consume(UsageAlertEvent event, Acknowledgment ack) {

        String type = usageAlertMapper.resolveType(event);
        String message = usageAlertMapper.resolveMessage(event);

        if (event.subId() != null && event.familyId() == null && event.giftId() == null) {
            repository.insert(event.subId(), type, message, event.sourceEventId());
        }

        else if (event.familyId() != null) {
            List<Long> members = repository.findFamilyMembers(event.familyId());
            for (Long subId : members) {
                repository.insert(subId, type, message, event.sourceEventId());
            }
        }

        else if (event.giftId() != null) {
            repository.insert(event.subId(), type, message, event.sourceEventId());
        }

        ack.acknowledge();
    }
}
