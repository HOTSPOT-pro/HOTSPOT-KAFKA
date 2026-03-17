package hotspot.worker.producer.orchestrator;

import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import hotspot.worker.common.config.kafka.producer.UsageKafkaProducer;
import hotspot.worker.producer.repository.UsageValidationRepository;
import hotspot.worker.producer.schema.UsageEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageOrchestrator {

    private final UsageValidationRepository validationRepository;
    private final UsageKafkaProducer kafkaProducer;

    public void process(List<UsageEvent> events) {

        long start = System.currentTimeMillis();

        Set<String> approved = validationRepository.validateAndCheck(events);

        long afterValidation = System.currentTimeMillis();

        List<UsageEvent> finalEvents =
                events.stream()
                        .filter(e -> approved.contains(e.eventId()))
                        .toList();

        // Kafka Produce
        for (UsageEvent event : finalEvents) {
            kafkaProducer.sendUsage(event);
        }

        long end = System.currentTimeMillis();

        log.info("usage process completed. total={}, approved={}, validationMs={}, publishMs={}, totalMs={}",
                events.size(),
                finalEvents.size(),
                afterValidation - start,
                end - afterValidation,
                end - start);
    }
}
