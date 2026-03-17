package hotspot.worker.producer.scheduler;


import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import hotspot.worker.producer.generator.UsageGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageScheduler {

    private final UsageGenerator usageGenerator;

    // 1초마다 실행
    @Scheduled(
            initialDelayString = "${app.usage.scheduler.initial-delay-ms:60000}",
            fixedRateString = "${app.usage.scheduler.fixed-rate-ms:1000}"
    )
    public void generate() {
        usageGenerator.produceEvent();
    }
}
