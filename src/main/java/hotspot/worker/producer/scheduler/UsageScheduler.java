package hotspot.worker.producer.scheduler;

import hotspot.worker.producer.generator.UsageGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

//@Slf4j
//@Component
//@RequiredArgsConstructor
//public class UsageScheduler {
//
//    private final UsageGenerator usageGenerator;
//
//    // 1초마다 실행
//    @Scheduled(fixedDelay = 1000)
//    public void generate() {
//        usageGenerator.produceEvent();
//    }
//}
