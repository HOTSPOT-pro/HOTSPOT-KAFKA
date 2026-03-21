package hotspot.worker.writer.log.service;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import hotspot.worker.outbox.infrastructure.NotificationOutboxEventJpaRepository;
import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;
import hotspot.worker.writer.infrastructure.UsageAppliedEventLogJdbcWriter;
import hotspot.worker.writer.infrastructure.entity.UsageAppliedEventLogEntity;

@Service
public class UsageAppliedBatchService {

    private final NotificationOutboxEventJpaRepository outboxRepository;
    private final UsageAppliedEventLogJdbcWriter usageAppliedWriter;

    // 배치 저장에 필요한 저장소를 주입받는다.
    public UsageAppliedBatchService(
            NotificationOutboxEventJpaRepository outboxRepository,
            UsageAppliedEventLogJdbcWriter usageAppliedWriter
    ) {
        this.outboxRepository = outboxRepository;
        this.usageAppliedWriter = usageAppliedWriter;
    }

    // outbox 이벤트와 사용량 적용 로그를 하나의 트랜잭션으로 저장한다.
    @Transactional
    public void persistBatch(
            List<NotificationOutboxEventEntity> outboxEvents,
            List<UsageAppliedEventLogEntity> usageAppliedLogs
    ) {
        if (!usageAppliedLogs.isEmpty()) {
            usageAppliedWriter.saveAll(usageAppliedLogs);
        }
        if (!outboxEvents.isEmpty()) {
            outboxRepository.saveAll(outboxEvents);
        }
    }
}
