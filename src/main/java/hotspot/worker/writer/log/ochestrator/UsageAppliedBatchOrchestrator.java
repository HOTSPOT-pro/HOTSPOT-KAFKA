package hotspot.worker.writer.log.ochestrator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;
import hotspot.worker.writer.infrastructure.UsageAppliedFailedEventJdbcWriter;
import hotspot.worker.writer.infrastructure.entity.UsageAppliedEventLogEntity;
import hotspot.worker.writer.log.dto.UsageAppliedEnvelope;
import hotspot.worker.writer.log.dto.UsageAppliedFailedEvent;
import hotspot.worker.writer.log.service.UsageAppliedBatchService;
import hotspot.worker.writer.log.support.UsageAppliedFailureFactory;
import hotspot.worker.writer.retry.UsageAppliedFailureClassifier;
import hotspot.worker.writer.retry.UsageAppliedRetryStrategy;

@Component
public class UsageAppliedBatchOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(UsageAppliedBatchOrchestrator.class);

    private final UsageAppliedBatchService persistenceService;
    private final UsageAppliedFailedEventJdbcWriter failedEventWriter;
    private final UsageAppliedFailureClassifier failureClassifier;
    private final UsageAppliedRetryStrategy retryStrategy;
    private final UsageAppliedFailureFactory failurePayloadFactory;

    public UsageAppliedBatchOrchestrator(
            UsageAppliedBatchService persistenceService,
            UsageAppliedFailedEventJdbcWriter failedEventWriter,
            UsageAppliedFailureClassifier failureClassifier,
            UsageAppliedRetryStrategy retryStrategy,
            UsageAppliedFailureFactory failurePayloadFactory
    ) {
        this.persistenceService = persistenceService;
        this.failedEventWriter = failedEventWriter;
        this.failureClassifier = failureClassifier;
        this.retryStrategy = retryStrategy;
        this.failurePayloadFactory = failurePayloadFactory;
    }

    public void persist(List<UsageAppliedEnvelope> batch, BooleanSupplier running) throws InterruptedException {
        List<NotificationOutboxEventEntity> outboxEvents = new ArrayList<>();
        List<UsageAppliedEventLogEntity> appliedLogs = new ArrayList<>();
        for (UsageAppliedEnvelope envelope : batch) {
            outboxEvents.addAll(envelope.outboxEvents());
            if (envelope.usageAppliedLog() != null) {
                appliedLogs.add(envelope.usageAppliedLog());
            }
        }

        UsageAppliedRetryStrategy.RetryContext retryContext = retryStrategy.newContext();
        while (true) {
            try {
                if (!outboxEvents.isEmpty() || !appliedLogs.isEmpty()) {
                    persistenceService.persistBatch(outboxEvents, appliedLogs);
                }
                retryStrategy.onSuccess();
                return;
            } catch (Exception e) {
                if (!running.getAsBoolean()) {
                    throw e;
                }

                if (failureClassifier.isPermanent(e)) {
                    parkFailedBatch(batch, e);
                    retryStrategy.onSuccess();
                    return;
                }

                long waitMillis = retryStrategy.onTransientFailure(
                        retryContext,
                        e,
                        batch.size(),
                        outboxEvents.size(),
                        appliedLogs.size()
                );
                Thread.sleep(waitMillis);
            }
        }
    }

    private void parkFailedBatch(List<UsageAppliedEnvelope> batch, Exception cause) {
        List<UsageAppliedFailedEvent> failures = new ArrayList<>(batch.size());
        for (UsageAppliedEnvelope envelope : batch) {
            failures.add(failurePayloadFactory.create(envelope, cause));
        }
        failedEventWriter.saveAll(failures);
        log.error(
                "Permanent persistence failure detected. "
                        + "Parked batch to usage_applied_failed_event and will ACK. batchSize={}",
                batch.size(),
                cause
        );
    }
}
