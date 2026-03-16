package hotspot.worker.writer.log.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import hotspot.worker.writer.log.dto.UsageAppliedEnvelope;
import hotspot.worker.writer.log.ochestrator.UsageAppliedBatchOrchestrator;
import hotspot.worker.writer.retry.UsageAppliedRetryStrategy;

@Component
public class UsageAppliedLogWriter {

    private static final Logger log = LoggerFactory.getLogger(UsageAppliedLogWriter.class);
    private static final int QUEUE_WARN_THRESHOLD = 5000;

    private final UsageAppliedBatchCollector batchCollector;
    private final UsageAppliedBatchOrchestrator persistenceOrchestrator;
    private final UsageAppliedRetryStrategy retryStrategy;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread workerThread;

    public UsageAppliedLogWriter(
            UsageAppliedBatchCollector batchCollector,
            UsageAppliedBatchOrchestrator persistenceOrchestrator,
            UsageAppliedRetryStrategy retryStrategy
    ) {
        this.batchCollector = batchCollector;
        this.persistenceOrchestrator = persistenceOrchestrator;
        this.retryStrategy = retryStrategy;
    }

    @PostConstruct
    public void start() {
        running.set(true);
        workerThread = new Thread(this::runLoop, "usage-applied-log-writer");
        workerThread.start();
        log.info(
                "UsageAppliedLogWriter started. batchSize={}, batchWaitMs={}, maxRetries={}, pausedRetryWaitMs={}",
                batchCollector.batchSize(),
                batchCollector.maxWaitMillis(),
                retryStrategy.maxRetries(),
                retryStrategy.pausedRetryWaitMillis()
        );
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (workerThread != null) {
            workerThread.interrupt();
            try {
                workerThread.join(5000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runLoop() {
        List<UsageAppliedEnvelope> batch = new ArrayList<>(batchCollector.batchSize());
        while (running.get() || !batch.isEmpty()) {
            try {
                if (!batchCollector.collectBatch(batch) && !running.get()) {
                    break;
                }
                if (batch.isEmpty()) {
                    continue;
                }
                persistenceOrchestrator.persist(batch, running::get);
                acknowledgeBatch(batch);
                logPerf(batch.size());
                batch.clear();
            } catch (InterruptedException e) {
                if (!running.get()) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception e) {
                log.error("Unexpected error in UsageAppliedLogWriter loop.", e);
            }
        }
    }

    private void acknowledgeBatch(List<UsageAppliedEnvelope> batch) {
        for (UsageAppliedEnvelope envelope : batch) {
            envelope.acknowledgment().acknowledge();
        }
    }

    private void logPerf(int batchItems) {
        int queueSize = batchCollector.queueSize();
        if (queueSize >= QUEUE_WARN_THRESHOLD) {
            log.warn(
                    "UsageAppliedLogWriter queue warning. queueSize={}, batchItems={}",
                    queueSize,
                    batchItems
            );
        }
    }
}
