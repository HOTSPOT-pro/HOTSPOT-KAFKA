package hotspot.worker.writer.log.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
    private static final long PERF_LOG_EVERY = 1000L;

    private final UsageAppliedBatchCollector batchCollector;
    private final UsageAppliedBatchOrchestrator persistenceOrchestrator;
    private final UsageAppliedRetryStrategy retryStrategy;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong persistedCount = new AtomicLong();
    private final AtomicLong totalPersistNanos = new AtomicLong();
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
                long persistStart = System.nanoTime();
                persistenceOrchestrator.persist(batch, running::get);
                long persistElapsedNanos = System.nanoTime() - persistStart;
                acknowledgeBatch(batch);
                logPerf(batch.size(), persistElapsedNanos);
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

    private void logPerf(int batchItems, long persistElapsedNanos) {
        long previous = persistedCount.getAndAdd(batchItems);
        long count = previous + batchItems;
        long totalNanos = totalPersistNanos.addAndGet(persistElapsedNanos);
        if ((previous / PERF_LOG_EVERY) != (count / PERF_LOG_EVERY)) {
            long avgMicros = (totalNanos / count) / 1000L;
            log.info(
                    "비동기 DB writer 성능 지표입니다. persisted={}, avgPersistMicros={}, queueSize={}, batchItems={}",
                    count,
                    avgMicros,
                    batchCollector.queueSize(),
                    batchItems
            );
        }
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
