package hotspot.worker.outbox.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;
import hotspot.worker.outbox.infrastructure.entity.UsageAppliedEventLogEntity;

@Component
public class UsageAppliedLogWriter {

    private static final Logger log = LoggerFactory.getLogger(UsageAppliedLogWriter.class);
    private static final long FAILURE_RETRY_WAIT_MILLIS = 1000L;
    private static final int QUEUE_WARN_THRESHOLD = 5000;

    private final UsageAppliedEventQueue queue;
    private final UsageAppliedBatchPersistenceService persistenceService;
    private final int batchSize;
    private final long maxWaitMillis;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread workerThread;

    // 배치 쓰기 워커 구동에 필요한 의존성과 설정값을 주입받는다.
    public UsageAppliedLogWriter(
            UsageAppliedEventQueue queue,
            UsageAppliedBatchPersistenceService persistenceService,
            @Value("${app.usage.db-writer.batch-size:200}") int batchSize,
            @Value("${app.usage.db-writer.batch-wait-ms:200}") long maxWaitMillis
    ) {
        this.queue = queue;
        this.persistenceService = persistenceService;
        this.batchSize = batchSize;
        this.maxWaitMillis = maxWaitMillis;
    }

    // 애플리케이션 시작 시 배치 쓰기 워커 스레드를 시작한다.
    @PostConstruct
    public void start() {
        running.set(true);
        workerThread = new Thread(this::runLoop, "usage-applied-log-writer");
        workerThread.start();
        log.info("사용량 적용 로그 라이터가 시작되었습니다. batchSize={}, batchWaitMs={}", batchSize, maxWaitMillis);
    }

    // 애플리케이션 종료 시 워커 스레드를 안전하게 중지한다.
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

    // 내부 큐를 배치 단위로 소비해 저장과 ACK를 반복 수행한다.
    private void runLoop() {
        List<UsageAppliedEnvelope> batch = new ArrayList<>(batchSize);
        while (running.get() || !batch.isEmpty()) {
            try {
                if (!collectBatch(batch) && !running.get()) {
                    break;
                }
                if (batch.isEmpty()) {
                    continue;
                }
                persistWithRetry(batch);
                acknowledgeBatch(batch);
                logPerf(batch.size());
                batch.clear();
            } catch (InterruptedException e) {
                if (!running.get()) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception e) {
                log.error("UsageAppliedLogWriter 루프에서 예기치 못한 오류가 발생했습니다.", e);
            }
        }
    }

    // 큐에서 첫 항목을 가져오고 가능한 만큼 배치를 채운다.
    private boolean collectBatch(List<UsageAppliedEnvelope> batch) throws InterruptedException {
        if (!batch.isEmpty()) {
            return true;
        }
        UsageAppliedEnvelope first = queue.poll(maxWaitMillis);
        if (first == null) {
            return false;
        }
        batch.add(first);
        queue.drainTo(batch, batchSize - 1);
        return true;
    }

    // 배치 데이터를 저장 실패 시 재시도 정책으로 영속화한다.
    private void persistWithRetry(List<UsageAppliedEnvelope> batch) throws InterruptedException {
        List<NotificationOutboxEventEntity> outboxEvents = new ArrayList<>();
        List<UsageAppliedEventLogEntity> appliedLogs = new ArrayList<>();
        for (UsageAppliedEnvelope envelope : batch) {
            outboxEvents.addAll(envelope.outboxEvents());
            if (envelope.usageAppliedLog() != null) {
                appliedLogs.add(envelope.usageAppliedLog());
            }
        }

        while (true) {
            try {
                if (!outboxEvents.isEmpty() || !appliedLogs.isEmpty()) {
                    persistenceService.persistBatch(outboxEvents, appliedLogs);
                }
                return;
            } catch (Exception e) {
                if (!running.get()) {
                    throw e;
                }
                log.error(
                        "사용량 적용 배치 저장에 실패하여 재시도합니다. size={}, outbox={}, applied={}",
                        batch.size(),
                        outboxEvents.size(),
                        appliedLogs.size(),
                        e
                );
                Thread.sleep(FAILURE_RETRY_WAIT_MILLIS);
            }
        }
    }

    // 저장이 완료된 배치의 Kafka ACK를 수행한다.
    private void acknowledgeBatch(List<UsageAppliedEnvelope> batch) {
        for (UsageAppliedEnvelope envelope : batch) {
            envelope.acknowledgment().acknowledge();
        }
    }

    // 큐 적체 임계치 초과 시 경고 로그를 남긴다.
    private void logPerf(int batchItems) {
        int queueSize = queue.size();
        if (queueSize >= QUEUE_WARN_THRESHOLD) {
            log.warn(
                    "UsageAppliedLogWriter 큐 적체 경고입니다. queueSize={}, batchItems={}",
                    queueSize,
                    batchItems
            );
        }
    }
}
