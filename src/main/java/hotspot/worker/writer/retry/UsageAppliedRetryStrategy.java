package hotspot.worker.writer.retry;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class UsageAppliedRetryStrategy {

    private static final Logger log = LoggerFactory.getLogger(UsageAppliedRetryStrategy.class);
    private static final long DEFAULT_RETRY_WAIT_MILLIS = 1000L;
    public static final String REASON_DB_RETRY = "db-retry-exceeded";
    public static final String REASON_QUEUE_BACKPRESSURE = "queue-backpressure";

    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final int maxRetries;
    private final long pausedRetryWaitMillis;
    private final String listenerId;
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final Set<String> activePauseReasons = ConcurrentHashMap.newKeySet();
    private final Object pauseLock = new Object();

    public UsageAppliedRetryStrategy(
            KafkaListenerEndpointRegistry listenerRegistry,
            @Value("${app.usage.db-writer.max-retries:2}") int maxRetries,
            @Value("${app.usage.db-writer.paused-retry-wait-ms:10000}") long pausedRetryWaitMillis,
            @Value("${app.usage.db-writer.listener-id:usage-events-listener}") String listenerId
    ) {
        this.listenerRegistry = listenerRegistry;
        this.maxRetries = maxRetries;
        this.pausedRetryWaitMillis = pausedRetryWaitMillis;
        this.listenerId = listenerId;
    }

    public RetryContext newContext() {
        return new RetryContext();
    }

    public long onTransientFailure(
            RetryContext context,
            Exception e,
            int batchSize,
            int outboxSize,
            int appliedSize
    ) {
        int failures = context.incrementAndGet();
        boolean exceeded = failures > maxRetries;
        if (exceeded) {
            requestPause(REASON_DB_RETRY);
        }

        long waitMillis = exceeded ? pausedRetryWaitMillis : DEFAULT_RETRY_WAIT_MILLIS;
        log.error(
                "Transient batch persistence failure. size={}, outbox={}, applied={}, "
                        + "failure={}, maxRetries={}, paused={}",
                batchSize,
                outboxSize,
                appliedSize,
                failures,
                maxRetries,
                paused.get(),
                e
        );
        return waitMillis;
    }

    public void onSuccess() {
        releasePause(REASON_DB_RETRY);
    }

    public int maxRetries() {
        return maxRetries;
    }

    public long pausedRetryWaitMillis() {
        return pausedRetryWaitMillis;
    }

    public void requestPause(String reason) {
        synchronized (pauseLock) {
            if (!activePauseReasons.add(reason)) {
                return;
            }
            if (paused.get()) {
                return;
            }
            MessageListenerContainer container = listenerRegistry.getListenerContainer(listenerId);
            if (container == null) {
                log.error(
                        "Failed to pause listener because container not found. id={}, reason={}",
                        listenerId,
                        reason
                );
                return;
            }
            container.pause();
            paused.set(true);
            log.warn(
                    "Paused usage listener. id={}, reason={}, activeReasons={}",
                    listenerId,
                    reason,
                    activePauseReasons
            );
        }
    }

    public void releasePause(String reason) {
        synchronized (pauseLock) {
            if (!activePauseReasons.remove(reason)) {
                return;
            }
            if (!activePauseReasons.isEmpty()) {
                return;
            }
            if (!paused.get()) {
                return;
            }
            MessageListenerContainer container = listenerRegistry.getListenerContainer(listenerId);
            if (container == null) {
                log.error(
                        "Failed to resume listener because container not found. id={}, reason={}",
                        listenerId,
                        reason
                );
                return;
            }
            container.resume();
            paused.set(false);
            log.info("Resumed usage listener. id={}, reason={}", listenerId, reason);
        }
    }

    public static final class RetryContext {
        private int failures;

        private int incrementAndGet() {
            failures += 1;
            return failures;
        }
    }
}
