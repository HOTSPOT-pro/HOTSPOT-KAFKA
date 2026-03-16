package hotspot.worker.writer.retry;

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

    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final int maxRetries;
    private final long pausedRetryWaitMillis;
    private final String listenerId;
    private final AtomicBoolean paused = new AtomicBoolean(false);

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
            pauseIfNeeded();
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
        resumeIfPaused();
    }

    public int maxRetries() {
        return maxRetries;
    }

    public long pausedRetryWaitMillis() {
        return pausedRetryWaitMillis;
    }

    private void pauseIfNeeded() {
        if (!paused.compareAndSet(false, true)) {
            return;
        }
        MessageListenerContainer container = listenerRegistry.getListenerContainer(listenerId);
        if (container == null) {
            log.error("Failed to pause listener because container not found. id={}", listenerId);
            return;
        }
        container.pause();
        log.warn("Paused usage listener after transient DB retries exceeded. id={}", listenerId);
    }

    private void resumeIfPaused() {
        if (!paused.compareAndSet(true, false)) {
            return;
        }
        MessageListenerContainer container = listenerRegistry.getListenerContainer(listenerId);
        if (container == null) {
            log.error("Failed to resume listener because container not found. id={}", listenerId);
            return;
        }
        container.resume();
        log.info("Resumed usage listener after persistence recovered. id={}", listenerId);
    }

    public static final class RetryContext {
        private int failures;

        private int incrementAndGet() {
            failures += 1;
            return failures;
        }
    }
}
