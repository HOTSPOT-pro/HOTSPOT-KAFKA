package hotspot.worker.writer.log.support;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import hotspot.worker.writer.retry.UsageAppliedRetryStrategy;

@Component
public class UsageQueueBackpressureController {

    private final UsageAppliedEventQueue queue;
    private final UsageAppliedRetryStrategy retryStrategy;
    private final int resumeThreshold;

    public UsageQueueBackpressureController(
            UsageAppliedEventQueue queue,
            UsageAppliedRetryStrategy retryStrategy,
            @Value("${app.usage.db-writer.backpressure.resume-ratio:0.8}") double resumeRatio
    ) {
        this.queue = queue;
        this.retryStrategy = retryStrategy;
        int clamped = (int) Math.floor(queue.capacity() * clampRatio(resumeRatio));
        this.resumeThreshold = clamped;
    }

    public boolean tryReserveBatchSlots(int batchSize) {
        if (batchSize <= 0) {
            return true;
        }
        if (batchSize > queue.capacity()) {
            throw new IllegalStateException(
                    "Kafka batch size exceeds queue capacity. batchSize=" + batchSize
                            + ", queueCapacity=" + queue.capacity()
            );
        }
        boolean reserved = queue.tryReserveSlots(batchSize);
        if (!reserved) {
            retryStrategy.requestPause(UsageAppliedRetryStrategy.REASON_QUEUE_BACKPRESSURE);
        }
        return reserved;
    }

    public void tryResumeOnDrain() {
        if (queue.size() <= resumeThreshold) {
            retryStrategy.releasePause(UsageAppliedRetryStrategy.REASON_QUEUE_BACKPRESSURE);
        }
    }

    private static double clampRatio(double ratio) {
        return Math.max(0.0, Math.min(1.0, ratio));
    }
}
