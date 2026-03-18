package hotspot.worker.writer.log.support;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

public class UsageBatchAcknowledgment {

    private static final Logger log = LoggerFactory.getLogger(UsageBatchAcknowledgment.class);

    private final Acknowledgment acknowledgment;
    private final AtomicInteger remaining;
    private final AtomicBoolean acknowledged = new AtomicBoolean(false);

    public UsageBatchAcknowledgment(Acknowledgment acknowledgment, int batchSize) {
        this.acknowledgment = acknowledgment;
        this.remaining = new AtomicInteger(batchSize);
    }

    public void markPersisted() {
        int left = remaining.decrementAndGet();
        if (left < 0) {
            log.error("markPersisted() called more times than batch size. left={}", left);
            return;
        }
        if (left == 0 && acknowledged.compareAndSet(false, true)) {
            acknowledgment.acknowledge();
        }
    }
}
