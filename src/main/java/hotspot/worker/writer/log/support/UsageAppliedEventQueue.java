package hotspot.worker.writer.log.support;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import hotspot.worker.writer.log.dto.UsageAppliedEnvelope;

@Component
public class UsageAppliedEventQueue {

    private final BlockingQueue<UsageAppliedEnvelope> queue;
    private final Semaphore availableSlots;
    private final int capacity;

    public UsageAppliedEventQueue(
            @Value("${app.usage.db-writer.queue-capacity:20000}") int queueCapacity
    ) {
        this.capacity = queueCapacity;
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
        this.availableSlots = new Semaphore(queueCapacity, true);
    }

    public boolean tryReserveSlots(int count) {
        if (count <= 0) {
            return true;
        }
        return availableSlots.tryAcquire(count);
    }

    public void releaseReservedSlots(int count) {
        if (count <= 0) {
            return;
        }
        availableSlots.release(count);
    }

    public void enqueueReserved(UsageAppliedEnvelope envelope) {
        if (!queue.offer(envelope)) {
            availableSlots.release();
            throw new IllegalStateException("Failed to enqueue reserved usage envelope");
        }
    }

    public UsageAppliedEnvelope poll(long timeoutMillis) throws InterruptedException {
        UsageAppliedEnvelope envelope = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        if (envelope != null) {
            availableSlots.release();
        }
        return envelope;
    }

    public int drainTo(Collection<UsageAppliedEnvelope> target, int maxElements) {
        int drained = queue.drainTo(target, maxElements);
        if (drained > 0) {
            availableSlots.release(drained);
        }
        return drained;
    }

    public int size() {
        return queue.size();
    }

    public int capacity() {
        return capacity;
    }
}
