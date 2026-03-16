package hotspot.worker.writer.log.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import hotspot.worker.writer.log.dto.UsageAppliedEnvelope;
import hotspot.worker.writer.log.support.UsageAppliedEventQueue;

@Component
public class UsageAppliedBatchCollector {

    private final UsageAppliedEventQueue queue;
    private final int batchSize;
    private final long maxWaitMillis;

    public UsageAppliedBatchCollector(
            UsageAppliedEventQueue queue,
            @Value("${app.usage.db-writer.batch-size:200}") int batchSize,
            @Value("${app.usage.db-writer.batch-wait-ms:200}") long maxWaitMillis
    ) {
        this.queue = queue;
        this.batchSize = batchSize;
        this.maxWaitMillis = maxWaitMillis;
    }

    public boolean collectBatch(List<UsageAppliedEnvelope> batch) throws InterruptedException {
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

    public int batchSize() {
        return batchSize;
    }

    public long maxWaitMillis() {
        return maxWaitMillis;
    }

    public int queueSize() {
        return queue.size();
    }
}
