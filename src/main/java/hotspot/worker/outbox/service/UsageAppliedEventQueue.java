package hotspot.worker.outbox.service;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class UsageAppliedEventQueue {

    private final BlockingQueue<UsageAppliedEnvelope> queue;

    // 내부 배치 처리용 큐를 설정된 용량으로 생성한다.
    public UsageAppliedEventQueue(
            @Value("${app.usage.db-writer.queue-capacity:20000}") int queueCapacity
    ) {
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
    }

    // 사용량 처리 결과를 내부 큐에 적재한다.
    public void enqueue(UsageAppliedEnvelope envelope) {
        try {
            queue.put(envelope);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while enqueueing usage applied envelope", e);
        }
    }

    // 지정한 대기시간 동안 큐에서 단건을 조회한다.
    public UsageAppliedEnvelope poll(long timeoutMillis) throws InterruptedException {
        return queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    // 큐의 데이터를 최대 개수만큼 대상 컬렉션으로 이동한다.
    public int drainTo(Collection<UsageAppliedEnvelope> target, int maxElements) {
        return queue.drainTo(target, maxElements);
    }

    // 현재 큐 적재 건수를 반환한다.
    public int size() {
        return queue.size();
    }
}
