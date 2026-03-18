package hotspot.worker.consumer.usage.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.outbox.service.UsageAlertOutboxAppender;
import hotspot.worker.writer.log.dto.UsageAppliedEnvelope;
import hotspot.worker.writer.log.support.UsageAppliedEventQueue;
import hotspot.worker.writer.log.support.UsageBatchAcknowledgment;
import hotspot.worker.writer.log.support.UsageQueueBackpressureController;

@Service
public class UsageEventHandler {

    private static final Logger log = LoggerFactory.getLogger(UsageEventHandler.class);
    private static final long PERF_LOG_EVERY = 10_000L;

    private final UsageLuaExecutor lua;
    private final UsageAlertOutboxAppender outboxAppender;
    private final UsageAppliedEventQueue queue;
    private final UsageQueueBackpressureController backpressureController;
    private final AtomicLong handledCount = new AtomicLong();
    private final AtomicLong totalHandleNanos = new AtomicLong();

    public UsageEventHandler(
            UsageLuaExecutor lua,
            UsageAlertOutboxAppender outboxAppender,
            UsageAppliedEventQueue queue,
            UsageQueueBackpressureController backpressureController
    ) {
        this.lua = lua;
        this.outboxAppender = outboxAppender;
        this.queue = queue;
        this.backpressureController = backpressureController;
    }

    public void handle(List<UsageEvent> events, Acknowledgment ack) {
        if (events == null || events.isEmpty()) {
            ack.acknowledge();
            return;
        }

        if (!backpressureController.tryReserveBatchSlots(events.size())) {
            throw new IllegalStateException(
                    "Usage queue backpressure: insufficient queue capacity for batch size=" + events.size()
            );
        }

        int enqueued = 0;
        UsageBatchAcknowledgment batchAcknowledgment = new UsageBatchAcknowledgment(ack, events.size());
        try {
            for (UsageEvent ev : events) {
                long start = System.nanoTime();
                UsageLuaResult result = lua.execute(ev);
                queue.enqueueReserved(new UsageAppliedEnvelope(
                        ev,
                        result,
                        outboxAppender.buildOutboxEntities(ev, result),
                        outboxAppender.buildAppliedLogEntity(ev, result),
                        batchAcknowledgment
                ));
                enqueued += 1;
                if (result.duplicate()) {
                    log.debug("사용량 이벤트가 중복 또는 무시 처리되었습니다. eventId={}", ev.eventId());
                }

                long elapsedNanos = System.nanoTime() - start;
                long count = handledCount.incrementAndGet();
                long total = totalHandleNanos.addAndGet(elapsedNanos);
                if (count % PERF_LOG_EVERY == 0) {
                    long avgMicros = (total / count) / 1000L;
                    log.info(
                            "사용량 핸들러 성능 지표입니다. handled={}, avgMicros={}, queueSize={}",
                            count,
                            avgMicros,
                            queue.size()
                    );
                }
            }
        } finally {
            int notUsed = events.size() - enqueued;
            if (notUsed > 0) {
                queue.releaseReservedSlots(notUsed);
            }
        }
    }
}
