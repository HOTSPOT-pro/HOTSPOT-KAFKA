package hotspot.worker.consumer.usage.service;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;
import hotspot.worker.outbox.service.UsageAlertOutboxAppender;
import hotspot.worker.writer.infrastructure.entity.UsageAppliedEventLogEntity;
import hotspot.worker.writer.log.dto.UsageAppliedEnvelope;
import hotspot.worker.writer.log.support.UsageAppliedEventQueue;
import hotspot.worker.writer.log.support.UsageBatchAcknowledgment;
import hotspot.worker.writer.log.support.UsageQueueBackpressureController;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class UsageEventHandler {

    private static final Logger log = LoggerFactory.getLogger(UsageEventHandler.class);
    private static final long PERF_LOG_EVERY = 10_000L;

    private final UsageLuaExecutor lua;
    private final UsageAlertOutboxAppender outboxAppender;
    private final UsageAppliedEventQueue queue;
    private final UsageQueueBackpressureController backpressureController;
    private final Timer handlerTotalTimer;
    private final Timer handlerLuaTimer;
    private final Timer handlerOutboxTimer;
    private final Timer handlerEnqueueTimer;
    private final AtomicLong handledCount = new AtomicLong();
    private final AtomicLong totalHandleNanos = new AtomicLong();
    private final AtomicLong totalLuaNanos = new AtomicLong();
    private final AtomicLong totalOutboxNanos = new AtomicLong();
    private final AtomicLong totalEnqueueNanos = new AtomicLong();

    public UsageEventHandler(
            UsageLuaExecutor lua,
            UsageAlertOutboxAppender outboxAppender,
            UsageAppliedEventQueue queue,
            UsageQueueBackpressureController backpressureController,
            MeterRegistry meterRegistry
    ) {
        this.lua = lua;
        this.outboxAppender = outboxAppender;
        this.queue = queue;
        this.backpressureController = backpressureController;
        this.handlerTotalTimer = Timer.builder("hotspot.usage.handler.total")
                .description("Usage handler total time per event")
                .register(meterRegistry);
        this.handlerLuaTimer = Timer.builder("hotspot.usage.handler.lua")
                .description("Usage handler Lua execution time per event")
                .register(meterRegistry);
        this.handlerOutboxTimer = Timer.builder("hotspot.usage.handler.outbox")
                .description("Usage handler outbox/build entity time per event")
                .register(meterRegistry);
        this.handlerEnqueueTimer = Timer.builder("hotspot.usage.handler.enqueue")
                .description("Usage handler queue enqueue time per event")
                .register(meterRegistry);
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

                long luaStart = System.nanoTime();
                UsageLuaResult result = lua.execute(ev);
                long luaElapsedNanos = System.nanoTime() - luaStart;

                long outboxStart = System.nanoTime();
                List<NotificationOutboxEventEntity> outboxEntities = outboxAppender.buildOutboxEntities(ev, result);
                UsageAppliedEventLogEntity appliedLogEntity = outboxAppender.buildAppliedLogEntity(ev, result);
                long outboxElapsedNanos = System.nanoTime() - outboxStart;

                long enqueueStart = System.nanoTime();
                queue.enqueueReserved(new UsageAppliedEnvelope(
                        ev,
                        result,
                        outboxEntities,
                        appliedLogEntity,
                        batchAcknowledgment
                ));
                long enqueueElapsedNanos = System.nanoTime() - enqueueStart;

                enqueued += 1;
                if (result.duplicate()) {
                    log.debug("사용량 이벤트 중복/무시 처리됨. eventId={}", ev.eventId());
                }

                long elapsedNanos = System.nanoTime() - start;
                handlerTotalTimer.record(elapsedNanos, TimeUnit.NANOSECONDS);
                handlerLuaTimer.record(luaElapsedNanos, TimeUnit.NANOSECONDS);
                handlerOutboxTimer.record(outboxElapsedNanos, TimeUnit.NANOSECONDS);
                handlerEnqueueTimer.record(enqueueElapsedNanos, TimeUnit.NANOSECONDS);

                long count = handledCount.incrementAndGet();
                long total = totalHandleNanos.addAndGet(elapsedNanos);
                long totalLua = totalLuaNanos.addAndGet(luaElapsedNanos);
                long totalOutbox = totalOutboxNanos.addAndGet(outboxElapsedNanos);
                long totalEnqueue = totalEnqueueNanos.addAndGet(enqueueElapsedNanos);
                if (count % PERF_LOG_EVERY == 0) {
                    long avgMicros = (total / count) / 1000L;
                    long avgLuaMicros = (totalLua / count) / 1000L;
                    long avgOutboxMicros = (totalOutbox / count) / 1000L;
                    long avgEnqueueMicros = (totalEnqueue / count) / 1000L;
                    log.info(
                            "사용량 핸들러 성능 지표: 처리건수={}, 평균처리={}µs, Lua평균={}µs, Outbox평균={}µs, Enqueue평균={}µs, 큐사이즈={}",
                            count,
                            avgMicros,
                            avgLuaMicros,
                            avgOutboxMicros,
                            avgEnqueueMicros,
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
