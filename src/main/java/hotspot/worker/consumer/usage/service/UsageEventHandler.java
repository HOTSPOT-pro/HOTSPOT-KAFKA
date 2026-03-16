package hotspot.worker.consumer.usage.service;

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

@Service
public class UsageEventHandler {

    private static final Logger log = LoggerFactory.getLogger(UsageEventHandler.class);
    private static final long PERF_LOG_EVERY = 1000L;

    private final UsageLuaExecutor lua;
    private final UsageAlertOutboxAppender outboxAppender;
    private final UsageAppliedEventQueue queue;
    private final AtomicLong handledCount = new AtomicLong();
    private final AtomicLong totalHandleNanos = new AtomicLong();

    // 사용량 처리 의존 컴포넌트를 주입받는다.
    public UsageEventHandler(
            UsageLuaExecutor lua,
            UsageAlertOutboxAppender outboxAppender,
            UsageAppliedEventQueue queue
    ) {
        this.lua = lua;
        this.outboxAppender = outboxAppender;
        this.queue = queue;
    }

    // 사용량 이벤트를 Lua로 처리하고 후속 적재 큐에 전달한다.
    public void handle(UsageEvent ev, Acknowledgment ack) {
        long start = System.nanoTime();
        UsageLuaResult result = lua.execute(ev);
        queue.enqueue(new UsageAppliedEnvelope(
                ev,
                result,
                outboxAppender.buildOutboxEntities(ev, result),
                outboxAppender.buildAppliedLogEntity(ev, result),
                ack
        ));
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
}
