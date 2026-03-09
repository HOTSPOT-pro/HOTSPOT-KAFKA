package hotspot.worker.consumer.usage.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.outbox.service.UsageAlertOutboxAppender;

@Service
public class UsageEventHandler {

    private static final Logger log = LoggerFactory.getLogger(UsageEventHandler.class);

    private final UsageLuaExecutor lua;
    private final UsageAlertOutboxAppender outboxAppender;

    public UsageEventHandler(UsageLuaExecutor lua, UsageAlertOutboxAppender outboxAppender) {
        this.lua = lua;
        this.outboxAppender = outboxAppender;
    }

    // 사용량 이벤트를 Lua로 처리한다. 중복 또는 무효 이벤트면 종료하고, 유효하면 결과를 Outbox에 적재한다.
    public void handle(UsageEvent ev) {
        if (ev.bytes() <= 0) {
            log.warn(
                    "Ignore usage event due to non-positive bytes. eventId={}, subId={}, bytes={}",
                    ev.eventId(), ev.subId(), ev.bytes()
            );
            return;
        }

        UsageLuaResult result = lua.execute(ev);
        if (result.duplicate()) {
            return;
        }
        outboxAppender.append(ev, result);
    }
}
