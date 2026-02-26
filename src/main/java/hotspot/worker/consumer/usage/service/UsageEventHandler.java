package hotspot.worker.consumer.usage.service;

import org.springframework.stereotype.Service;

import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageEvent;
import hotspot.worker.outbox.service.UsageAlertOutboxAppender;

@Service
public class UsageEventHandler {

    private final UsageLuaExecutor lua;
    private final UsageAlertOutboxAppender outboxAppender;

    public UsageEventHandler(UsageLuaExecutor lua, UsageAlertOutboxAppender outboxAppender) {
        this.lua = lua;
        this.outboxAppender = outboxAppender;
    }

    // 사용량 이벤트를 Lua로 처리한 뒤 중복이면 종료하고, 중복이 아니면 임계치 결과를 Outbox에 적재한다.
    public void handle(UsageEvent ev) {
        UsageLuaResult result = lua.execute(ev);
        if (result.duplicate()) {
            return;
        }
        outboxAppender.append(ev, result);
    }
}
