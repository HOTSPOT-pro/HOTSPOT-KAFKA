package hotspot.worker.consumer.usage.service;

import org.springframework.stereotype.Service;

import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageEvent;

/**
 * 이벤트 1건 처리 유스케이스 서비스
 */
@Service
public class UsageEventHandler {

    private final UsageLuaExecutor lua;

    public UsageEventHandler(UsageLuaExecutor lua) {
        this.lua = lua;
    }

    // 사용량 반영 + outbox 적재까지 Lua에서 원자 처리
    public void handle(UsageEvent ev) {
        UsageLuaResult result = lua.execute(ev);
        if (result.duplicate()) {
            return;
        }
    }
}
