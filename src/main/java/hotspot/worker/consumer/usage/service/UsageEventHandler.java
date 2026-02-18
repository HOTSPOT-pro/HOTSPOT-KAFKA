package hotspot.worker.consumer.usage.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import hotspot.worker.consumer.usage.domain.GiftFire;
import hotspot.worker.consumer.usage.domain.UsageLuaResult;
import hotspot.worker.consumer.usage.schema.UsageAlertEvent;
import hotspot.worker.consumer.usage.schema.UsageEvent;

/**
 * 이벤트 1건 처리 유스케이스 서비스
 */
@Service
public class UsageEventHandler {

    private final UsageLuaExecutor lua;
    private final AlertPublisher publisher;

    public UsageEventHandler(UsageLuaExecutor lua, AlertPublisher publisher) {
        this.lua = lua;
        this.publisher = publisher;
    }

    // Lua 실행 결과를 바탕으로 필요한 알림만 발행
    public void handle(UsageEvent ev) {
        UsageLuaResult result = lua.execute(ev);
        if (result.duplicate()) {
            return;
        }

        List<UsageAlertEvent> alerts = new ArrayList<>();

        if (result.planFired()) {
            alerts.add(new UsageAlertEvent(
                    "PLAN_REMAINING",
                    String.valueOf(result.planThreshold()),
                    ev.occurredAt(),
                    ev.subId(),
                    null,
                    null,
                    result.planRemainingBytes(),
                    result.planRemainingPct(),
                    ev.eventId()
            ));
        }

        if (result.familyFired()) {
            alerts.add(new UsageAlertEvent(
                    "FAMILY_POOL_REMAINING",
                    String.valueOf(result.familyThreshold()),
                    ev.occurredAt(),
                    null,
                    ev.familyId(),
                    null,
                    result.familyRemainingBytes(),
                    result.familyRemainingPct(),
                    ev.eventId()
            ));
        }

        for (GiftFire giftFire : result.giftFires()) {
            alerts.add(new UsageAlertEvent(
                    "GIFT_REMAINING",
                    String.valueOf(giftFire.th()),
                    ev.occurredAt(),
                    ev.subId(),
                    null,
                    giftFire.giftId(),
                    giftFire.rem(),
                    giftFire.pct(),
                    ev.eventId()
            ));
        }

        for (UsageAlertEvent alert : alerts) {
            String key = keyFor(alert);
            publisher.publish(key, alert);
        }
    }

    // 알림 타입에 따라 Kafka partition key를 생성
    private String keyFor(UsageAlertEvent event) {
        if ("GIFT_REMAINING".equals(event.alertType()) && event.giftId() != null) {
            return "gift:" + event.giftId();
        }
        if (event.subId() != null) {
            return "sub:" + event.subId();
        }
        if (event.familyId() != null) {
            return "family:" + event.familyId();
        }
        return "unknown";
    }
}
