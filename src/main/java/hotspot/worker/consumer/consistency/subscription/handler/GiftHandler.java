package hotspot.worker.consumer.consistency.subscription.handler;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.subscription.executor.SubscriptionLuaExecutor;
import hotspot.worker.producer.schema.AppType;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class GiftHandler implements SubscriptionEventHandler {

    private static final long GIFT_APP_ID = AppType.GIFT_DATA.getAppId();
    private final SubscriptionLuaExecutor executor;

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("GIFT_RECEIVED");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = event.get("eventId").asText();

        long receiverSubId = event.get("receiverSubId").asLong();
        long giverSubId = event.get("giverSubId").asLong();

        long giftId = event.get("giftId").asLong();
        long giftLimitBytes = event.get("giftLimitBytes").asLong();
        long giftAmountBytes = event.get("giftAmountBytes").asLong();

        String yyyyMM = event.get("yyyyMM").asText();
        String yyyyMMDD = event.get("yyyyMMDD").asText();

        executor.executeGift(
                eventId,
                receiverSubId,
                giftId,
                yyyyMM,
                giftLimitBytes,
                giverSubId,
                yyyyMMDD,
                giftAmountBytes,
                GIFT_APP_ID
        );
    }
}
