package hotspot.worker.consumer.consistency.subscription.handler;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import hotspot.worker.consumer.consistency.subscription.executor.SubscriptionLuaExecutor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class PolicyHandler implements SubscriptionEventHandler {

    private final SubscriptionLuaExecutor executor;

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("POLICY_APPLIED")
                || eventType.equals("POLICY_REMOVED");
    }

    @Override
    public void handle(JsonNode event) {

        String eventId = event.get("eventId").asText();
        long subId = event.get("subId").asLong();
        long policyId = event.get("policyId").asLong();
        String type = event.get("type").asText();

        if (type.equals("POLICY_APPLIED")) {

            String policyType = event.get("policyType").asText();

            if (policyType.equals("SCHEDULED")) {

                executor.executePolicy(
                        eventId,
                        subId,
                        "APPLY",
                        "SCHEDULED",
                        policyId,
                        event.get("encoded").asText(),
                        null
                );
            } else {

                executor.executePolicy(
                        eventId,
                        subId,
                        "APPLY",
                        "ONCE",
                        policyId,
                        null,
                        event.get("expireEpoch").asLong()
                );
            }

        } else {

            executor.executePolicy(
                    eventId,
                    subId,
                    "REMOVE",
                    "IGNORED",
                    policyId,
                    null,
                    null
            );
        }
    }
}
