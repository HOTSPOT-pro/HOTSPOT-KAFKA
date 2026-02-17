package hotspot.worker.common.config.kafka.mapper;

import org.springframework.stereotype.Component;

import hotspot.worker.consumer.usage.schema.UsageAlertEvent;

@Component
public class UsageAlertMapper {

    public String resolveType(UsageAlertEvent ev) {

        int th = Integer.parseInt(ev.threshold());

        return switch (ev.alertType()) {

            case "PLAN_REMAINING" -> switch (th) {
                case 50 -> "SINGLE_USAGE_THRESHOLD_50";
                case 30 -> "SINGLE_USAGE_THRESHOLD_30";
                case 10 -> "SINGLE_USAGE_THRESHOLD_10";
                case 0  -> "SINGLE_USAGE_EXHAUSTED";
                default -> throw new IllegalArgumentException();
            };

            case "FAMILY_POOL_REMAINING" -> switch (th) {
                case 50 -> "FAMILY_USAGE_THRESHOLD_50";
                case 30 -> "FAMILY_USAGE_THRESHOLD_30";
                case 10 -> "FAMILY_USAGE_THRESHOLD_10";
                case 0  -> "FAMILY_USAGE_EXHAUSTED";
                default -> throw new IllegalArgumentException();
            };

            case "GIFT_REMAINING" -> switch (th) {
                case 50 -> "PRESENT_USAGE_THRESHOLD_50";
                case 30 -> "PRESENT_USAGE_THRESHOLD_30";
                case 10 -> "PRESENT_USAGE_THRESHOLD_10";
                case 0  -> "PRESENT_USAGE_EXHAUSTED";
                default -> throw new IllegalArgumentException();
            };

            default -> throw new IllegalArgumentException();
        };
    }

    public String resolveMessage(UsageAlertEvent ev) {
        int th = Integer.parseInt(ev.threshold());

        return switch (th) {
            case 50 -> "데이터 잔여량이 50% 남았습니다.";
            case 30 -> "데이터 잔여량이 30% 남았습니다.";
            case 10 -> "데이터 잔여량이 10% 남았습니다.";
            case 0  -> "데이터 잔여량이 모두 소진되었습니다.";
            default -> "";
        };
    }
}
