package hotspot.worker.writer.log.support;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hotspot.worker.writer.log.dto.UsageAppliedEnvelope;
import hotspot.worker.writer.log.dto.UsageAppliedFailedEvent;

@Component
public class UsageAppliedFailureFactory {

    private static final int MAX_EXCEPTION_MESSAGE_LENGTH = 1000;

    private final ObjectMapper objectMapper;

    public UsageAppliedFailureFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public UsageAppliedFailedEvent create(UsageAppliedEnvelope envelope, Exception cause) {
        return new UsageAppliedFailedEvent(
                envelope.event().eventId(),
                serializeEnvelope(envelope),
                cause.getClass().getName(),
                truncate(cause.getMessage(), MAX_EXCEPTION_MESSAGE_LENGTH),
                LocalDateTime.now()
        );
    }

    private String serializeEnvelope(UsageAppliedEnvelope envelope) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("event", envelope.event());
        payload.put("result", envelope.result());
        payload.put("outboxEvents", envelope.outboxEvents());
        payload.put("usageAppliedLog", envelope.usageAppliedLog());
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize failed envelope payload", e);
        }
    }

    private String truncate(String source, int maxLength) {
        if (source == null || source.length() <= maxLength) {
            return source;
        }
        return source.substring(0, maxLength);
    }
}
