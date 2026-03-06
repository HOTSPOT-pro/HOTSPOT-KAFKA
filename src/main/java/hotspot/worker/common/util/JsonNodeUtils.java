package hotspot.worker.common.util;

import com.fasterxml.jackson.databind.JsonNode;

public final class JsonNodeUtils {

    private JsonNodeUtils() {}

    public static String requireText(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || !v.isTextual()) {
            throw new IllegalArgumentException("Missing or invalid '" + field + "'");
        }
        return v.asText();
    }

    public static long requireLong(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || !v.isNumber()) {
            throw new IllegalArgumentException("Missing or invalid '" + field + "'");
        }
        return v.asLong();
    }
}
