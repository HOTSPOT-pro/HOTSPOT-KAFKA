package hotspot.worker.writer.retry;

import java.sql.SQLException;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;

@Component
public class UsageAppliedFailureClassifier {

    public boolean isPermanent(Exception e) {
        if (e instanceof DataIntegrityViolationException) {
            return true;
        }

        Throwable cursor = e;
        while (cursor != null) {
            if (cursor instanceof JsonProcessingException) {
                return true;
            }
            if (cursor instanceof SQLException sql) {
                String state = sql.getSQLState();
                if (state != null && state.length() >= 2) {
                    String clazz = state.substring(0, 2);
                    if ("22".equals(clazz) || "23".equals(clazz) || "42".equals(clazz)) {
                        return true;
                    }
                }
            }
            cursor = cursor.getCause();
        }
        return false;
    }
}
