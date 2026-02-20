package hotspot.worker.common.config.kafka.consumer;

import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class OutboxAsyncConfig {

    // Outbox Kafka 콜백(ACK/실패처리) 전용 실행기
    @Bean(name = "outboxAlertCallbackExecutor")
    public Executor outboxAlertCallbackExecutor(
            @Value("${app.outbox.usage-alerts.callback-executor.pool-size:4}") int poolSize,
            @Value("${app.outbox.usage-alerts.callback-executor.queue-capacity:1000}") int queueCapacity
    ) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("outbox-alert-callback-");
        executor.initialize();
        return executor;
    }
}
