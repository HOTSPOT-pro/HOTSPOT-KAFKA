package hotspot.worker.common.config.kafka.consumer;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import hotspot.worker.consumer.usage.schema.UsageEvent;

@Configuration
public class KafkaConsumerConfig {

    // usage 이벤트 역직렬화를 포함한 ConsumerFactory를 생성한다.
    @Bean
    public ConsumerFactory<String, UsageEvent> usageConsumerFactory(
            KafkaProperties props,
            SslBundles sslBundles
    ) {
        return KafkaConsumerFactorySupport.createConsumerFactory(
                props,
                sslBundles,
                UsageEvent.class
        );
    }

    // 수동 ACK/동시성/재시도 백오프를 적용한 usage 리스너 팩토리를 구성한다.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UsageEvent>
    usageKafkaListenerContainerFactory(
            ConsumerFactory<String, UsageEvent> usageConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, UsageEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(usageConsumerFactory);
        factory.setConcurrency(20);
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setAsyncAcks(true);

        ExponentialBackOff backOff =
                new ExponentialBackOff(500L, 2.0);

        backOff.setMaxInterval(10_000L);
        backOff.setMaxElapsedTime(60_000L);

        factory.setCommonErrorHandler(new DefaultErrorHandler(backOff));

        return factory;
    }

}
