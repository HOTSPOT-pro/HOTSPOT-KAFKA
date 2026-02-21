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

import hotspot.worker.consumer.usage.schema.UsageAlertEvent;
import hotspot.worker.consumer.usage.schema.UsageEvent;

@Configuration
public class KafkaConsumerConfig {

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

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UsageEvent>
    usageKafkaListenerContainerFactory(
            ConsumerFactory<String, UsageEvent> usageConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, UsageEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(usageConsumerFactory);
        factory.setConcurrency(6);
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL);

        ExponentialBackOff backOff =
                new ExponentialBackOff(500L, 2.0);

        backOff.setMaxInterval(10_000L);
        backOff.setMaxElapsedTime(60_000L);

        factory.setCommonErrorHandler(new DefaultErrorHandler(backOff));

        return factory;
    }

    @Bean
    public ConsumerFactory<String, UsageAlertEvent> alertConsumerFactory(
            KafkaProperties props,
            SslBundles sslBundles
    ) {
        return KafkaConsumerFactorySupport.createConsumerFactory(
                props,
                sslBundles,
                UsageAlertEvent.class
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UsageAlertEvent>
    alertKafkaListenerContainerFactory(
            ConsumerFactory<String, UsageAlertEvent> alertConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, UsageAlertEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(alertConsumerFactory);
        factory.setConcurrency(6);
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }
}
