package hotspot.worker.common.config.kafka.producer;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import hotspot.worker.consumer.usage.schema.UsageAlertEvent;
import hotspot.worker.producer.schema.UsageEvent;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, UsageEvent> usageProducerFactory(
            KafkaProperties props,
            SslBundles sslBundles
    ) {
        return KafkaProducerFactorySupport.createProducerFactory(props, sslBundles);
    }

    @Bean
    public KafkaTemplate<String, UsageEvent> usageKafkaTemplate(
            ProducerFactory<String, UsageEvent> pf
    ) {
        return new KafkaTemplate<>(pf);
    }

    @Bean
    public ProducerFactory<String, UsageAlertEvent> alertProducerFactory(
            KafkaProperties props,
            SslBundles sslBundles
    ) {
        return KafkaProducerFactorySupport.createProducerFactory(props, sslBundles);
    }

    @Bean
    public KafkaTemplate<String, UsageAlertEvent> alertKafkaTemplate(
            ProducerFactory<String, UsageAlertEvent> pf
    ) {
        return new KafkaTemplate<>(pf);
    }
}
