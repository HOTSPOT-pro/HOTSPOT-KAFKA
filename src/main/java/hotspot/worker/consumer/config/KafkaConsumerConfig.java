package hotspot.worker.consumer.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import hotspot.worker.consumer.schema.UsageAlertEvent;
import hotspot.worker.consumer.schema.UsageEvent;

/**
 * Consumer 전용 Kafka 설정 클래스
 */
@Configuration
public class KafkaConsumerConfig {

    // 수동 ACK와 재시도 정책을 적용한 리스너 컨테이너 팩토리
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UsageEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, UsageEvent> consumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, UsageEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(10_000L);
        backOff.setMaxElapsedTime(60_000L);
        factory.setCommonErrorHandler(new DefaultErrorHandler(backOff));
        return factory;
    }

    // UsageEvent 역직렬화를 위한 ConsumerFactory
    @Bean
    public ConsumerFactory<String, UsageEvent> consumerFactory(
            KafkaProperties props,
            SslBundles sslBundles
    ) {
        Map<String, Object> cfg = new HashMap<>(props.buildConsumerProperties(sslBundles));
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JsonDeserializer<UsageEvent> valueDeserializer = new JsonDeserializer<>(UsageEvent.class);
        valueDeserializer.addTrustedPackages("*");
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(cfg, new StringDeserializer(), valueDeserializer);
    }

    // 알림 이벤트 발행용 ProducerFactory
    @Bean
    public ProducerFactory<String, UsageAlertEvent> producerFactory(
            KafkaProperties props,
            SslBundles sslBundles
    ) {
        Map<String, Object> cfg = new HashMap<>(props.buildProducerProperties(sslBundles));
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        cfg.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(cfg);
    }

    // 알림 토픽 발행용 KafkaTemplate
    @Bean
    public KafkaTemplate<String, UsageAlertEvent> kafkaTemplate(
            ProducerFactory<String, UsageAlertEvent> pf
    ) {
        return new KafkaTemplate<>(pf);
    }
}
