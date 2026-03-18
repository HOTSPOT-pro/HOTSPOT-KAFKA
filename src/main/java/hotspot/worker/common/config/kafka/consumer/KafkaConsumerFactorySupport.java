package hotspot.worker.common.config.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class KafkaConsumerFactorySupport {

    public static <T> ConsumerFactory<String, T> createConsumerFactory(
            KafkaProperties props,
            SslBundles sslBundles,
            Class<T> clazz
    ) {
        Map<String, Object> cfg =
                new HashMap<>(props.buildConsumerProperties(sslBundles));

        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);

        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        cfg.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600_000);
        cfg.putIfAbsent(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 65536);
        cfg.putIfAbsent(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 300);

        JsonDeserializer<T> valueDeserializer =
                new JsonDeserializer<>(clazz);

        valueDeserializer.addTrustedPackages("*");
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                cfg,
                new StringDeserializer(),
                valueDeserializer
        );
    }
}
