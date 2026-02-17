package hotspot.worker.common.config.redis;

import java.time.ZoneId;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import hotspot.worker.consumer.usage.support.RedisKeyBuilder;

/**
 * Consumer에서 사용하는 Redis 보조 설정
 */
@Configuration
public class RedisClientConfig {

    // Redis 키 규칙 빌더를 KST 기준으로 생성
    @Bean
    public RedisKeyBuilder redisKeyBuilder() {
        return new RedisKeyBuilder(ZoneId.of("Asia/Seoul"));
    }
}
