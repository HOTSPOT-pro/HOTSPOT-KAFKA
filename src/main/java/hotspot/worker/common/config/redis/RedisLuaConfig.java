package hotspot.worker.common.config.redis;

import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisLuaConfig {

    // 정책 검증 Lua Script
    @Bean
    public DefaultRedisScript<List> policyBatchScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/usage_valid.lua"));
        script.setResultType(List.class);
        return script;
    }

}
