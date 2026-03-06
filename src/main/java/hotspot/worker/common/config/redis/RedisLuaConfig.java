package hotspot.worker.common.config.redis;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Redis Lua 스크립트 빈 설정
 */
@Configuration
public class RedisLuaConfig {

    /* ================= 데이터 사용량 스크립트 ================= */

    @Bean
    public DefaultRedisScript<List> usageValidBatchScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/usage_valid.lua"));
        script.setResultType(List.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<List> usageAtomicScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/usage_atomic.lua"));
        script.setResultType(List.class);
        return script;
    }

    /* ================= Family 정합성 스크립트 ================= */

    @Bean
    public DefaultRedisScript<Long> familyPolicyChangedScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/family_policy_changed.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> familyPolicyDeletedScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/family_policy_deleted.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> familyPolicyDeactivateScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/family_policy_deactivate.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> familyCreateScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/family_create.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> familyMemberScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/family_member.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> familyModeScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/family_mode.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> familySubLimitScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/family_sub_limit.lua"));
        script.setResultType(Long.class);
        return script;
    }

    /* ================= Subscription 정합성 스크립트 ================= */

    @Bean
    public DefaultRedisScript<Long> subscriptionLockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/subscription_lock.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> subscriptionPlanChangedScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/subscription_plan_changed.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> subscriptionPolicySnapshotScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/subscription_policy_snapshot.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> subscriptionGiftScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/subscription_gift.lua"));

        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> subscriptionAppScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(loadLua("lua/subscription_app.lua"));
        script.setResultType(Long.class);
        return script;
    }

    private String loadLua(String path) {
        try {
            ClassPathResource resource = new ClassPathResource(path);
            String script = new String(
                    resource.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8
            );

            if (script.startsWith("\uFEFF")) {
                script = script.substring(1);
            }

            return script;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load lua script: " + path, e);
        }
    }
}
