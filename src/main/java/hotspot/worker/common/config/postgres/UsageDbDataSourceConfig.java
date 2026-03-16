package hotspot.worker.common.config.postgres;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import com.zaxxer.hikari.HikariDataSource;

@Configuration
public class UsageDbDataSourceConfig {

    // 기본 DB 연결 설정 프로퍼티를 바인딩한다.
    @Bean
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    // 기본 DB DataSource를 생성한다.
    @Bean
    @Primary
    public DataSource dataSource(
            @Qualifier("dataSourceProperties") DataSourceProperties properties
    ) {
        return properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
    }

    // 사용량 적재 전용 DB 연결 설정 프로퍼티를 바인딩한다.
    @Bean(name = "usageDbDataSourceProperties")
    @ConfigurationProperties(prefix = "app.usage-db.datasource")
    public DataSourceProperties usageDbDataSourceProperties() {
        return new DataSourceProperties();
    }

    // 사용량 적재 전용 DataSource를 생성한다.
    @Bean(name = "usageDbDataSource")
    public DataSource usageDbDataSource(
            @Qualifier("usageDbDataSourceProperties") DataSourceProperties properties
    ) {
        return properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
    }

    // 사용량 적재 전용 JdbcTemplate을 생성한다.
    @Bean(name = "usageDbJdbcTemplate")
    public JdbcTemplate usageDbJdbcTemplate(
            @Qualifier("usageDbDataSource") DataSource usageDbDataSource
    ) {
        return new JdbcTemplate(usageDbDataSource);
    }
}
