package sample;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * RULE: SPRING_JSON_DESERIALIZER_TRUSTED_PACKAGES_WILDCARD.
 *
 * Four call sites that should each fire:
 *  - JsonDeserializer.addTrustedPackages("*")  (single-element varargs)
 *  - JsonDeserializer.addTrustedPackages("com.example", "*")  (multi-element with "*")
 *  - JsonDeserializer.trustedPackages("*")  (fluent setter)
 *  - DefaultJackson2JavaTypeMapper.addTrustedPackages("*")
 *
 * Plus a control call that should NOT fire:
 *  - JsonDeserializer.addTrustedPackages("com.acme.events", "com.acme.shared")
 */
@Configuration
public class BadJsonDeserializerTrustedPackagesWildcard {

    @Bean
    public ConsumerFactory<String, Object> consumerFactoryWildcardAdd() {
        Map<String, Object> props = new HashMap<>();
        JsonDeserializer<Object> deserializer = new JsonDeserializer<>();
        deserializer.addTrustedPackages("*");  // FIRES
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactoryWildcardMixedAdd() {
        Map<String, Object> props = new HashMap<>();
        JsonDeserializer<Object> deserializer = new JsonDeserializer<>();
        deserializer.addTrustedPackages("com.example", "*");  // FIRES (wildcard among non-wildcards)
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactoryFluentWildcard() {
        Map<String, Object> props = new HashMap<>();
        JsonDeserializer<Object> deserializer = new JsonDeserializer<Object>().trustedPackages("*");  // FIRES
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> typeMapperWildcard() {
        DefaultJackson2JavaTypeMapper mapper = new DefaultJackson2JavaTypeMapper();
        mapper.addTrustedPackages("*");  // FIRES
        return new ConcurrentKafkaListenerContainerFactory<>();
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactoryExplicitAllowlist() {
        Map<String, Object> props = new HashMap<>();
        JsonDeserializer<Object> deserializer = new JsonDeserializer<>();
        deserializer.addTrustedPackages("com.acme.events", "com.acme.shared");  // CONTROL — must NOT fire
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }
}
