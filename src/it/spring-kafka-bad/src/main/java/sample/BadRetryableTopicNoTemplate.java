package sample;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;

/**
 * RULE: SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE.
 *
 * @RetryableTopic publishes retry/DLT records via an injected KafkaTemplate.
 * The class uses @RetryableTopic but no @Bean method anywhere in the project
 * returns KafkaTemplate — Spring Boot auto-config may or may not provide one
 * depending on the boot version + active profile, so retries can silently
 * fall through to the DLT or fail at startup.
 */
public final class BadRetryableTopicNoTemplate {

    @RetryableTopic(attempts = "3")
    @KafkaListener(topics = "orders")
    public void onMessage(String body) {
        if (body == null) throw new IllegalStateException("null body");
        System.out.println(body);
    }
}
