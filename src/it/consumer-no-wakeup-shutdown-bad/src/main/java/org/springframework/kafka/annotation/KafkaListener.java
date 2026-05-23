package org.springframework.kafka.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Locally-declared stand-in for {@code org.springframework.kafka.annotation.KafkaListener}.
 *
 * <p>The kafka-linter's CONSUMER_NO_WAKEUP_SHUTDOWN rule checks for the descriptor
 * {@code Lorg/springframework/kafka/annotation/KafkaListener;}. The bytecode descriptor
 * is determined by the annotation's fully-qualified name only — declaring this annotation
 * type at the same FQN makes it bytecode-indistinguishable from Spring's real one for
 * the rule's purposes, without dragging in the spring-kafka dependency.
 *
 * <p>Only the {@code topics} attribute is included because that is the only one this
 * fixture references.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaListener {
    String topics();
}
