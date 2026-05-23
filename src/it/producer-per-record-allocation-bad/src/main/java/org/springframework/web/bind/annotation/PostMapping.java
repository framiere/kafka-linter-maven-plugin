package org.springframework.web.bind.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Stub of Spring MVC's {@code @PostMapping} declared at its real FQN.
 * The rule checks for the annotation descriptor
 * {@code Lorg/springframework/web/bind/annotation/PostMapping;} verbatim,
 * so the stub MUST live at that exact package — declaring it locally
 * (e.g. as {@code @sample.PostMapping}) would produce a different
 * bytecode descriptor and the rule would not engage.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PostMapping {
    String[] value() default {};
}
