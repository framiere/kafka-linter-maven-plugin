package io.conductor.kafkalinter.rules.spring;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Fires when at least one class in the project uses {@code @RetryableTopic}
 * but no class in the project exposes a {@code @Bean} method returning
 * {@code KafkaTemplate}. The retry/DLT publication path requires a
 * {@code KafkaTemplate} bean — Spring Boot's auto-config provides one in some
 * setups but not all, and relying on auto-config when {@code @RetryableTopic}
 * is in use is fragile across boot versions.
 */
public final class SpringRetryableTopicNoKafkaTemplateRule implements ProjectScopedRule {

    private static final String RETRYABLE_TOPIC_DESC = "Lorg/springframework/kafka/annotation/RetryableTopic;";
    private static final String BEAN_DESC = "Lorg/springframework/context/annotation/Bean;";
    private static final String KAFKA_TEMPLATE_INTERNAL = "org/springframework/kafka/core/KafkaTemplate";

    private final Severity severity;

    public SpringRetryableTopicNoKafkaTemplateRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Path classesDir = ctx.classesDir();
        if (classesDir == null || !Files.isDirectory(classesDir)) return List.of();

        List<RetryableTopicSite> sites = new ArrayList<>();
        boolean hasKafkaTemplateBean = false;

        try (Stream<Path> walk = Files.walk(classesDir)) {
            for (Path cf : walk.filter(p -> p.toString().endsWith(".class")).filter(Files::isRegularFile).toList()) {
                ClassNode cn = readClass(cf);
                if (cn == null) continue;
                for (MethodNode mn : cn.methods) {
                    if (hasAnnotation(mn, RETRYABLE_TOPIC_DESC)) {
                        sites.add(new RetryableTopicSite(cn.name, mn.name));
                    }
                    if (hasAnnotation(mn, BEAN_DESC) && returnsKafkaTemplate(mn)) {
                        hasKafkaTemplateBean = true;
                    }
                }
            }
        } catch (IOException e) {
            return List.of();
        }

        if (sites.isEmpty() || hasKafkaTemplateBean) return List.of();

        List<Violation> out = new ArrayList<>();
        for (RetryableTopicSite s : sites) {
            out.add(new Violation(
                    RuleId.SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE, severity,
                    s.className, s.methodName, 0,
                    "@RetryableTopic on " + s.className.substring(s.className.lastIndexOf('/') + 1)
                            + "#" + s.methodName + " but no @Bean KafkaTemplate is exposed anywhere in the project — "
                            + "the retry/DLT publication path needs an explicit KafkaTemplate bean. "
                            + "Expose `@Bean public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> pf) { return new KafkaTemplate<>(pf); }` "
                            + "in a @Configuration class — Spring Boot auto-config provides one in some setups but not all, "
                            + "and relying on auto-config when @RetryableTopic is in use is fragile across boot versions."));
        }
        return out;
    }

    private static ClassNode readClass(Path cf) {
        try (InputStream in = Files.newInputStream(cf)) {
            ClassReader cr = new ClassReader(in);
            ClassNode cn = new ClassNode();
            cr.accept(cn, ClassReader.SKIP_FRAMES | ClassReader.SKIP_DEBUG);
            return cn;
        } catch (IOException e) {
            return null;
        }
    }

    private static boolean hasAnnotation(MethodNode mn, String descriptor) {
        return hasAnnotationIn(mn.visibleAnnotations, descriptor)
                || hasAnnotationIn(mn.invisibleAnnotations, descriptor);
    }

    private static boolean hasAnnotationIn(List<AnnotationNode> annots, String descriptor) {
        if (annots == null) return false;
        for (AnnotationNode a : annots) {
            if (descriptor.equals(a.desc)) return true;
        }
        return false;
    }

    private static boolean returnsKafkaTemplate(MethodNode mn) {
        Type ret = Type.getReturnType(mn.desc);
        return ret.getSort() == Type.OBJECT && KAFKA_TEMPLATE_INTERNAL.equals(ret.getInternalName());
    }

    private record RetryableTopicSite(String className, String methodName) {}
}
