package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when the project contains BOTH:
 * <ul>
 *   <li>a producer-config file that sets {@code transactional.id} (or Spring's
 *       {@code spring.kafka.producer.transaction-id-prefix}); AND</li>
 *   <li>a consumer-config file (has {@code group.id} / Spring's
 *       {@code spring.kafka.consumer.group-id}) that does NOT explicitly set
 *       {@code isolation.level=read_committed}.</li>
 * </ul>
 * Emits one violation per offending consumer file and names the producer file
 * that established the EOS expectation.
 */
public final class ConsumerIsolationReadUncommittedWithTxnRule implements ProjectScopedRule {

    private static final String PLAIN_TXN_ID = "transactional.id";
    private static final String SPRING_TXN_ID_PREFIX = "spring.kafka.producer.transaction-id-prefix";

    private static final String PLAIN_GROUP_ID = "group.id";
    private static final String SPRING_GROUP_ID = "spring.kafka.consumer.group-id";

    private static final String PLAIN_ISOLATION_LEVEL = "isolation.level";
    private static final String SPRING_ISOLATION_LEVEL = "spring.kafka.consumer.isolation-level";
    private static final String SPRING_PROPS_ISOLATION_LEVEL = "spring.kafka.properties.isolation.level";

    private static final String READ_COMMITTED = "read_committed";

    private final Severity severity;

    public ConsumerIsolationReadUncommittedWithTxnRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        Path txnProducerFile = findTransactionalProducer(ctx);
        if (txnProducerFile == null) return out;

        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isConsumerConfig(p)) continue;
            if (hasReadCommitted(p)) continue;

            out.add(new Violation(
                    RuleId.CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN, severity,
                    ctx.relativize(e.getKey()), "key:" + PLAIN_ISOLATION_LEVEL, 0,
                    "consumer config has group.id but no isolation.level=read_committed, while "
                            + ctx.relativize(txnProducerFile)
                            + " sets a transactional producer — the consumer will see aborted-transaction "
                            + "records and pre-commit drafts as if they were committed. "
                            + "Add isolation.level=read_committed."));
        }
        return out;
    }

    private static Path findTransactionalProducer(ProjectContext ctx) {
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (notBlank(p.getProperty(PLAIN_TXN_ID))) return e.getKey();
            if (notBlank(p.getProperty(SPRING_TXN_ID_PREFIX))) return e.getKey();
        }
        return null;
    }

    private static boolean isConsumerConfig(Properties p) {
        return notBlank(p.getProperty(PLAIN_GROUP_ID))
                || notBlank(p.getProperty(SPRING_GROUP_ID));
    }

    private static boolean hasReadCommitted(Properties p) {
        return isReadCommitted(p.getProperty(PLAIN_ISOLATION_LEVEL))
                || isReadCommitted(p.getProperty(SPRING_ISOLATION_LEVEL))
                || isReadCommitted(p.getProperty(SPRING_PROPS_ISOLATION_LEVEL));
    }

    private static boolean isReadCommitted(String v) {
        return v != null && READ_COMMITTED.equalsIgnoreCase(v.trim());
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
