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
 * Project-scoped rule. Fires on a plain kafka-clients producer .properties
 * file (identified by top-level {@code key.serializer} or
 * {@code value.serializer}) that does NOT set {@code max.block.ms}.
 *
 * <p>kafka-clients' {@code ProducerConfig.MAX_BLOCK_MS_CONFIG} defaults to
 * {@code 60000} (60 seconds). It is the PRE-ENQUEUE wait budget:
 * {@code send()} blocks for up to {@code max.block.ms} when the record-
 * accumulator buffer is full or topic metadata is missing.
 *
 * <p>Distinct from and ADDITIVE to {@code delivery.timeout.ms}. A record's
 * total max wall-clock time inside the producer is
 * {@code max.block.ms + delivery.timeout.ms} (default total = 180s).
 *
 * <p>60s default is much longer than typical HTTP timeouts; synchronous
 * API paths calling {@code send().get()} silently hang for up to a minute
 * before throwing.
 *
 * <p>INFO severity because the default works for batch-oriented workloads;
 * the rule is a documentation nudge prompting the operator to set
 * max.block.ms paired with the calling code's deadline.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key. Excludes Connect
 * ({@code connector.class}) and Streams ({@code application.id}).
 */
public final class ProducerPropertiesMaxBlockMsAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String MAX_BLOCK_MS = "max.block.ms";

    private final Severity severity;

    public ProducerPropertiesMaxBlockMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_MAX_BLOCK_MS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String shapeKey = producerShapeKey(p);
            if (shapeKey == null) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(MAX_BLOCK_MS))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_MAX_BLOCK_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + MAX_BLOCK_MS, 0,
                    "kafka-clients producer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + MAX_BLOCK_MS + "`. kafka-clients defaults `"
                            + MAX_BLOCK_MS + "` to `60000` (60 seconds). "
                            + "This is the PRE-ENQUEUE wait budget — how "
                            + "long `KafkaProducer.send()`, `partitions"
                            + "For()`, `initTransactions()`, `beginTrans"
                            + "action()`, `commitTransaction()`, `abort"
                            + "Transaction()`, and `sendOffsetsToTrans"
                            + "action()` will BLOCK the calling thread "
                            + "when (a) the record-accumulator buffer "
                            + "(sized by `buffer.memory`, default 32 MB) "
                            + "is FULL, OR (b) topic metadata for the "
                            + "destination topic is not yet known. This "
                            + "is DISTINCT from and ADDITIVE to `delivery."
                            + "timeout.ms` — a record's total max wall-"
                            + "clock time inside the producer is `"
                            + MAX_BLOCK_MS + " + delivery.timeout.ms` "
                            + "(default total = 60s + 120s = 180s = 3 min)."
                            + " The default 60000ms is much LONGER than "
                            + "typical HTTP request timeouts (5-30s), "
                            + "Spring @Async defaults, and downstream "
                            + "service-call deadlines. Specific impacts of "
                            + "the absent key: (a) **synchronous API "
                            + "`send().get()` stalls for 60s before "
                            + "throwing.** REST handler calls `producer."
                            + "send(record).get(5, SECONDS)` — but send() "
                            + "ITSELF blocks for up to 60s before "
                            + "returning the Future; the 5s .get() "
                            + "timeout never fires; HTTP client times out "
                            + "at 30s; worker thread stuck another 30s; "
                            + "thread pool exhausts; service degrades. "
                            + "(b) **back-pressure invisible.** Producer "
                            + "buffer fills during broker hiccup; every "
                            + "send() progressively blocks longer (100ms "
                            + "→ 1s → 10s → 60s); p99 latency spike "
                            + "looks like a service outage. (c) **fresh-"
                            + "topic latency spike.** First send to an "
                            + "auto-created topic blocks up to 60s "
                            + "waiting for metadata propagation. (d) "
                            + "**transactional initTransactions() startup "
                            + "hang.** On coordinator unavailability, "
                            + "initTransactions() hangs 60s; Kubernetes "
                            + "liveness probe (30s) fails the pod; "
                            + "restart-loop pathology. Common bug shapes: "
                            + "(1) REST API + `send().get()` with HTTP "
                            + "deadline shorter than max.block.ms → "
                            + "stuck-thread cascade; (2) partition leader "
                            + "migration fills buffer, 60s p99 spike for "
                            + "what was a 5s leader transition; (3) "
                            + "auto-created topic first-send hangs 60s; "
                            + "(4) initTransactions() startup hang in "
                            + "coordinator-failover scenarios; (5) "
                            + "Resilience4j × max.block.ms × delivery."
                            + "timeout.ms = 540s total budget when "
                            + "operator wanted 5s; (6) Spring Boot multi-"
                            + "producer template, all producers inherit "
                            + "default 60s — silently breaks the "
                            + "synchronous API producer. Fix: ONE LINE. "
                            + "Set `" + MAX_BLOCK_MS + "` to a value "
                            + "BELOW the calling code's deadline. "
                            + "Examples: synchronous API path: `"
                            + MAX_BLOCK_MS + "=2000` (fail send() at 2s "
                            + "so HTTP layer can return 503); fire-and-"
                            + "forget background producer: `" + MAX_BLOCK_MS
                            + "=10000`; bulk batch pipeline willing to "
                            + "wait: `" + MAX_BLOCK_MS + "=600000` (10 "
                            + "min). Sibling rule [[producer-properties-"
                            + "buffer-memory-absent]] catches the "
                            + "coupled-partner: buffer.memory determines "
                            + "how often the buffer fills and triggers "
                            + "the max.block.ms wait."));
        }
        return out;
    }

    private static String producerShapeKey(Properties p) {
        if (isNonEmpty(p.getProperty(KEY_SERIALIZER))) return KEY_SERIALIZER;
        if (isNonEmpty(p.getProperty(VALUE_SERIALIZER))) return VALUE_SERIALIZER;
        return null;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
