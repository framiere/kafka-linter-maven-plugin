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
 * file that does NOT set {@code compression.type}.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key (no framework prefix). The
 * predicate is intentionally narrow:
 *
 * <ul>
 *   <li>It does NOT match Spring Boot's
 *       {@code spring.kafka.producer.properties.key.serializer} — Spring
 *       Boot has its own dedicated rules with framework-aware scoping.</li>
 *   <li>It does NOT match Quarkus / MicroProfile Messaging's
 *       {@code mp.messaging.outgoing.<channel>.key.serializer} — Quarkus
 *       has its own dedicated rules.</li>
 *   <li>It does NOT match Connect connector configs (which use
 *       {@code key.converter}/{@code value.converter} — different key).</li>
 *   <li>It does NOT match Streams configs (which use
 *       {@code default.key.serde}/{@code default.value.serde} — different
 *       key).</li>
 *   <li>It does NOT match consumer-only configs (which use
 *       {@code key.deserializer}/{@code value.deserializer} — different
 *       key).</li>
 * </ul>
 *
 * <p>The rule is the .properties-file counterpart of the bytecode-scope
 * {@code PRODUCER_NO_COMPRESSION} rule: same "compression disabled by
 * default" anti-pattern, different surface.
 */
public final class ProducerPropertiesCompressionTypeAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String COMPRESSION_TYPE = "compression.type";

    private final Severity severity;

    public ProducerPropertiesCompressionTypeAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_COMPRESSION_TYPE_ABSENT;
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
            if (isNonEmpty(p.getProperty(COMPRESSION_TYPE))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_COMPRESSION_TYPE_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + COMPRESSION_TYPE, 0,
                    "kafka-clients producer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + COMPRESSION_TYPE + "`. The "
                            + "kafka-clients ProducerConfig defaults `" + COMPRESSION_TYPE
                            + "` to `none` — every record-batch ships UNCOMPRESSED across "
                            + "producer→broker bandwidth, broker→broker replication, broker "
                            + "disk, and broker→consumer egress. Typical JSON/Avro payloads "
                            + "are 3-5× larger uncompressed than with `zstd` or `lz4`; the "
                            + "cost is paid at every hop across the cluster. The CPU cost "
                            + "of compression lives in the producer's sender thread, OFF "
                            + "the application's hot path, and is recouped many times over "
                            + "on the broker side. Fix: add `" + COMPRESSION_TYPE
                            + "=zstd` (recommended for Kafka 2.1+) or `" + COMPRESSION_TYPE
                            + "=lz4` (the safe choice for older client versions). The only "
                            + "legitimate reason to leave compression off is that the "
                            + "payload is ALREADY compressed (parquet, gzipped protobuf, "
                            + "snappy-framed bytes) — in which case set `" + COMPRESSION_TYPE
                            + "=none` EXPLICITLY with a comment documenting why."));
        }
        return out;
    }

    /**
     * Returns the producer-shape key that triggered the gate, or null
     * if the file is not producer-shaped.
     */
    private static String producerShapeKey(Properties p) {
        if (isNonEmpty(p.getProperty(KEY_SERIALIZER))) return KEY_SERIALIZER;
        if (isNonEmpty(p.getProperty(VALUE_SERIALIZER))) return VALUE_SERIALIZER;
        return null;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
