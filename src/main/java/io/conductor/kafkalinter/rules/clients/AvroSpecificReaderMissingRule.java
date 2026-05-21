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
 * Flags property files where {@code KafkaAvroDeserializer} is configured without
 * the matching {@code specific.avro.reader=true} toggle. The deserializer returns
 * {@code GenericRecord} by default; the consumer's first {@code (MyEvent) record.value()}
 * cast then throws {@code ClassCastException} on the first poll in production.
 */
public final class AvroSpecificReaderMissingRule implements ProjectScopedRule {

    private static final String AVRO_DESERIALIZER_FQCN = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    private static final String PLAIN_VALUE_DESERIALIZER_KEY = "value.deserializer";
    private static final String PLAIN_KEY_DESERIALIZER_KEY = "key.deserializer";
    private static final String PLAIN_SPECIFIC_AVRO_READER_KEY = "specific.avro.reader";

    private static final String SPRING_VALUE_DESERIALIZER_KEY = "spring.kafka.consumer.value-deserializer";
    private static final String SPRING_KEY_DESERIALIZER_KEY = "spring.kafka.consumer.key-deserializer";
    private static final String SPRING_SPECIFIC_AVRO_READER_KEY = "spring.kafka.properties.specific.avro.reader";

    private final Severity severity;

    public AvroSpecificReaderMissingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.AVRO_SPECIFIC_READER_MISSING;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            checkPair(out, ctx, e.getKey(), p,
                    PLAIN_VALUE_DESERIALIZER_KEY, PLAIN_KEY_DESERIALIZER_KEY,
                    PLAIN_SPECIFIC_AVRO_READER_KEY);
            checkPair(out, ctx, e.getKey(), p,
                    SPRING_VALUE_DESERIALIZER_KEY, SPRING_KEY_DESERIALIZER_KEY,
                    SPRING_SPECIFIC_AVRO_READER_KEY);
        }
        return out;
    }

    private void checkPair(List<Violation> out, ProjectContext ctx, Path file, Properties p,
                           String valueKey, String keyKey, String specificKey) {
        String valueDeser = trim(p.getProperty(valueKey));
        String keyDeser = trim(p.getProperty(keyKey));
        String matchedKey = AVRO_DESERIALIZER_FQCN.equals(valueDeser) ? valueKey
                : AVRO_DESERIALIZER_FQCN.equals(keyDeser) ? keyKey : null;
        if (matchedKey == null) return;

        String specific = trim(p.getProperty(specificKey));
        if (specific != null) return;

        out.add(new Violation(
                RuleId.AVRO_SPECIFIC_READER_MISSING, severity,
                ctx.relativize(file), "key:" + matchedKey, 0,
                matchedKey + "=" + AVRO_DESERIALIZER_FQCN
                        + " but " + specificKey + " is unset — the deserializer returns GenericRecord, "
                        + "and the first `(MyEvent) record.value()` cast throws ClassCastException at runtime. "
                        + "Add " + specificKey + "=true."));
    }

    private static String trim(String s) {
        return s == null ? null : s.trim();
    }
}
