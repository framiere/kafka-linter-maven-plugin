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
 * Three sub-cases on each properties file:
 * <ol>
 *   <li>{@code buffer.memory < batch.size} — a single batch cannot fit in the buffer.</li>
 *   <li>{@code buffer.memory < 1 MiB} — sustained back-pressure under any traffic spike.</li>
 *   <li>{@code buffer.memory > 1 GiB} — JVM heap pressure and large-OOM loss.</li>
 * </ol>
 * Plain Kafka keys, Spring DSL keys, and Spring passthrough keys are all considered.
 * Spring {@code DataSize} suffixes ({@code 32MB}, etc.) are not parsed and silently skipped.
 */
public final class ProducerBufferMemoryMisconfigRule implements ProjectScopedRule {

    private static final long ONE_MIB = 1024L * 1024L;
    private static final long ONE_GIB = 1024L * 1024L * 1024L;

    private static final String[] BUFFER_KEYS = {
            "buffer.memory",
            "spring.kafka.producer.buffer-memory",
            "spring.kafka.producer.properties.buffer.memory",
    };
    private static final String[] BATCH_KEYS = {
            "batch.size",
            "spring.kafka.producer.batch-size",
            "spring.kafka.producer.properties.batch.size",
    };

    private final Severity severity;

    public ProducerBufferMemoryMisconfigRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_BUFFER_MEMORY_MISCONFIG;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            checkFile(out, ctx, e.getKey(), e.getValue());
        }
        return out;
    }

    private void checkFile(List<Violation> out, ProjectContext ctx, Path file, Properties p) {
        FoundLong buffer = findFirst(p, BUFFER_KEYS);
        if (buffer == null) return;
        FoundLong batch = findFirst(p, BATCH_KEYS);

        if (batch != null && buffer.value < batch.value) {
            emit(out, ctx, file, buffer.matchedKey,
                    "buffer.memory=" + buffer.value + " is below batch.size=" + batch.value
                            + " — a single batch cannot fit in the buffer; every send() hits the back-pressure path "
                            + "and times out under any sustained traffic. Raise buffer.memory above batch.size × "
                            + "(partitions actively written).");
            return;
        }
        if (buffer.value < ONE_MIB) {
            emit(out, ctx, file, buffer.matchedKey,
                    "buffer.memory=" + buffer.value + " is absurdly small (< 1 MiB) — the producer enters "
                            + "sustained back-pressure on any traffic spike and times out send() with "
                            + "Expiring N records. Default 32 MiB.");
            return;
        }
        if (buffer.value > ONE_GIB) {
            emit(out, ctx, file, buffer.matchedKey,
                    "buffer.memory=" + buffer.value + " is absurdly large (> 1 GiB) — pins JVM heap that should "
                            + "be available for processing, lengthens GC pauses, and turns a producer OOM into a "
                            + "1+ GiB lost-write event. Default 32 MiB.");
        }
    }

    private void emit(List<Violation> out, ProjectContext ctx, Path file, String matchedKey, String msg) {
        out.add(new Violation(
                RuleId.PRODUCER_BUFFER_MEMORY_MISCONFIG, severity,
                ctx.relativize(file), "key:" + matchedKey, 0, msg));
    }

    private static FoundLong findFirst(Properties p, String[] keys) {
        for (String k : keys) {
            String raw = p.getProperty(k);
            if (raw == null) continue;
            String trimmed = raw.trim();
            if (trimmed.isEmpty()) continue;
            try {
                return new FoundLong(k, Long.parseLong(trimmed));
            } catch (NumberFormatException ignored) {
                // Skip Spring DataSize suffixes (e.g. "32MB") — out of scope for now.
            }
        }
        return null;
    }

    private record FoundLong(String matchedKey, long value) {}
}
