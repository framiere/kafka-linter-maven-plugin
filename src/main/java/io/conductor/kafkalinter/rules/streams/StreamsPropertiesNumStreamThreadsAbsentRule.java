package io.conductor.kafkalinter.rules.streams;

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
 * Project-scoped rule. Fires on a Kafka Streams .properties file that
 * does NOT set {@code num.stream.threads}. The Streams default is
 * {@code 1} — silently single-threaded. INFO severity because the
 * default is workload-dependent (right for a single-partition
 * pipeline or 1-vCPU dev pod, silently wasteful for anything with
 * horizontal scaling and multi-vCPU pods).
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesNumStreamThreadsAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String NUM_STREAM_THREADS = "num.stream.threads";

    private final Severity severity;

    public StreamsPropertiesNumStreamThreadsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_NUM_STREAM_THREADS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(NUM_STREAM_THREADS))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_NUM_STREAM_THREADS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + NUM_STREAM_THREADS, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + NUM_STREAM_THREADS + "`. Streams defaults `"
                            + NUM_STREAM_THREADS + "` to `1` — the topology "
                            + "runs on a SINGLE StreamThread. A multi-vCPU "
                            + "pod (resources.requests.cpu: 4) running one "
                            + "StreamThread uses ONE core; the other three "
                            + "sit idle. Operators see 25% pod CPU under "
                            + "load and scale horizontally (4× more pods at "
                            + "25% each) instead of vertically, burning "
                            + "Kubernetes node budget. Any blocking call in "
                            + "a `transform()` (DB lookup, HTTP call) also "
                            + "stalls every partition owned by the one "
                            + "thread. Fix: set `" + NUM_STREAM_THREADS
                            + "` to roughly min(input-partitions / "
                            + "instances, cpu-cores) — typically `4` or "
                            + "`8` on multi-vCPU pods. For a single-"
                            + "partition pipeline or a 1-vCPU dev pod, set "
                            + "`" + NUM_STREAM_THREADS + "=1` EXPLICITLY "
                            + "to document the single-threaded choice."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
