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
 * does NOT set {@code commit.interval.ms}. The Streams default depends
 * on {@code processing.guarantee}: 30000 ms (30 s) for at_least_once,
 * 100 ms for exactly_once_v2. Both defaults are workload-dependent;
 * at-least-once 30 s is too long for high-throughput topologies (large
 * replay storms on rebalance); EOS 100 ms is too aggressive for
 * production deployments (transaction-coordinator pressure). INFO
 * severity — the operator should make the choice explicit.
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesCommitIntervalMsAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String COMMIT_INTERVAL_MS = "commit.interval.ms";

    private final Severity severity;

    public StreamsPropertiesCommitIntervalMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_COMMIT_INTERVAL_MS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(COMMIT_INTERVAL_MS))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_COMMIT_INTERVAL_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + COMMIT_INTERVAL_MS, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + COMMIT_INTERVAL_MS + "`. StreamsConfig declares "
                            + "TWO defaults: `30000` ms (30 s) for "
                            + "`at_least_once`, `100` ms for "
                            + "`exactly_once_v2`. Both are workload-dependent "
                            + "and rarely match production needs. At-least-"
                            + "once 30 s = up to 30 s of records re-processed "
                            + "on every rebalance, deploy, or thread crash — "
                            + "for a high-throughput topology (5k rec/s/"
                            + "partition × 16 partitions), that's ~2.4 M "
                            + "records replayed on each restart. EOS 100 ms = "
                            + "10 transactions/sec/thread of broker-side "
                            + "transaction-coordinator pressure + "
                            + "`__transaction_state` writes + commit markers "
                            + "to every output partition; a 4-thread Streams "
                            + "app commits 40 transactions/sec, saturating the "
                            + "coordinator at modest cluster sizes. EOS commit "
                            + "interval also bounds end-to-end latency for "
                            + "downstream `isolation.level=read_committed` "
                            + "consumers (they wait for the next commit marker "
                            + "before seeing records). Fix: for at-least-once "
                            + "high-throughput, set `" + COMMIT_INTERVAL_MS
                            + "=5000` (5 s — cuts replay window 6x); for "
                            + "exactly_once_v2, set `" + COMMIT_INTERVAL_MS
                            + "=1000` (1 s — cuts coordinator pressure 10x, "
                            + "keeps end-to-end latency under 1 s). For "
                            + "workloads where the default is right, set the "
                            + "value EXPLICITLY to document the deliberate "
                            + "choice and insulate the app from future "
                            + "Streams version changes that might shift the "
                            + "default."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
