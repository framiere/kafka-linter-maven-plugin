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
 * does NOT set {@code state.dir}. The Streams default is
 * {@code ${java.io.tmpdir}/kafka-streams} which resolves to
 * {@code /tmp/kafka-streams} on most JVMs — state lives on tmpfs
 * and is wiped on JVM restart. WARNING severity (the sibling
 * STREAMS_STATE_DIR_TMP catches explicit `/tmp/*` values at ERROR).
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesStateDirAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String STATE_DIR = "state.dir";

    private final Severity severity;

    public StreamsPropertiesStateDirAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_STATE_DIR_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(STATE_DIR))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_STATE_DIR_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + STATE_DIR, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + STATE_DIR + "`. Streams defaults `"
                            + STATE_DIR + "` to `${java.io.tmpdir}/kafka-"
                            + "streams` — typically `/tmp/kafka-streams` on "
                            + "Linux. `/tmp` is a tmpfs mount cleared on "
                            + "reboot (systemd-tmpfiles) and the ephemeral "
                            + "writable layer inside containers (lost on "
                            + "container restart). Every JVM restart "
                            + "triggers a FULL state-store rebuild from the "
                            + "changelog topic — seconds for small stores, "
                            + "minutes-to-hours for GB-scale stores. In "
                            + "Kubernetes, EVERY pod restart (rolling "
                            + "upgrade, OOM kill, eviction) replays the "
                            + "entire changelog, turning a 30-second "
                            + "deployment into a multi-minute outage. Fix: "
                            + "set `" + STATE_DIR + "` to a persistent "
                            + "mount: `/var/lib/<app>/streams` on bare-"
                            + "metal, a PVC mount on Kubernetes, an EBS "
                            + "volume on EC2. The cold-start cost of losing "
                            + "the state is the bottleneck, not the few-"
                            + "MB/s of RocksDB writes."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
