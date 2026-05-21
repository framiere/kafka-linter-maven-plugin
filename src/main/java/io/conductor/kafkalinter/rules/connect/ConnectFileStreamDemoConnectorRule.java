package io.conductor.kafkalinter.rules.connect;

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
import java.util.Set;

/**
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * declares {@code connector.class=org.apache.kafka.connect.file.FileStreamSourceConnector}
 * or {@code FileStreamSinkConnector}.
 *
 * <p>These are demo connectors shipped with Apache Kafka for the Connect
 * quickstart tutorial. Apache Kafka documentation explicitly states they
 * are "intended primarily for demonstration purposes". They have no file
 * rotation handling, no atomic-write detection, no error handling, no
 * multi-file support, and no resilience guarantees.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectFileStreamDemoConnectorRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";

    private static final Set<String> DEMO_CLASSES = Set.of(
            "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "org.apache.kafka.connect.file.FileStreamSinkConnector");

    private final Severity severity;

    public ConnectFileStreamDemoConnectorRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_FILE_STREAM_DEMO_CONNECTOR;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            String connectorClass = trimOrNull(e.getValue().getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!DEMO_CLASSES.contains(connectorClass)) continue;

            String role = connectorClass.endsWith("SourceConnector") ? "source" : "sink";
            out.add(new Violation(
                    RuleId.CONNECT_FILE_STREAM_DEMO_CONNECTOR, severity,
                    ctx.relativize(e.getKey()), "key:" + CONNECTOR_CLASS, 0,
                    "Connect " + role + " connector uses " + connectorClass
                            + " — this is a DEMO connector shipped with Apache Kafka for "
                            + "the Connect quickstart tutorial, not for production use. It "
                            + "has no file-rotation handling (logrotate breaks it silently), "
                            + "no atomic-write detection, no multi-file support, no error "
                            + "tolerance, and no clean restart semantics. Apache Kafka docs "
                            + "explicitly mark it as 'intended primarily for demonstration "
                            + "purposes'. Replace with a production-grade connector: "
                            + "kafka-connect-spooldir or kafka-connect-fs for file ingest, "
                            + "kafka-connect-s3/hdfs for file output, or a log shipper "
                            + "(Filebeat, Fluent Bit, Vector) that publishes to Kafka."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
