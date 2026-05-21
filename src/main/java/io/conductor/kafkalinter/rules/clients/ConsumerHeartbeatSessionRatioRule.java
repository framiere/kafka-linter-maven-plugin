package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Flags consumer configs where {@code heartbeat.interval.ms} is not below
 * 1/3 of {@code session.timeout.ms}. Suppressed when {@code group.protocol=consumer}
 * (KIP-848) is set — the broker controls heartbeat cadence in that mode.
 */
public final class ConsumerHeartbeatSessionRatioRule implements ProjectScopedRule {

    private static final String SPRING_HEARTBEAT_KEY = "spring.kafka.consumer.properties.heartbeat.interval.ms";
    private static final String SPRING_SESSION_KEY = "spring.kafka.consumer.properties.session.timeout.ms";
    private static final String SPRING_GROUP_PROTOCOL_KEY = "spring.kafka.consumer.properties.group.protocol";

    private final Severity severity;

    public ConsumerHeartbeatSessionRatioRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_HEARTBEAT_SESSION_RATIO;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            checkPair(out, ctx, e.getKey(), p,
                    KafkaTypes.HEARTBEAT_INTERVAL_MS_KEY,
                    KafkaTypes.SESSION_TIMEOUT_MS_KEY,
                    KafkaTypes.GROUP_PROTOCOL_KEY);
            checkPair(out, ctx, e.getKey(), p,
                    SPRING_HEARTBEAT_KEY, SPRING_SESSION_KEY, SPRING_GROUP_PROTOCOL_KEY);
        }
        return out;
    }

    private void checkPair(List<Violation> out, ProjectContext ctx, Path file, Properties p,
                           String heartbeatKey, String sessionKey, String groupProtocolKey) {
        String heartbeatRaw = p.getProperty(heartbeatKey);
        String sessionRaw = p.getProperty(sessionKey);
        if (heartbeatRaw == null || sessionRaw == null) return;
        if ("consumer".equalsIgnoreCase(trim(p.getProperty(groupProtocolKey)))) return;

        Integer heartbeat = parsePositive(heartbeatRaw);
        Integer session = parsePositive(sessionRaw);
        if (heartbeat == null || session == null) return;
        if ((long) heartbeat * 3 < session) return;

        out.add(new Violation(
                RuleId.CONSUMER_HEARTBEAT_SESSION_RATIO, severity,
                ctx.relativize(file), "key:" + heartbeatKey, 0,
                heartbeatKey + "=" + heartbeat + ", " + sessionKey + "=" + session
                        + " — heartbeat is not below 1/3 of session. Under normal jitter a single missed heartbeat evicts the consumer and triggers a rebalance. Raise " + sessionKey
                        + " (typically 30000–45000) or lower " + heartbeatKey + " so that heartbeat * 3 < session."));
    }

    private static String trim(String s) {
        return s == null ? null : s.trim();
    }

    private static Integer parsePositive(String s) {
        try {
            int n = Integer.parseInt(s.trim());
            return n > 0 ? n : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
