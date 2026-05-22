package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Project-scoped rule. Fires on Kafka Connect WORKER configuration files
 * that declare {@code rest.advertised.host.name} as a loopback hostname —
 * one of {@code localhost} / {@code 127.0.0.1} / {@code ::1} / the full
 * IPv6 loopback expansion {@code 0:0:0:0:0:0:0:1} (case-insensitive,
 * whitespace-trimmed).
 *
 * <p>The {@code rest.advertised.host.name} config is the hostname the worker
 * publishes to the cluster's {@code connect-configs} topic as the address
 * at which OTHER workers can reach it for REST-API forwarding. When every
 * worker advertises a loopback address, cross-worker forwarding becomes
 * self-forwarding (the forwarder resolves the target against ITS OWN
 * loopback interface).
 *
 * <p>Does NOT fire on absent key (the worker falls back to
 * {@code rest.host.name} / {@code InetAddress.getLocalHost()} — different
 * failure modes captured by future sibling rules).
 *
 * <p>Does NOT fire on non-loopback hostnames (any other hostname is presumed
 * correct; misconfigurations like 'wrong DNS name' are out of scope for
 * static analysis).
 */
public final class ConnectRestAdvertisedHostNameLocalhostRule implements ProjectScopedRule {

    private static final String KEY = "rest.advertised.host.name";

    private static final Set<String> LOOPBACK_VALUES = Set.of(
            "localhost",
            "127.0.0.1",
            "::1",
            "0:0:0:0:0:0:0:1"
    );

    private final Severity severity;

    public ConnectRestAdvertisedHostNameLocalhostRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_REST_ADVERTISED_HOST_NAME_LOCALHOST;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String raw = p.getProperty(KEY);
            if (raw == null) continue;
            String trimmed = raw.trim();
            if (trimmed.isEmpty()) continue;
            String normalized = trimmed.toLowerCase(Locale.ROOT);
            if (!LOOPBACK_VALUES.contains(normalized)) continue;
            out.add(new Violation(
                    RuleId.CONNECT_REST_ADVERTISED_HOST_NAME_LOCALHOST, severity,
                    ctx.relativize(e.getKey()), "key:" + KEY, 0,
                    "Kafka Connect worker config declares " + KEY + "=" + trimmed
                            + " — a LOOPBACK address. This is the hostname the worker "
                            + "advertises to OTHER workers in the cluster (via the "
                            + "`connect-configs` topic) as the address for inter-worker "
                            + "REST-API forwarding. When every worker advertises a "
                            + "loopback address, cross-worker forwarding becomes "
                            + "self-forwarding: the forwarding worker resolves the "
                            + "target hostname against ITS OWN loopback interface, "
                            + "either contacting its own REST endpoint (returning "
                            + "wrong-cluster state) or getting connection-refused. "
                            + "Connect's distributed-mode coordination — REST forwarding "
                            + "for client API calls AND the herder's config-propagation "
                            + "protocol — depends on every worker advertising an "
                            + "EXTERNALLY-reachable hostname. The misconfiguration is "
                            + "invisible in single-worker dev/staging tests (no "
                            + "forwarding ever happens) and only manifests at "
                            + "multi-worker production scale, producing random 404, "
                            + "stale-data, and connection-refused responses on REST "
                            + "calls and stalling the herder's rebalance protocol. "
                            + "Fix: set " + KEY + "=<worker-resolvable-hostname> — in "
                            + "Kubernetes, use a downward-API env var ($(POD_NAME) or "
                            + "$(POD_IP)); in Docker, use the container hostname; on "
                            + "bare-metal, use the worker's FQDN. The advertised "
                            + "hostname must be reachable from every other worker in "
                            + "the cluster."));
        }
        return out;
    }
}
