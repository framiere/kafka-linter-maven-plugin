package sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Control for CONSUMER_GROUP_INSTANCE_ID_RANDOM.
 *
 * <p>Five silent shapes that each AVOID the rule for a DIFFERENT reason:
 *
 * <ol>
 *   <li>{@code stableEnvPodName} — {@code group.instance.id} is the Kubernetes
 *       StatefulSet pod name from the downward API ({@code $POD_NAME}). The next
 *       significant instruction before the put is INVOKEVIRTUAL on
 *       {@code System.getenv}, not {@code UUID.toString()}. Silent.</li>
 *   <li>{@code stableHostName} — bare-metal canonical pattern: hostname via
 *       {@code InetAddress.getLocalHost().getHostName()}. Silent (different
 *       value source).</li>
 *   <li>{@code stableLiteral} — explicit literal {@code "consumer-0"} written
 *       directly. Silent (no {@code UUID.randomUUID} on the value side).</li>
 *   <li>{@code randomUuidForClientIdNotInstanceId} — {@code UUID.randomUUID()
 *       .toString()} is used but for {@code client.id}, NOT
 *       {@code group.instance.id}. The rule's key check rejects it. Silent.</li>
 *   <li>{@code groupInstanceIdAbsent} — {@code group.instance.id} is NEVER set;
 *       the consumer is a dynamic-membership member. No put on the key, no
 *       value-side check happens. Silent.</li>
 * </ol>
 */
public final class GoodConsumerGroupInstanceIdStable {

    /** Canonical k8s pattern: downward-API POD_NAME. Stable across restarts of the same pod. */
    public Properties stableEnvPodName() {
        Properties props = new Properties();
        String podName = System.getenv("POD_NAME"); // silent: value is not UUID.randomUUID()
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, podName);
        return props;
    }

    /** Bare-metal canonical pattern: hostname. */
    public Properties stableHostName() throws UnknownHostException {
        Properties props = new Properties();
        String host = InetAddress.getLocalHost().getHostName(); // silent: value is not UUID.randomUUID()
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, host);
        return props;
    }

    /** Explicit literal — e.g. orchestrator-assigned per-replica index baked into the config. */
    public Properties stableLiteral() {
        Properties props = new Properties();
        props.put("group.instance.id", "consumer-0"); // silent: value is a literal, not UUID.toString
        return props;
    }

    /** UUID.randomUUID() used, but bound to client.id (a legitimate use). Different key — rule does not fire. */
    public Properties randomUuidForClientIdNotInstanceId() {
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()); // silent: key is client.id, not group.instance.id
        return props;
    }

    /** group.instance.id is simply absent — dynamic membership, no static-membership opt-in. */
    public Map<String, Object> groupInstanceIdAbsent() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("group.id", "order-processor-v1"); // silent: no group.instance.id key at all
        return cfg;
    }
}
