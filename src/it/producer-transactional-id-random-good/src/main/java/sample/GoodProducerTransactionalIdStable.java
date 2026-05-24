package sample;

import java.util.Properties;
import java.util.UUID;

/**
 * Good shapes — must NOT fire PRODUCER_TRANSACTIONAL_ID_RANDOM.
 */
public final class GoodProducerTransactionalIdStable {

    /** Control #1: stable orchestrator-assigned id from environment. */
    public Properties orchestratorIdentity() {
        Properties props = new Properties();
        String podName = System.getenv("POD_NAME");
        props.put("transactional.id", podName != null ? podName : "fallback-id");
        return props;
    }

    /** Control #2: composed stable id from application config. */
    public Properties partitionScopedIdentity(String appId, int partition) {
        Properties props = new Properties();
        props.put("transactional.id", appId + "-" + partition);
        return props;
    }

    /** Control #3: UUID.randomUUID() used for an UNRELATED key — must not fire. */
    public Properties unrelatedRandom() {
        Properties props = new Properties();
        props.put("client.id", UUID.randomUUID().toString());
        return props;
    }

    /** Control #4: literal transactional.id (e.g. test scaffold) — different rule. */
    public Properties literalTxnId() {
        Properties props = new Properties();
        props.put("transactional.id", "order-processor-pod-7");
        return props;
    }
}
