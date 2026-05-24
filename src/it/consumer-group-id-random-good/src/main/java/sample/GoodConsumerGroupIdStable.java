package sample;

import java.util.Properties;
import java.util.UUID;

/**
 * Good shapes — must NOT fire CONSUMER_GROUP_ID_RANDOM.
 */
public final class GoodConsumerGroupIdStable {

    /** Control #1: stable orchestrator-assigned id from environment. */
    public Properties orchestratorIdentity() {
        Properties props = new Properties();
        String deployment = System.getenv("DEPLOYMENT_NAME");
        props.put("group.id", deployment != null ? deployment : "order-processor");
        return props;
    }

    /** Control #2: versioned id explicitly bumped on incompatible semantics changes. */
    public Properties versionedIdentity(String env) {
        Properties props = new Properties();
        props.put("group.id", "order-processor-" + env + "-v3");
        return props;
    }

    /** Control #3: UUID.randomUUID() used for an UNRELATED key (client.id) — must not fire. */
    public Properties unrelatedRandom() {
        Properties props = new Properties();
        props.put("client.id", UUID.randomUUID().toString());
        return props;
    }

    /** Control #4: literal group.id — different rule (typo/generic-id), not this one. */
    public Properties literalGroupId() {
        Properties props = new Properties();
        props.put("group.id", "billing-aggregator");
        return props;
    }
}
