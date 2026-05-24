package sample;

import java.util.Properties;
import java.util.UUID;

/**
 * Good shapes — must NOT fire STREAMS_APPLICATION_ID_RANDOM.
 */
public final class GoodStreamsApplicationIdStable {

    /** Control #1: stable orchestrator-assigned id from environment. */
    public Properties orchestratorIdentity() {
        Properties props = new Properties();
        String deployment = System.getenv("DEPLOYMENT_NAME");
        props.put("application.id", deployment != null ? deployment : "order-projection");
        return props;
    }

    /** Control #2: versioned id explicitly bumped on incompatible topology changes. */
    public Properties versionedIdentity(String env) {
        Properties props = new Properties();
        props.put("application.id", "order-projection-" + env + "-v3");
        return props;
    }

    /** Control #3: UUID.randomUUID() used for an UNRELATED key (client.id) — must not fire. */
    public Properties unrelatedRandom() {
        Properties props = new Properties();
        props.put("client.id", UUID.randomUUID().toString());
        return props;
    }

    /** Control #4: literal application.id — different rule (typo/generic-id), not this one. */
    public Properties literalApplicationId() {
        Properties props = new Properties();
        props.put("application.id", "billing-aggregator");
        return props;
    }
}
