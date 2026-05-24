package sample;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Properties;

/**
 * Bad shape #3 — AdminClient.create() in &lt;clinit&gt;.
 * Compiles to {@code INVOKESTATIC AdminClient.create}; the rule matches the
 * static factory directly because the {@code NEW KafkaAdminClient} happens
 * inside the factory body and is invisible to a caller-side {@code NEW} scan.
 */
public final class BadAdminInStaticInit {

    /** Violation #3: INVOKESTATIC AdminClient.create inside &lt;clinit&gt;. */
    static final AdminClient ADMIN;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        ADMIN = AdminClient.create(props);
    }

    private BadAdminInStaticInit() {}
}
