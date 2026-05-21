package sample;

import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * RULES (project-scoped, exercised via pom.xml):
 *   KAFKA_CLIENTS_EOL              — kafka-clients 3.4.0 < 3.5.0 EOL floor.
 *   KAFKA_CLIENTS_CVE_JNDI_LDAP    — kafka-clients 3.4.0 < 3.5.2 JNDI-safe floor.
 *   JAVA_VERSION_TOO_LOW           — maven.compiler.target=1.8 with kafka-clients dep.
 *
 * The Java source itself is trivial — the rules fire on dependency/version metadata,
 * not on bytecode. We need at least one class so Maven compiles something.
 */
public final class BadVersion {

    public static String bootstrapKey() {
        return ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
    }
}
