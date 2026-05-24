package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

/**
 * RULE: STREAMS_NO_SHUTDOWN_HOOK.
 *
 * KafkaStreams.start() is called in this project but
 * Runtime.getRuntime().addShutdownHook(...) is never called anywhere — neither
 * as a direct INVOKEVIRTUAL nor as an INVOKEDYNAMIC method-reference handle.
 *
 * On SIGTERM (Kubernetes pod rotation, systemd stop, `docker stop`), the JVM
 * runs registered shutdown hooks then exits. With zero registered hooks for
 * Streams, the kubelet's grace-period SIGKILL fires while StreamThreads are
 * still processing:
 *   - RocksDB's in-memory write cache is dropped (next restart restores the
 *     store to a state the topology never emitted),
 *   - the embedded producer accumulator is dropped (silent data loss on
 *     in-flight records),
 *   - any open EOS transaction is abandoned — the transactional.id stays
 *     fenced on the broker until transaction.timeout.ms expires (default 10
 *     minutes for Streams), CrashLoopBackOffing the next deploy with
 *     ProducerFencedException for that window.
 *
 * The rule is project-scoped: it walks target/classes for KafkaStreams.start()
 * sites and Runtime.addShutdownHook(...) sites. Every start() site fires when
 * no shutdown hook is registered anywhere in the project. Two start() sites in
 * this file → two violations.
 */
public final class BadStreamsNoShutdownHook {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "no-shutdown-hook-app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        return p;
    }

    private static Topology topology() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in").to("out");
        return b.build();
    }

    public void startWithoutHook() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.start();  // FIRES — no Runtime.addShutdownHook(...) anywhere in this project
    }

    public void startAgainElsewhereWithoutHook() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.start();  // FIRES — second site
    }
}
