package sample;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * RULE: STREAMS_IN_LOOP.
 *
 * <p>Fires when {@code new KafkaStreams(...)} appears inside any
 * iteration context — a classic {@code for}/{@code while} loop, OR
 * an iterating lambda body ({@code list.forEach(...)},
 * {@code stream.map(...)}, etc.). Constructing a {@code KafkaStreams}
 * once per iteration is THE highest-magnitude anti-pattern the linter
 * detects on the Streams side, easily an order of magnitude worse
 * than the producer / consumer variants because the cost shape per
 * construction includes everything those variants have AND the state
 * directory file lock AND RocksDB instances AND the per-instance
 * StreamThread pool AND the eager shutdown hook.
 *
 * <p>Why {@code KafkaStreams} construction is catastrophically expensive:
 * <ol>
 *   <li><b>Four parallel client lifecycles.</b> Each instance starts
 *       an internal {@code Admin}, a global {@code KafkaConsumer} (if
 *       the topology has any {@code GlobalKTable}), a {@code restore
 *       consumer}, plus one {@code KafkaConsumer} + one
 *       {@code KafkaProducer} per StreamThread. Every one of those
 *       opens TCP sockets, fetches metadata, and runs its own sender
 *       / heartbeat thread.</li>
 *   <li><b>State-directory exclusive lock.</b> The constructor
 *       acquires {@code <state.dir>/<application.id>/<task>/.lock} for
 *       every task this instance will own. A second instance with the
 *       same {@code application.id} and {@code state.dir} cannot
 *       co-exist — the second constructor either blocks indefinitely
 *       (some NFS configurations) or throws {@code LockException}.
 *       Inside a loop, the SECOND iteration fails for this reason
 *       alone.</li>
 *   <li><b>RocksDB instances per persistent state store.</b> Each
 *       persistent store in the topology opens its own RocksDB
 *       instance: ~10 file descriptors, ~64 MB block cache, one
 *       background compaction thread. A loop with even a handful of
 *       iterations exhausts the process's file descriptor budget
 *       ({@code ulimit -n}, default 1024-65536).</li>
 *   <li><b>JMX MBean collisions.</b> Each StreamThread registers ~30
 *       MBeans under a {@code client-id} deterministically computed
 *       from {@code application.id}. The second iteration's
 *       registration throws {@code InstanceAlreadyExistsException}.</li>
 *   <li><b>JVM shutdown-hook leak.</b> Every constructed instance
 *       registers a {@code Runtime.getRuntime().addShutdownHook(...)}
 *       eagerly in the constructor (NOT in {@code start()}, so even an
 *       instance that's never started leaks the hook). Application
 *       code rarely calls {@code removeShutdownHook} symmetrically, so
 *       every iteration leaks a strong reference to the instance for
 *       the lifetime of the JVM.</li>
 *   <li><b>Broker-side rebalance storm.</b> Each new instance joins
 *       the consumer group derived from {@code application.id}; the
 *       broker rebalances on every join; a loop creating instances
 *       every few seconds saturates the group coordinator's CPU and
 *       degrades latency for every OTHER consumer group on the same
 *       broker.</li>
 * </ol>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A multi-tenant pipeline tries to process N customers'
 *       topologies in the same JVM with {@code for (Tenant t : tenants)
 *       { new KafkaStreams(t.topology, t.props).start(); }}. The first
 *       iteration succeeds; the second iteration throws
 *       {@code LockException} on the state-dir lock; the customer N=2
 *       sees no data.</li>
 *   <li>A retry loop wraps the constructor: {@code for (int retry = 0;
 *       retry < 3; retry++) { try { streams = new KafkaStreams(...);
 *       break; } catch (Exception e) {} }}. The first attempt
 *       partially constructs and throws on a downstream step (broker
 *       blip, RocksDB I/O error); the catch block doesn't
 *       {@code close()} because there's no instance reference yet;
 *       the second attempt fails on the orphan file lock.</li>
 *   <li>Test scaffolding that legitimately constructs and tears down
 *       instances per test case gets copied into production
 *       initialization code as "make this configurable per topic" —
 *       the pattern that was safe in a test (because each test ran in
 *       its own JVM with a unique {@code state.dir}) catastrophically
 *       breaks in production.</li>
 * </ol>
 *
 * <p>What the rule catches: every {@code NEW org/apache/kafka/streams/KafkaStreams}
 * instruction whose containing instruction is either inside a
 * back-edge (classic loop, detected by {@code LoopFinder}) or inside
 * a method body classified as an iterating lambda (detected by
 * {@code LambdaTracker} — {@code forEach}, stream operations, etc.).
 *
 * <p>Correct pattern: ONE {@code KafkaStreams} instance per process,
 * constructed at application startup, owned by the application
 * lifecycle, and closed deterministically at shutdown. If the
 * application needs to process multiple pipelines, build them as
 * subgraphs of ONE topology — that is exactly what the DSL is for.
 * For unit tests, use {@code TopologyTestDriver} which has no network,
 * no state-dir lock, and no shutdown hook.
 */
public final class BadStreamsInLoop {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties streamsProps(String applicationId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "3");
        p.put(StreamsConfig.STATE_DIR_CONFIG, "/var/lib/streams/" + applicationId);
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return p;
    }

    private static Topology topologyFor(String topic) {
        StreamsBuilder b = new StreamsBuilder();
        b.stream(topic).to(topic + "-out");
        return b.build();
    }

    /** Anti-pattern: new KafkaStreams inside a classic for-loop — FIRES. */
    public void newStreamsInForLoop(List<String> topics) {
        for (String topic : topics) {
            KafkaStreams streams = new KafkaStreams(topologyFor(topic), streamsProps(topic + "-app")); // FIRES
            try {
                streams.start();
            } finally {
                streams.close(Duration.ofSeconds(30));
            }
        }
    }

    /** Anti-pattern: new KafkaStreams inside a while-loop — FIRES. */
    public void newStreamsInWhileLoop(int iterations) {
        int i = 0;
        while (i < iterations) {
            KafkaStreams streams = new KafkaStreams(topologyFor("topic-" + i), streamsProps("app-" + i)); // FIRES
            try {
                streams.start();
            } finally {
                streams.close(Duration.ofSeconds(30));
            }
            i++;
        }
    }

    /** Anti-pattern: new KafkaStreams inside a forEach iterating lambda — FIRES. */
    public void newStreamsInForEachLambda(List<String> topics) {
        topics.forEach(topic -> {
            KafkaStreams streams = new KafkaStreams(topologyFor(topic), streamsProps(topic + "-app")); // FIRES
            try {
                streams.start();
            } finally {
                streams.close(Duration.ofSeconds(30));
            }
        });
    }
}
