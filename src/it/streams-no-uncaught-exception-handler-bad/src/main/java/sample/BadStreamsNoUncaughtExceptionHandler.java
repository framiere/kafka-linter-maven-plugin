package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

/**
 * RULE: STREAMS_NO_UNCAUGHT_EXCEPTION_HANDLER.
 *
 * <p>Project-scoped rule. {@code KafkaStreams.start()} is called on at least one
 * compiled class in this project, but {@code KafkaStreams.setUncaughtExceptionHandler(...)}
 * is never called anywhere — neither as a direct {@code INVOKEVIRTUAL} on
 * {@code org/apache/kafka/streams/KafkaStreams}, nor as an {@code INVOKEDYNAMIC}
 * method-reference bootstrap whose target handle is that setter.
 *
 * <h2>What goes wrong at runtime when no handler is registered</h2>
 *
 * <p>KafkaStreams is two layers: the {@code KafkaStreams} client (one per JVM)
 * and the pool of {@code StreamThread}s (one per {@code num.stream.threads},
 * default 1) that actually execute the topology. When a StreamThread hits an
 * exception the topology doesn't catch — a {@code DeserializationException} that
 * the deserialization handler returned FAIL on, a {@code ProcessorException} from
 * user code (a NullPointerException inside a {@code mapValues} lambda, an
 * {@code ArithmeticException} from a divide-by-zero in an aggregator), a
 * {@code ProducerFencedException} during EOS (the broker rotated the
 * transactional.id), or a {@code TaskCorruptedException} when the local state
 * store has drifted from its changelog — that one thread dies.
 *
 * <p>Without a handler, Streams' fallback is {@code Thread.getUncaughtExceptionHandler()},
 * which is the JVM-process-default — a single {@code System.err.println(stackTrace)}
 * and nothing else. The dead thread is NOT replaced. The {@code KafkaStreams}
 * client's state transitions to {@code ERROR} only after the LAST StreamThread
 * dies; until then it sits at {@code RUNNING}, reporting healthy. With
 * {@code num.stream.threads=4}, three dead threads still leaves one running —
 * meaning the application appears alive but is processing at 25% capacity, and
 * any task assigned to a dead thread is just... not processing. There is no
 * automatic restart, no operator notification, no {@code System.exit}, no
 * rebalance (because the client is still a group member with active heartbeats),
 * and no metric the application exposes by default. The lag grows on the dead
 * thread's partitions; downstream consumers see lag accumulating against a
 * topology that every dashboard reports as healthy.
 *
 * <h2>KIP-671 made the fix actionable, did NOT change the default</h2>
 *
 * <p>Before KIP-671 (Kafka 2.8, 2021), the only handler API was the legacy
 * {@code Thread.UncaughtExceptionHandler} overload — a void-returning callback
 * that couldn't actually do anything useful (the thread was already dying by
 * the time it ran, and there was no way to request a thread-replacement or a
 * client-shutdown from inside the handler). Most applications registered
 * nothing because the handler had no actionable behavior.
 *
 * <p>KIP-671 introduced {@code StreamsUncaughtExceptionHandler}, which returns
 * a {@code StreamThreadExceptionResponse} enum: {@code REPLACE_THREAD} (the
 * runtime spawns a replacement StreamThread on the same instance — best for
 * transient errors that won't recur), {@code SHUTDOWN_CLIENT} (this Streams
 * client transitions to ERROR and the application can react via state listener,
 * typically calling {@code System.exit(1)} so Kubernetes restarts the pod — best
 * for instance-local non-recoverables like state-store corruption), or
 * {@code SHUTDOWN_APPLICATION} (a coordinated multi-instance shutdown — best
 * for catastrophic errors that any instance hitting the same poison record will
 * also hit, e.g. a schema-incompatible upstream).
 *
 * <p>What KIP-671 did NOT change is the default — there is still no implicit
 * {@code REPLACE_THREAD}, no implicit {@code SHUTDOWN_APPLICATION}. The safe
 * default does not exist; you must opt in by registering a handler. The rule
 * accepts EITHER overload (both legacy and KIP-671 are suppressors), but the
 * companion rule {@code STREAMS_SET_UNCAUGHT_EXCEPTION_HANDLER_LEGACY_DEPRECATED}
 * (separate rule) flags the legacy overload because in practice the legacy
 * handler does nothing actionable.
 *
 * <h2>What this rule catches in THIS project</h2>
 *
 * <p>The class below has TWO {@code KafkaStreams.start()} invocations
 * ({@code startWithoutHandler} and {@code startAgainElsewhereWithoutHandler})
 * and NO {@code setUncaughtExceptionHandler(...)} invocation anywhere. The
 * rule walks every {@code .class} file under {@code target/classes},
 * accumulates start sites, looks for handler-registration sites (direct
 * {@code MethodInsnNode} OR {@code INVOKEDYNAMIC} bootstrap-arg handle), and
 * — finding none — emits one violation per start site. Expected: 2 violations,
 * build failure.
 */
public final class BadStreamsNoUncaughtExceptionHandler {

    private static Properties streamsProps(String appId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return p;
    }

    private static Topology topology() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("orders").mapValues(v -> v.toUpperCase()).to("orders-upper");
        return b.build();
    }

    /**
     * First start() site. KafkaStreams client constructed with 4 StreamThreads
     * and EOS-v2. No handler registered — every StreamThread death is silent,
     * every dead thread's tasks stop processing, the client stays RUNNING.
     */
    public void startWithoutHandler() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("no-handler-app-a"));
        streams.start();  // FIRES — no setUncaughtExceptionHandler() anywhere in this project
    }

    /**
     * Second start() site (different method, same project). The rule is
     * project-scoped so the absence of any handler call anywhere fires every
     * start() site independently.
     */
    public void startAgainElsewhereWithoutHandler() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("no-handler-app-b"));
        streams.start();  // FIRES — second start site, still no handler anywhere
    }
}
