package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

/**
 * Silent: KafkaStreams.start() is called AND the hook is wired via a
 * deferred method-reference — Optional.of(thread).ifPresent(
 * Runtime.getRuntime()::addShutdownHook). That method-ref compiles to an
 * INVOKEDYNAMIC at the call site whose bootstrap-method args include a
 * REF_invokeVirtual handle to java/lang/Runtime.addShutdownHook. The
 * user-class bytecode contains zero INVOKE* instructions targeting
 * Runtime.addShutdownHook directly, so a MethodInsnNode-only scan would
 * (incorrectly) conclude hasHook=false and false-positive.
 *
 * The rule's INVOKEDYNAMIC walk inspects bsmArgs handles via
 * AsmUtil.indyTargetHandle(indy, RUNTIME_OWNERS, ADD_SHUTDOWN_HOOK, null)
 * and treats any captured handle as evidence that a hook is wired. The
 * project-scoped scan sets hasHook=true and the rule returns no violations.
 *
 * This shape is rare in straight-line Streams code (most apps inline
 * Runtime.getRuntime().addShutdownHook(...)) but appears in framework
 * helpers, in functional-style shutdown registries, and in test scaffolding
 * — the rule needs to recognise it to avoid noisy false-positives in those
 * codebases.
 */
public final class GoodStreamsWithMethodRefHook {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-methodref-hook-app");
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

    public void startWithMethodReferenceHook() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        Thread hookThread = new Thread(() -> streams.close(Duration.ofSeconds(30)));
        Optional.of(hookThread).ifPresent(Runtime.getRuntime()::addShutdownHook);
        streams.start();  // SILENT — INVOKEDYNAMIC method-ref to Runtime.addShutdownHook is recognised
    }
}
