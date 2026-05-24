package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Optional;
import java.util.Properties;

/**
 * RULES: STREAMS_NO_UNCAUGHT_EXCEPTION_HANDLER /
 *        STREAMS_NO_STATE_LISTENER /
 *        STREAMS_NO_GLOBAL_STATE_RESTORE_LISTENER.
 *
 * <p>This fixture exercises the three Streams presence-detector rules' indy-aware
 * setter recognition. Each setter is wired through a conditional method-reference
 * capture — the canonical {@code Optional.ofNullable(x).ifPresent(streams::setXxx)}
 * idiom for "wire it only when configured".
 *
 * <h2>Why each capture site is a method-ref blind spot</h2>
 *
 * <p>{@code streams::setUncaughtExceptionHandler} compiles to an
 * {@code INVOKEDYNAMIC} bound to {@code LambdaMetafactory.metafactory}. The
 * indy's bsm-args carry the SAM-method signature, a direct
 * {@code REF_invokeVirtual KafkaStreams.setUncaughtExceptionHandler:(...)V}
 * handle, and the instantiated-method signature. The user-class bytecode
 * contains <strong>zero {@code INVOKE*} instructions targeting the setter</strong>
 * — the LambdaMetafactory-generated SAM adapter that actually calls the setter
 * lives in the JDK, not in the user class.
 *
 * <p>Before the indy walk was added, each rule scanned only {@code MethodInsnNode}s.
 * It would conclude {@code hasHandler == false} / {@code hasListener == false}
 * for every shape below, and fire a false-positive at the {@code start()} site
 * for each rule. With the indy walk, each rule inspects the bsm-arg handles
 * via {@code AsmUtil.indyTargetHandle(...)}, recognises the captured setter
 * handle as evidence the listener is wired, and suppresses.
 *
 * <h2>Expected behavior</h2>
 *
 * <p>All three rules are elevated to ERROR in the IT pom. With the indy walk
 * in place, ZERO violations fire — the IT build SUCCEEDS. A regression that
 * reverts the indy walk on any of the three rules would cause the
 * corresponding rule to fire at the {@code start()} site, and the IT build
 * would FAIL — which is the contract this fixture protects.
 */
public final class GoodStreamsListenersMethodRef {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "listener-method-ref-app");
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

    /**
     * Conditional wiring for all three listeners via method-ref capture, then
     * {@code start()}. Each {@code ifPresent(streams::setXxx)} emits an
     * {@code INVOKEDYNAMIC} whose bsm-args reference the setter directly; the
     * user-class bytecode has no {@code INVOKE*} targeting the setter.
     *
     * <p>Each setter takes the same {@code streams} receiver; we keep the
     * variable effectively-final by reading the trio of optional dependencies
     * BEFORE the {@code ifPresent} calls.
     */
    public void startWithListenersViaMethodRef(
            StreamsUncaughtExceptionHandler maybeHandler,
            KafkaStreams.StateListener maybeStateListener,
            StateRestoreListener maybeRestoreListener) {

        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());

        // Three setter method-ref captures. Without the indy walk, each rule
        // would fire at the start() site below for false-positive
        // "setter is never called anywhere in this project".
        Optional.ofNullable(maybeHandler).ifPresent(streams::setUncaughtExceptionHandler);
        Optional.ofNullable(maybeStateListener).ifPresent(streams::setStateListener);
        Optional.ofNullable(maybeRestoreListener).ifPresent(streams::setGlobalStateRestoreListener);

        streams.start();  // MUST NOT FIRE on any of the three rules
    }
}
