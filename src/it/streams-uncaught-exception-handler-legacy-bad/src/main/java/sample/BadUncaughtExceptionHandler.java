package sample;

import org.apache.kafka.streams.KafkaStreams;

import java.util.function.BiConsumer;

/**
 * RULE: STREAMS_SET_UNCAUGHT_EXCEPTION_HANDLER_LEGACY_DEPRECATED —
 * must fire on both methods below.
 *
 * <p>The two methods below exercise the two distinct bytecode shapes
 * the rule is required to catch — one direct call plus one
 * {@code INVOKEDYNAMIC} method-ref capture:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaStreams.setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)}.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KafkaStreams::setUncaughtExceptionHandler} bound to a
 *       {@code BiConsumer<KafkaStreams, Thread.UncaughtExceptionHandler>}
 *       whose erased signature is
 *       {@code (Ljava/lang/Object;Ljava/lang/Object;)V}. javac resolves
 *       the method-ref to the legacy overload by matching the SAM's
 *       (receiver, arg-erasures, return-erasure) triple — the legacy
 *       overload takes {@code Thread.UncaughtExceptionHandler}, which
 *       erases to {@code Object} in the SAM's bridge. The user-class
 *       bytecode at this site contains ZERO direct INVOKEVIRTUAL on
 *       the legacy method — only the INVOKEDYNAMIC +
 *       LambdaMetafactory bridge.</li>
 * </ol>
 *
 * <h2>Why this overload is deprecated</h2>
 *
 * <p>KIP-671 (Kafka Streams 2.8, April 2021) introduced
 * {@code setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)}
 * whose handler returns a triple-valued
 * {@code StreamThreadExceptionResponse} enum — {@code REPLACE_THREAD},
 * {@code SHUTDOWN_CLIENT}, or {@code SHUTDOWN_APPLICATION} — letting
 * the topology author tell the Kafka Streams supervisor how to react
 * to each exception class. The legacy
 * {@code Thread.UncaughtExceptionHandler} returns void and can only
 * observe the dying thread, which loses per-instance parallelism
 * silently, cannot fail the whole application cluster on contract-
 * violation exceptions, cannot distinguish host-local from cluster-
 * wide failure, and blocks the rebalance for as long as the handler
 * is running on the dying thread.
 */
public final class BadUncaughtExceptionHandler {

    @SuppressWarnings("deprecation")
    public void directLegacy(KafkaStreams streams) {
        // MUST FIRE — setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler).
        streams.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
            System.err.println("thread died: " + t.getName());
        });
    }

    @SuppressWarnings("deprecation")
    public BiConsumer<KafkaStreams, Thread.UncaughtExceptionHandler> capturedLegacy() {
        // MUST FIRE — INVOKEDYNAMIC unbound method-ref capture
        // targeting the deprecated
        // setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)
        // overload. javac emits an INVOKEDYNAMIC site whose bsm-args
        // contain a REF_invokeVirtual handle on
        // (Ljava/lang/Thread$UncaughtExceptionHandler;)V. The user-
        // class bytecode here contains ZERO direct INVOKEVIRTUAL on
        // the legacy method.
        return KafkaStreams::setUncaughtExceptionHandler;
    }
}
