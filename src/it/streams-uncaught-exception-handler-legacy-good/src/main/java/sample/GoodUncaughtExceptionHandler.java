package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;

import java.util.function.BiConsumer;

/**
 * RULE: STREAMS_SET_UNCAUGHT_EXCEPTION_HANDLER_LEGACY_DEPRECATED —
 * must NOT fire on any method below.
 *
 * <p>Both methods reach the supported KIP-671 overload
 * {@code setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on the new overload. Same method
 *       name as the legacy overload, but a distinct descriptor:
 *       {@code (Lorg/apache/kafka/streams/errors/StreamsUncaughtExceptionHandler;)V}.
 *       The rule pins the legacy descriptor, so this call site is
 *       rejected.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KafkaStreams::setUncaughtExceptionHandler} bound to a
 *       SAM whose erased argument type matches
 *       {@code StreamsUncaughtExceptionHandler} — javac resolves the
 *       handle to the new overload, so the bsm-arg descriptor is the
 *       new one and the rule's descriptor filter rejects it.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>The handler now returns a {@code StreamThreadExceptionResponse}
 * enum value that tells the Kafka Streams supervisor what to do next:
 * {@code REPLACE_THREAD} spawns a fresh stream-thread on this instance
 * to preserve parallelism without a consumer-group rebalance;
 * {@code SHUTDOWN_CLIENT} closes this instance while peers absorb the
 * tasks; {@code SHUTDOWN_APPLICATION} propagates a sentinel through
 * the rebalance protocol asking every other instance of the
 * application to close as well — cluster-wide fail-fast on
 * contract-violation exceptions. The legacy void handler could
 * express none of these.
 */
public final class GoodUncaughtExceptionHandler {

    public void directNewOverload(KafkaStreams streams) {
        // DOES NOT FIRE — setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)
        // is the supported KIP-671 replacement. Distinct descriptor
        // from the legacy overload, so the rule's descriptor filter
        // rejects this site.
        streams.setUncaughtExceptionHandler((Throwable e) -> {
            if (e instanceof IllegalStateException) {
                return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            }
            if (e instanceof OutOfMemoryError) {
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
            return StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }

    public BiConsumer<KafkaStreams, StreamsUncaughtExceptionHandler> capturedNew() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler).
        // The bsm-arg handle's descriptor is the new one, not the
        // legacy one, so the rule's descriptor filter rejects this
        // site.
        return KafkaStreams::setUncaughtExceptionHandler;
    }
}
