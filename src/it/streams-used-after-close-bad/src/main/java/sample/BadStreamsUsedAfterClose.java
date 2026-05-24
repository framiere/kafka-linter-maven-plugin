package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.QueryableStoreTypes;

import java.time.Duration;
import java.util.Properties;

/**
 * RULE: STREAMS_USED_AFTER_CLOSE.
 *
 * <p>Fires when a lifecycle method (start / pause / resume /
 * addStreamThread / removeStreamThread / set*Listener /
 * setUncaughtExceptionHandler / store / allLocalStorePartitionLags /
 * queryMetadataForKey / allMetadata / allMetadataForStore /
 * streamsMetadataForStore) is invoked on a local-slot {@link KafkaStreams}
 * AFTER {@code close()} has been called on the same slot earlier in the
 * same method.
 *
 * <p>Why use-after-close on a Streams application is a serious bug — what
 * actually happens at runtime:
 * <ol>
 *   <li>{@code KafkaStreams.close()} initiates a ONE-WAY transition from
 *       whatever state the instance is in (CREATED / REBALANCING / RUNNING
 *       / ERROR) through PENDING_SHUTDOWN to NOT_RUNNING. It signals every
 *       StreamThread to shut down, waits up to the configured timeout for
 *       each to drain its assigned tasks, closes the underlying restore
 *       consumer / main consumer / producer / admin sockets, and finally
 *       sets the instance state to NOT_RUNNING. The transition is
 *       irreversible — there is no "reopen()".</li>
 *   <li>Every state-mutating lifecycle call (start, pause, resume,
 *       addStreamThread, removeStreamThread) checks the current state
 *       on entry and throws {@code IllegalStateException} unless the
 *       instance is in the specific state(s) documented for that
 *       method. After close() the state is NOT_RUNNING, which none of
 *       these methods accepts — every one of them throws.</li>
 *   <li>Every listener-registration call (setStateListener,
 *       setGlobalStateRestoreListener, setStandbyUpdateListener,
 *       setUncaughtExceptionHandler) requires the instance to be in
 *       state CREATED (i.e. the very brief window between construction
 *       and the first call to start()). After close() the state is
 *       NOT_RUNNING — those calls throw {@code IllegalStateException}
 *       too. A common author mistake is to register a handler in a
 *       "shutdown" routine that has already called close() in an earlier
 *       branch.</li>
 *   <li>Every interactive-query call (store, allLocalStorePartitionLags,
 *       queryMetadataForKey, allMetadata, allMetadataForStore,
 *       streamsMetadataForStore) requires RUNNING and otherwise
 *       throws {@code InvalidStateStoreException} or returns null —
 *       silent in production. An async query worker that races the
 *       shutdown path will see {@code null} and surface "store not
 *       found" to the caller rather than the real cause ("the topology
 *       you are querying is closed").</li>
 *   <li>The synchronous IllegalStateException is acceptable when it
 *       propagates to a caller that catches and surfaces it. In
 *       practice the streams instance is often used via an async path:
 *       a shutdown hook that races a control-plane HTTP endpoint, a
 *       worker pool that calls {@code addStreamThread()} from a metrics
 *       reaction, a {@code CompletableFuture.thenApply(...)} that fires
 *       after the try-with-resources block has already exited. In those
 *       flows, the exception is swallowed by the framework (or logged
 *       at WARN and forgotten) and the requested operation NEVER
 *       ACTUALLY RUNS — the application appears to have done what was
 *       asked but the change is silently dropped.</li>
 * </ol>
 *
 * <p>The only documented exception is {@code cleanUp()}, which the
 * Javadoc explicitly states is callable in NOT_RUNNING state — and which
 * applications legitimately call right after close() to wipe the local
 * state-store directory before a fresh topology run. The rule MUST
 * exclude {@code cleanUp()} from the lifecycle set, otherwise it would
 * fire on the canonical close → cleanUp → start reset pattern.
 *
 * <p>What the rule catches (per-METHOD scan, local-slot tracking):
 * <ol>
 *   <li>Track every {@code new KafkaStreams(...) + ASTORE N} — record
 *       slot N. The constructor pattern is what registers the slot
 *       (no static factory like Producer / Consumer).</li>
 *   <li>For every method call whose receiver resolves to a tracked slot
 *       (via {@code AsmUtil.resolveReceiverSlot} — backwards stack
 *       simulation that handles nested arg expressions like
 *       {@code store(StoreQueryParameters.fromNameAndType(...))}):
 *       <ul>
 *         <li>If the method name starts with {@code "close"}, add the
 *             slot to {@code closedSlots}.</li>
 *         <li>Otherwise, if the name is in the lifecycle set AND the
 *             slot is already in {@code closedSlots}, fire.</li>
 *       </ul></li>
 *   <li>{@code INVOKEDYNAMIC} method-reference captures (e.g.
 *       {@code Runnable r = streams::start}) are also caught — the
 *       deferred call throws when the captured functional interface is
 *       eventually invoked, often on an executor where the
 *       IllegalStateException is silently swallowed.</li>
 *   <li>Fire site is the lifecycle call (NOT the close call) — the
 *       operator wants to know where the offending use happens.</li>
 * </ol>
 *
 * <p>This Bad class triggers SIX fires — six lifecycle operations each
 * performed after close() on the same slot, covering the three families
 * (state transitions, listeners, interactive queries) plus a
 * nested-arg-expression case and a method-reference capture.
 */
public final class BadStreamsUsedAfterClose {

    /** Anti-pattern: close() then start(). The classic "I'll close first to be safe" misread of the lifecycle. */
    public void closeThenStart() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.close();
        streams.start(); // reported
    }

    /** Anti-pattern: close() then pause(). Pause requires RUNNING — IllegalStateException, silently swallowed on the control thread. */
    public void closeThenPause() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.close();
        streams.pause(); // reported
    }

    /** Anti-pattern: close() then addStreamThread(). The "scale-up reaction" path that races shutdown. */
    public void closeThenAddStreamThread() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.close();
        streams.addStreamThread(); // reported
    }

    /** Anti-pattern: close() then setStateListener(...). State-listener registration requires CREATED — throws after NOT_RUNNING. */
    public void closeThenSetStateListener() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.close();
        streams.setStateListener((newState, oldState) -> {}); // reported
    }

    /** Anti-pattern: close() then setUncaughtExceptionHandler(...). The "I'll add the handler in finally" misread — finally runs AFTER close. */
    public void closeThenSetUncaughtExceptionHandler() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.close();
        streams.setUncaughtExceptionHandler( // reported
                (StreamsUncaughtExceptionHandler) ex ->
                        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
    }

    /** Anti-pattern: close() then store(...). IQ after shutdown — returns null / throws InvalidStateStoreException, often misread as "store not found". Nested-arg expression exercises receiver-resolution. */
    public void closeThenStoreWithNestedArg() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.close();
        streams.store(StoreQueryParameters.fromNameAndType( // reported
                "orders-by-id", QueryableStoreTypes.keyValueStore()));
    }

    private static Properties baseStreamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-used-after-close-bad");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "3");
        p.put(StreamsConfig.STATE_DIR_CONFIG, "/var/lib/streams");
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return p;
    }

    private static Topology baseTopology() {
        StreamsBuilder b = new StreamsBuilder();
        b.stream("in").to("out");
        return b.build();
    }
}
