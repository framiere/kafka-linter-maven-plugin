package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.time.Duration;
import java.util.Properties;

/**
 * Control for STREAMS_USED_AFTER_CLOSE.
 *
 * <p>Five methods that each avoid the rule for a DIFFERENT reason —
 * covering both the structural orthogonal exits the rule's check
 * supports AND the load-bearing documented exception
 * ({@code cleanUp()} is safe in NOT_RUNNING).
 *
 * <ol>
 *   <li>{@code startThenCloseInFinally} — the textbook correct order:
 *       start (USE) then close (CLOSED). The lifecycle method is
 *       reached BEFORE close, so when the rule scans linearly the
 *       slot is not yet in {@code closedSlots} at the start site.
 *       Silent.</li>
 *   <li>{@code closeOnlyNeverUseAfter} — close happens, but nothing
 *       lifecycle-relevant happens AFTER close on the same slot. The
 *       slot is in {@code closedSlots} from then on, but no
 *       subsequent lifecycle call references it. Silent.</li>
 *   <li>{@code closeThenCleanUp} — the load-bearing documented
 *       exception. The Javadoc explicitly states that {@code cleanUp()}
 *       is callable in NOT_RUNNING state, and applications legitimately
 *       call {@code close()} → {@code cleanUp()} to wipe the local
 *       state-store directory before a fresh topology run. The rule
 *       MUST exclude {@code cleanUp()} from the lifecycle set —
 *       silent.</li>
 *   <li>{@code closeThenStateAndMetrics} — {@code state()} and
 *       {@code metrics()} are not state-mutating and not in the
 *       lifecycle set; the rule deliberately excludes them. Calling
 *       them after close() to log the final state / scrape final
 *       metrics is a documented and idiomatic pattern. Silent.</li>
 *   <li>{@code closeFirstSlotUseSecondSlot} — two DIFFERENT streams
 *       slots in the same method. The rule tracks both slots
 *       independently. Closing slot A and then calling start() on
 *       slot B does NOT fire — the start's receiver resolves to B
 *       which is not closed. Silent. This is the case the rule's
 *       use of {@code resolveReceiverSlot} (rather than a "any
 *       previous close means we're done" approximation) is designed
 *       to handle.</li>
 * </ol>
 */
public final class GoodStreamsCloseOrder {

    /** Textbook order: start first, then close in finally. Slot not yet in closedSlots at start site. */
    public void startThenCloseInFinally() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        try {
            streams.start();                       // silent: closedSlots empty at this point
        } finally {
            streams.close(Duration.ofSeconds(30));
        }
    }

    /** Close is the only lifecycle call. No use-after. */
    public void closeOnlyNeverUseAfter() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.close(Duration.ofSeconds(30)); // and that's the entire body. Silent.
    }

    /** Documented exception: close() then cleanUp() is the canonical "wipe-state-then-restart" pattern. cleanUp is explicitly safe in NOT_RUNNING and is excluded from the lifecycle set. */
    public void closeThenCleanUp() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.close(Duration.ofSeconds(30));
        streams.cleanUp(); // silent — cleanUp is the ONE documented after-close-safe call
    }

    /** state() and metrics() are not state-mutating and are excluded from the lifecycle set. Logging final state / scraping final metrics post-close is idiomatic. */
    public void closeThenStateAndMetrics() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.close(Duration.ofSeconds(30));
        KafkaStreams.State finalState = streams.state();          // silent — state() excluded
        java.util.Map<?, ?> finalMetrics = streams.metrics();     // silent — metrics() excluded
        if (finalState != null && finalMetrics != null) { /* log */ }
    }

    /** Two slots: closing one and then using the OTHER must not fire. */
    public void closeFirstSlotUseSecondSlot() {
        Properties props = baseStreamsProps();
        Topology topology = baseTopology();
        KafkaStreams first = new KafkaStreams(topology, props);   // slot A
        KafkaStreams second = new KafkaStreams(topology, props);  // slot B
        first.close(Duration.ofSeconds(30));                       // closedSlots = { A }
        second.start();                                            // receiver resolves to B, B not closed — silent
        second.close(Duration.ofSeconds(30));
    }

    private static Properties baseStreamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-streams-close-order");
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
