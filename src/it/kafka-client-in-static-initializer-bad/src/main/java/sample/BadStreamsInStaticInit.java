package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

/**
 * Bad shape #4 — KafkaStreams constructed in &lt;clinit&gt;.
 * The worst case: Streams allocates a thread pool, locks the state directory,
 * and starts a global consumer — all triggered by classpath touch.
 */
public final class BadStreamsInStaticInit {

    /** Violation #4: NEW KafkaStreams inside &lt;clinit&gt;. */
    static final KafkaStreams STREAMS;

    static {
        Properties props = new Properties();
        props.put("application.id", "bad-streams-static-init");
        props.put("bootstrap.servers", "localhost:9092");
        Topology topology = new StreamsBuilder().build();
        STREAMS = new KafkaStreams(topology, props);
    }

    private BadStreamsInStaticInit() {}
}
