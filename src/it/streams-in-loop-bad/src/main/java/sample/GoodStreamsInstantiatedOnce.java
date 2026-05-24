package sample;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Control for STREAMS_IN_LOOP.
 *
 * <p>Two methods that each avoid the rule by following the only
 * correct shape: construct ONE {@link KafkaStreams} OUTSIDE any loop
 * and reuse it.
 *
 * <ol>
 *   <li>{@code singleStreamsReusedAcrossWork} — one KafkaStreams
 *       instance constructed once, started once, and the loop body
 *       only performs operational work that is safe to repeat (a
 *       state-check log line, sleep, metric scrape). The
 *       {@code new KafkaStreams} site is OUTSIDE the loop, so the
 *       rule does not fire.</li>
 *   <li>{@code multiTopicsViaOneTopology} — the correct DSL shape for
 *       "process many topics in one application": build them as
 *       multiple sources of ONE topology, then construct ONE
 *       KafkaStreams from the combined topology. No loop around the
 *       constructor.</li>
 * </ol>
 */
public final class GoodStreamsInstantiatedOnce {

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

    /** Correct: one KafkaStreams constructed outside the loop, reused across iterations. */
    public void singleStreamsReusedAcrossWork(int iterations) {
        StreamsBuilder b = new StreamsBuilder();
        b.stream("in").to("out");
        KafkaStreams streams = new KafkaStreams(b.build(), streamsProps("good-streams-app"));
        try {
            streams.start();
            for (int i = 0; i < iterations; i++) {
                // operational work that does NOT construct a new streams instance
                KafkaStreams.State state = streams.state();
                if (state == null) { /* log */ }
            }
        } finally {
            streams.close(Duration.ofSeconds(30));
        }
    }

    /** Correct: multiple topics expressed as multiple sources of ONE topology — single KafkaStreams. */
    public void multiTopicsViaOneTopology(List<String> topics) {
        StreamsBuilder b = new StreamsBuilder();
        topics.forEach(topic -> b.stream(topic).to(topic + "-out")); // forEach over topics IS allowed — no KafkaStreams construction inside
        Topology topology = b.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsProps("good-streams-multi-topic-app"));
        try {
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(30));
        }
    }
}
