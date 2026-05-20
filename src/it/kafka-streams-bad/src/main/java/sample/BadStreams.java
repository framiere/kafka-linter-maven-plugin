package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public final class BadStreams {

    // RULE: STREAMS_REPLICATION_FACTOR_ONE.
    public Properties replicationFactorOne() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("replication.factor", "1");
        return p;
    }

    // RULE: STREAMS_STATE_DIR_TMP.
    public Properties stateDirInTmp() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("state.dir", "/tmp/streams-state");
        return p;
    }

    // RULE: STREAMS_EOS_V1_DEPRECATED.
    public Properties eosV1() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("processing.guarantee", "exactly_once");
        return p;
    }

    // RULE: STREAMS_CLEANUP_IN_PROD.
    public void cleanUpAtStart() {
        StreamsBuilder b = new StreamsBuilder();
        Topology t = b.build();
        KafkaStreams streams = new KafkaStreams(t, replicationFactorOne());
        streams.cleanUp();
        streams.start();
    }

    // RULE: STREAMS_THROUGH_DEPRECATED.
    public void throughDeprecated(StreamsBuilder b) {
        KStream<String, String> s = b.stream("in");
        s.through("intermediate").to("out");
    }
}
