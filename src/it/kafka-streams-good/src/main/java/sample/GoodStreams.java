package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public final class GoodStreams {

    public Properties safeProperties() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("replication.factor", "3");
        p.put("state.dir", "/var/lib/streams");
        p.put("processing.guarantee", "exactly_once_v2");
        return p;
    }

    public void topology(StreamsBuilder b) {
        KStream<String, String> s = b.stream("in");
        s.repartition().to("out");
    }
}
