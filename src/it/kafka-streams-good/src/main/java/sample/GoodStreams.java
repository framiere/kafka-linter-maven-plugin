package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;

import java.util.Properties;

public final class GoodStreams {

    public Properties safeProperties() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("replication.factor", "3");
        p.put("state.dir", "/var/lib/streams");
        p.put("processing.guarantee", "exactly_once_v2");
        return p;
    }

    public void topology(StreamsBuilder b) {
        KStream<String, String> s = b.stream("in", Consumed.with(Serdes.String(), Serdes.String()).withName("in-source"));
        s.repartition(Repartitioned.<String, String>as("in-repartition"))
         .to("out", Produced.with(Serdes.String(), Serdes.String()).withName("out-sink"));
    }
}
