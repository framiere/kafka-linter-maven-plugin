package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

public final class BadStreams {

    // RULE: STREAMS_REPLICATION_FACTOR_ONE.
    public Properties replicationFactorOne() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("replication.factor", "1");
        return p;
    }

    // RULE: STREAMS_STATE_DIR_TMP.
    public Properties stateDirInTmp() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("state.dir", "/tmp/streams-state");
        return p;
    }

    // RULE: STREAMS_EOS_V1_DEPRECATED.
    public Properties eosV1() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
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

    // RULE: STREAMS_COMMIT_INTERVAL_TOO_LOW.
    public Properties commitIntervalTooLow() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("commit.interval.ms", "50");
        return p;
    }

    // RULE: STREAMS_CACHE_DISABLED.
    public Properties cacheDisabled() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("cache.max.bytes.buffering", "0");
        return p;
    }

    // RULE: STREAMS_THROUGH_DEPRECATED.
    public void throughDeprecated(StreamsBuilder b) {
        KStream<String, String> s = b.stream("in");
        s.through("intermediate").to("out");
    }

    // RULE: STREAMS_NUM_STANDBY_REPLICAS_ZERO.
    public Properties numStandbyReplicasZero() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("num.standby.replicas", "0");
        return p;
    }

    // RULE: STREAMS_DESER_HANDLER_LOG_AND_CONTINUE.
    public Properties deserHandlerLogAndContinue() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("default.deserialization.exception.handler",
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
        return p;
    }

    // RULE: STREAMS_CACHE_KEY_DEPRECATED — non-zero value, isolates this rule from STREAMS_CACHE_DISABLED.
    public Properties cacheKeyDeprecated() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("cache.max.bytes.buffering", "10485760");
        return p;
    }

    // RULE: STREAMS_APPLICATION_ID_GENERIC.
    public Properties applicationIdGeneric() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_TASK_TIMEOUT_MS_ZERO.
    public Properties taskTimeoutMsZero() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("task.timeout.ms", "0");
        return p;
    }

    // RULE: STREAMS_KSTREAM_PRINT.
    public void kstreamPrint(StreamsBuilder b) {
        KStream<String, String> s = b.stream("in");
        s.print(Printed.toSysOut());
    }

    // RULE: STREAMS_REPLICATION_FACTOR_TWO.
    public Properties replicationFactorTwo() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("replication.factor", "2");
        return p;
    }

    // RULE: STREAMS_TOPOLOGY_OPTIMIZATION_NONE.
    public Properties topologyOptimizationNone() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("topology.optimization", "none");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_MAX_TASK_IDLE_MS_HIGH.
    public Properties maxTaskIdleMsHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("max.task.idle.ms", "60000");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_PRODUCTION_EXCEPTION_HANDLER_ALWAYS_CONTINUE.
    public Properties productionExceptionHandlerAlwaysContinue() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("default.production.exception.handler",
                "org.apache.kafka.streams.errors.AlwaysContinueProductionExceptionHandler");
        p.put("replication.factor", "3");
        return p;
    }
}
