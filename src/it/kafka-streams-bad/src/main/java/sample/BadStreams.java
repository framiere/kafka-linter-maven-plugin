package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

import java.time.Duration;
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

    // RULE: STREAMS_NUM_STREAM_THREADS_ONE.
    public Properties numStreamThreadsOne() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("num.stream.threads", "1");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_WALL_CLOCK.
    public Properties defaultTimestampExtractorWallClock() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("default.timestamp.extractor",
                "org.apache.kafka.streams.processor.WallclockTimestampExtractor");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_NUM_STREAM_THREADS_TOO_HIGH.
    public Properties numStreamThreadsTooHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("num.stream.threads", "256");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_TASK_TIMEOUT_MS_TOO_HIGH.
    public Properties taskTimeoutMsTooHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("task.timeout.ms", "3600000");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_REPLICATION_FACTOR_TOO_HIGH.
    public Properties replicationFactorTooHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("replication.factor", "7");
        return p;
    }

    // RULE: STREAMS_NUM_STANDBY_REPLICAS_TOO_HIGH.
    public Properties numStandbyReplicasTooHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("num.standby.replicas", "5");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_HIGH.
    public Properties bufferedRecordsPerPartitionTooHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("buffered.records.per.partition", "1000000");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_NONE.
    public Properties rackAwareAssignmentStrategyNone() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("rack.aware.assignment.strategy", "none");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_LOW.
    public Properties probingRebalanceIntervalMsTooLow() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("probing.rebalance.interval.ms", "10000");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_STATESTORE_CACHE_MAX_BYTES_ZERO.
    public Properties statestoreCacheMaxBytesZero() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("statestore.cache.max.bytes", "0");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_APPLICATION_SERVER_LOCALHOST.
    public Properties applicationServerLocalhost() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("application.server", "localhost:8080");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_MAX_WARMUP_REPLICAS_ZERO.
    public Properties maxWarmupReplicasZero() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("max.warmup.replicas", "0");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_APPLICATION_ID_PLACEHOLDER.
    public Properties applicationIdPlaceholder() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "${SERVICE_NAME}");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_TRANSFORM_DEPRECATED — KIP-820 replaced this with KStream.process(ProcessorSupplier).
    @SuppressWarnings("deprecation")
    public void transformDeprecated(StreamsBuilder b) {
        KStream<String, String> s = b.stream("in");
        TransformerSupplier<String, String, KeyValue<String, String>> supplier = () -> null;
        s.transform(supplier);
    }

    // RULE: STREAMS_TRANSFORM_VALUES_DEPRECATED — KIP-820 replaced this with KStream.processValues(FixedKeyProcessorSupplier).
    @SuppressWarnings("deprecation")
    public void transformValuesDeprecated(StreamsBuilder b) {
        KStream<String, String> s = b.stream("in");
        ValueTransformerWithKeySupplier<String, String, String> supplier = () -> null;
        s.transformValues(supplier);
    }

    // RULE: STREAMS_BRANCH_DEPRECATED — KIP-418 replaced this with KStream.split().branch(...).
    @SuppressWarnings({"deprecation", "unchecked"})
    public void branchDeprecated(StreamsBuilder b) {
        KStream<String, String> s = b.stream("in");
        KStream<String, String>[] branches = s.branch((k, v) -> true, (k, v) -> false);
    }

    // RULE: STREAMS_FLAT_TRANSFORM_DEPRECATED — KIP-820 replaced this with KStream.process(ProcessorSupplier).
    @SuppressWarnings("deprecation")
    public void flatTransformDeprecated(StreamsBuilder b) {
        KStream<String, String> s = b.stream("in");
        TransformerSupplier<String, String, Iterable<KeyValue<String, String>>> supplier = () -> null;
        s.flatTransform(supplier);
    }

    // RULE: STREAMS_TIME_WINDOWS_OF_DEPRECATED — KIP-633 forces explicit grace period via ofSizeWithNoGrace/ofSizeAndGrace.
    @SuppressWarnings("deprecation")
    public void timeWindowsOfDeprecated() {
        TimeWindows w = TimeWindows.of(Duration.ofMinutes(1));
    }

    // RULE: STREAMS_JOIN_WINDOWS_OF_DEPRECATED — KIP-633 forces explicit grace period via ofTimeDifferenceWithNoGrace.
    @SuppressWarnings("deprecation")
    public void joinWindowsOfDeprecated() {
        JoinWindows w = JoinWindows.of(Duration.ofMinutes(1));
    }

    // RULE: STREAMS_GLOBAL_CONSUMER_AUTO_OFFSET_RESET_LATEST.
    public Properties globalConsumerAutoOffsetResetLatest() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("global.consumer.auto.offset.reset", "latest");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_RESTORE_CONSUMER_AUTO_OFFSET_RESET_LATEST.
    public Properties restoreConsumerAutoOffsetResetLatest() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("restore.consumer.auto.offset.reset", "latest");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_DEFAULT_DSL_STORE_INMEMORY.
    public Properties defaultDslStoreInmemory() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("default.dsl.store", "in_memory");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_LOCAL_THREADS_METADATA_DEPRECATED — renamed to metadataForLocalThreads in 3.0.
    @SuppressWarnings("deprecation")
    public void localThreadsMetadataDeprecated() {
        StreamsBuilder b = new StreamsBuilder();
        KafkaStreams streams = new KafkaStreams(b.build(), replicationFactorOne());
        streams.localThreadsMetadata();
    }

    // RULE: STREAMS_COMMIT_INTERVAL_TOO_HIGH.
    public Properties commitIntervalTooHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("commit.interval.ms", "300000");
        p.put("replication.factor", "3");
        return p;
    }
}
