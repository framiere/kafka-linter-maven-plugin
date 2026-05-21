package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

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

    // RULE: STREAMS_PROCESSING_GUARANTEE_AT_LEAST_ONCE_EXPLICIT.
    public Properties processingGuaranteeAtLeastOnceExplicit() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put("processing.guarantee", "at_least_once");
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

    // RULE: STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_LOW.
    public Properties bufferedRecordsPerPartitionTooLow() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("buffered.records.per.partition", "10");
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

    // RULE: STREAMS_ACCEPTABLE_RECOVERY_LAG_ZERO.
    public Properties acceptableRecoveryLagZero() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("acceptable.recovery.lag", "0");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_LOG_AND_SKIP.
    public Properties defaultTimestampExtractorLogAndSkip() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("default.timestamp.extractor",
                "org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp");
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

    // RULE: STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_HIGH.
    public Properties probingRebalanceIntervalMsTooHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("probing.rebalance.interval.ms", "14400000");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_SESSION_WINDOWS_WITH_DEPRECATED — KIP-633 forces explicit grace period via ofInactivityGapWithNoGrace.
    @SuppressWarnings("deprecation")
    public void sessionWindowsWithDeprecated() {
        SessionWindows w = SessionWindows.with(Duration.ofMinutes(1));
    }

    // RULE: STREAMS_REPLICATION_FACTOR_BROKER_DEFAULT.
    public Properties replicationFactorBrokerDefault() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("replication.factor", "-1");
        return p;
    }

    // RULE: STREAMS_WINDOWSTORE_CHANGELOG_ADDITIONAL_RETENTION_MS_TOO_HIGH.
    public Properties windowstoreChangelogAdditionalRetentionMsTooHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("windowstore.changelog.additional.retention.ms", "2592000000"); // 30 days
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_LOW.
    public Properties repartitionPurgeIntervalMsTooLow() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("repartition.purge.interval.ms", "1000");
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_HIGH.
    public Properties repartitionPurgeIntervalMsTooHigh() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-pipeline-v1");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put("repartition.purge.interval.ms", "600000"); // 10 min — well above the 5-min ceiling
        p.put("replication.factor", "3");
        return p;
    }

    // RULE: STREAMS_MATERIALIZED_WITH_LOGGING_DISABLED.
    public Materialized<String, Long, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>> materializedWithLoggingDisabled() {
        return Materialized.<String, Long, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("counts-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
                .withLoggingDisabled(); // no changelog topic → state lost on rebalance
    }

    // RULE: STREAMS_STOREBUILDER_WITH_LOGGING_DISABLED.
    public StoreBuilder<KeyValueStore<String, Long>> storeBuilderWithLoggingDisabled() {
        return Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("processor-store"),
                        Serdes.String(),
                        Serdes.Long())
                .withLoggingDisabled(); // Processor-API equivalent — no changelog, no fault tolerance
    }

    // RULE: STREAMS_CLOSE_NO_TIMEOUT.
    public void shutdownWithoutTimeout(KafkaStreams streams) {
        // No-argument close() — blocks for Long.MAX_VALUE waiting for every StreamThread.
        streams.close();
    }

    // RULE: STREAMS_REMOVE_THREAD_NO_TIMEOUT.
    public java.util.Optional<String> scaleDownWithoutTimeout(KafkaStreams streams) {
        // No-argument removeStreamThread() — blocks for Long.MAX_VALUE waiting for the thread to drain.
        return streams.removeStreamThread();
    }

    // RULE: STREAMS_SET_UNCAUGHT_EXCEPTION_HANDLER_LEGACY_DEPRECATED.
    public void wireLegacyExceptionHandler(KafkaStreams streams) {
        // Deprecated Thread.UncaughtExceptionHandler overload — cannot REPLACE_THREAD.
        streams.setUncaughtExceptionHandler((Thread t, Throwable e) ->
                System.err.println("Stream thread " + t.getName() + " died: " + e));
    }

    // RULE: STREAMS_REPARTITION_NO_NAMED.
    public Topology unnamedRepartition() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> in = b.stream("in");
        // No Repartitioned argument — topic name auto-generated from graph index.
        in.repartition().to("out");
        return b.build();
    }

    // RULE: STREAMS_GROUP_BY_NO_GROUPED.
    public Topology unnamedGroupBy() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> in = b.stream("in");
        // groupBy(KeyValueMapper) — no Grouped, auto-named repartition topic.
        in.groupBy((k, v) -> v).count().toStream().to("out");
        return b.build();
    }

    // RULE: STREAMS_SUPPRESS_BUFFER_UNBOUNDED.
    public Topology unboundedSuppressBuffer() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> in = b.stream("in");
        // BufferConfig.unbounded() — buffer grows until JVM heap exhaustion.
        in.groupByKey()
          .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
          .count(Materialized.with(Serdes.String(), Serdes.Long()))
          .suppress(org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses(
                  org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded()))
          .toStream()
          .to("out");
        return b.build();
    }

    // RULE: STREAMS_COUNT_NO_MATERIALIZED — count() with no Materialized → auto-named state store.
    public Topology unmaterializedCount() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> in = b.stream("in");
        in.groupByKey(org.apache.kafka.streams.kstream.Grouped.with("count-rep", Serdes.String(), Serdes.String()))
          .count()
          .toStream()
          .to("out");
        return b.build();
    }

    // RULE: STREAMS_AGGREGATE_NO_MATERIALIZED — aggregate() with no Materialized → auto-named state store.
    public Topology unmaterializedAggregate() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> in = b.stream("in");
        in.groupByKey(org.apache.kafka.streams.kstream.Grouped.with("agg-rep", Serdes.String(), Serdes.String()))
          .aggregate(() -> "", (k, v, agg) -> agg + v)
          .toStream()
          .to("out");
        return b.build();
    }

    // RULE: STREAMS_REDUCE_NO_MATERIALIZED — reduce() with no Materialized → auto-named state store.
    public Topology unmaterializedReduce() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> in = b.stream("in");
        in.groupByKey(org.apache.kafka.streams.kstream.Grouped.with("red-rep", Serdes.String(), Serdes.String()))
          .reduce((a, b2) -> a + b2)
          .toStream()
          .to("out");
        return b.build();
    }

    // RULE: STREAMS_STREAM_JOIN_NO_NAMED — join() with no StreamJoined → auto-named state stores.
    public Topology unnamedStreamJoin() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> a = b.stream("a");
        KStream<String, String> c = b.stream("c");
        ValueJoiner<String, String, String> joiner = (x, y) -> x + "/" + y;
        a.join(c, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)))
         .to("out");
        return b.build();
    }

    // RULE: STREAMS_KTABLE_GROUP_BY_NO_GROUPED — KTable.groupBy(KeyValueMapper) no-Grouped → auto-named repartition.
    public Topology unnamedKTableGroupBy() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> t =
                b.table("in", Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("t-store"));
        t.groupBy((k, v) -> KeyValue.pair(v, k))
         .count(Materialized.as("ktable-grouped-count-store"))
         .toStream()
         .to("out");
        return b.build();
    }

    // RULE: STREAMS_TABLE_NO_MATERIALIZED — table(topic) with no Materialized → auto-named store and changelog.
    public Topology tableNoMaterialized() {
        StreamsBuilder b = new StreamsBuilder();
        // No Materialized — store and changelog topic names are graph-index-derived.
        org.apache.kafka.streams.kstream.KTable<String, String> users = b.table("users");
        users.toStream().to("users-out");
        return b.build();
    }

    // RULE: STREAMS_GLOBAL_TABLE_NO_MATERIALIZED — globalTable(topic) with no Materialized → auto-named global store.
    public Topology globalTableNoMaterialized() {
        StreamsBuilder b = new StreamsBuilder();
        // No Materialized — global store name is graph-index-derived; restoration is empty after a topology edit.
        org.apache.kafka.streams.kstream.GlobalKTable<String, String> users = b.globalTable("users");
        // Reference 'users' to silence unused warnings while still exercising globalTable() bytecode.
        if (users == null) {
            throw new IllegalStateException();
        }
        return b.build();
    }

    // RULE: STREAMS_FOREIGN_KEY_JOIN_NO_MATERIALIZED — KTable FK join with no Materialized → 4 auto-named artifacts.
    public Topology foreignKeyJoinNoMaterialized() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> accounts =
                b.table("accounts", Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("accounts-store"));
        org.apache.kafka.streams.kstream.KTable<String, String> transactions =
                b.table("transactions", Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("transactions-store"));
        // No Materialized argument — subscription store, response store, subscription topic, response topic
        // are ALL graph-index-derived.
        accounts.join(transactions, (java.util.function.Function<String, String>) v -> v, (a, t) -> a + "|" + t)
                .toStream()
                .to("enriched");
        return b.build();
    }

    // RULE: STREAMS_SELECT_KEY_NO_NAMED — selectKey with no Named → auto-named processor; downstream repartition propagates.
    public Topology selectKeyNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("events")
                .selectKey((k, v) -> v)
                .groupByKey(org.apache.kafka.streams.kstream.Grouped.as("by-account"))
                .count(Materialized.as("events-per-account-store"))
                .toStream()
                .to("counts");
        return b.build();
    }

    // RULE: STREAMS_MERGE_NO_NAMED — merge with no Named → auto-named merge node breaks per-node metric labels.
    public Topology mergeNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> orders = b.stream("orders");
        KStream<String, String> refunds = b.stream("refunds");
        orders.merge(refunds).to("ledger");
        return b.build();
    }

    // RULE: STREAMS_TO_TABLE_NO_MATERIALIZED — KStream.toTable() with no Materialized;
    // resulting KTable's state store and changelog topic are auto-named by graph index.
    public Topology toTableNoMaterialized() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> events = b.stream("events");
        org.apache.kafka.streams.kstream.KTable<String, String> table = events.toTable();
        table.toStream().to("events-table-out");
        return b.build();
    }

    // RULE: STREAMS_PROCESS_NO_NAMED — KStream.process(ProcessorSupplier) without Named (KIP-820 replacement).
    public Topology processNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> events = b.stream("events");
        events.process(() -> new org.apache.kafka.streams.processor.api.Processor<String, String, String, String>() {
            private org.apache.kafka.streams.processor.api.ProcessorContext<String, String> ctx;
            @Override public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(org.apache.kafka.streams.processor.api.Record<String, String> rec) { ctx.forward(rec); }
        });
        return b.build();
    }

    // RULE: STREAMS_GROUP_BY_KEY_NO_GROUPED — groupByKey() with no Grouped after an upstream re-keying.
    public Topology groupByKeyNoGrouped() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> events = b.stream("events");
        events.map((k, v) -> KeyValue.pair(v, v))
              .groupByKey()
              .count(Materialized.as("events-per-key"))
              .toStream()
              .to("counts");
        return b.build();
    }

    // RULE: STREAMS_SPLIT_NO_NAMED — split() with no Named, prefix of branch map keys.
    public Topology splitNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> events = b.stream("events");
        java.util.Map<String, KStream<String, String>> branches = events
                .split()
                .branch((k, v) -> v != null, org.apache.kafka.streams.kstream.Branched.as("non-null"))
                .defaultBranch();
        if (branches.isEmpty()) { throw new IllegalStateException(); }
        return b.build();
    }

    // RULE: STREAMS_BRANCHED_NO_NAMED — split().branch(predicate) with no Branched.as(...).
    public Topology branchedNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> orders = b.stream("orders");
        java.util.Map<String, KStream<String, String>> branches = orders
                .split()
                .branch((k, v) -> v != null && v.length() > 5)
                .branch((k, v) -> v != null && v.length() <= 5)
                .defaultBranch();
        if (branches.isEmpty()) { throw new IllegalStateException(); }
        return b.build();
    }

    // RULE: STREAMS_STREAM_NO_CONSUMED — StreamsBuilder.stream(topic) with no Consumed.
    public Topology streamNoConsumed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("raw-events");
        s.foreach((k, v) -> { if (v == null) { throw new IllegalStateException(); } });
        return b.build();
    }

    // RULE: STREAMS_TO_NO_PRODUCED — KStream.to(topic) with no Produced.
    public Topology toNoProduced() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("raw-events",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("raw-events-source"));
        s.to("raw-events-out");
        return b.build();
    }

    // RULE: STREAMS_KTABLE_FILTER_NO_NAMED — KTable.filter without Named, name graph-index-derived.
    public Topology ktableFilterNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> users = b.table("users",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("users-source"));
        users.filter((k, v) -> v != null && v.length() > 3)
                .toStream(org.apache.kafka.streams.kstream.Named.as("filtered-to-stream"))
                .to("filtered-users",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("filtered-sink"));
        return b.build();
    }

    // RULE: STREAMS_KTABLE_MAP_VALUES_NO_NAMED — KTable.mapValues without Named, name graph-index-derived.
    public Topology ktableMapValuesNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> users = b.table("users",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("users-source"));
        users.mapValues(v -> v == null ? "" : v.toUpperCase())
                .toStream(org.apache.kafka.streams.kstream.Named.as("upper-to-stream"))
                .to("users-upper",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("upper-sink"));
        return b.build();
    }

    // RULE: STREAMS_KTABLE_JOIN_NO_NAMED — KTable.join with neither Named nor Materialized, store + changelog graph-index-derived.
    public Topology ktableJoinNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> left = b.table("users",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("users-source"),
                org.apache.kafka.streams.kstream.Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("users-store"));
        org.apache.kafka.streams.kstream.KTable<String, String> right = b.table("orders",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("orders-source"),
                org.apache.kafka.streams.kstream.Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("orders-store"));
        left.join(right, (u, o) -> u + "|" + o)
                .toStream(org.apache.kafka.streams.kstream.Named.as("joined-to-stream"))
                .to("users-orders",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("joined-sink"));
        return b.build();
    }

    // RULE: STREAMS_MAP_NO_NAMED — KStream.map without Named, key-changing → auto-repartition graph-index-derived.
    public Topology mapNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("raw-events",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("raw-events-source"));
        s.map((k, v) -> org.apache.kafka.streams.KeyValue.pair(v == null ? "null-key" : v.substring(0, 1), v))
                .to("rekeyed-events",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("rekeyed-sink"));
        return b.build();
    }

    // RULE: STREAMS_FLAT_MAP_VALUES_NO_NAMED — KStream.flatMapValues without Named, fan-out but not key-changing.
    public Topology flatMapValuesNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("pageviews",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("pageviews-source"));
        s.flatMapValues(v -> {
            java.util.List<String> out = new java.util.ArrayList<>();
            if (v != null) {
                for (String event : v.split(",")) {
                    out.add(event.trim());
                }
            }
            return out;
        }).to("events-expanded",
                org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("events-sink"));
        return b.build();
    }

    // RULE: STREAMS_FLAT_MAP_NO_NAMED — KStream.flatMap without Named, key-changing + fan-out → amplified auto-repartition.
    public Topology flatMapNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("documents",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("documents-source"));
        s.flatMap((k, v) -> {
            java.util.List<org.apache.kafka.streams.KeyValue<String, String>> out = new java.util.ArrayList<>();
            if (v != null) {
                for (String term : v.split("\\s+")) {
                    out.add(org.apache.kafka.streams.KeyValue.pair(term, k));
                }
            }
            return out;
        }).to("terms-index",
                org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("terms-sink"));
        return b.build();
    }

    // RULE: STREAMS_PEEK_NO_NAMED — KStream.peek without Named.
    public Topology peekNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("raw-events",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("raw-events-source"));
        s.peek((k, v) -> System.out.println("debug: " + k + " -> " + v))
                .to("raw-events-mirror",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("mirror-sink"));
        return b.build();
    }

    // RULE: STREAMS_KSTREAM_FILTER_NO_NAMED — KStream.filter/filterNot without Named, name graph-index-derived.
    public Topology kstreamFilterNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("raw-events",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("raw-events-source"));
        s.filter((k, v) -> v != null && !v.isEmpty())
                .filterNot((k, v) -> v.startsWith("ignore-"))
                .to("filtered-events",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("filtered-sink"));
        return b.build();
    }

    // RULE: STREAMS_KSTREAM_MAP_VALUES_NO_NAMED — KStream.mapValues without Named, name graph-index-derived.
    public Topology kstreamMapValuesNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("raw-events",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("raw-events-source"));
        s.mapValues(v -> v == null ? "" : v.toUpperCase())
                .to("upper-events",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("upper-sink"));
        return b.build();
    }

    // RULE: STREAMS_COUNT_NO_NAMED — KGroupedStream.count() with no Named, state-store/changelog auto-named.
    public Topology countNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("raw-events",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("raw-events-source"));
        s.groupByKey(org.apache.kafka.streams.kstream.Grouped.with("by-key", Serdes.String(), Serdes.String()))
                .count()
                .toStream(org.apache.kafka.streams.kstream.Named.as("count-to-stream"))
                .to("counted-events",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.Long()).withName("counted-sink"));
        return b.build();
    }

    // RULE: STREAMS_REDUCE_NO_NAMED — KGroupedStream.reduce(Reducer) with no Named.
    public Topology reduceNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("raw-events",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("raw-events-source"));
        s.groupByKey(org.apache.kafka.streams.kstream.Grouped.with("by-key", Serdes.String(), Serdes.String()))
                .reduce((a, bb) -> a.length() >= bb.length() ? a : bb)
                .toStream(org.apache.kafka.streams.kstream.Named.as("reduce-to-stream"))
                .to("reduced-events",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("reduced-sink"));
        return b.build();
    }

    // RULE: STREAMS_AGGREGATE_NO_NAMED — KGroupedStream.aggregate(Initializer, Aggregator) with no Named.
    public Topology aggregateNoNamed() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.stream("raw-events",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("raw-events-source"));
        s.groupByKey(org.apache.kafka.streams.kstream.Grouped.with("by-key", Serdes.String(), Serdes.String()))
                .aggregate(() -> 0L, (k, v, agg) -> agg + (v == null ? 0L : v.length()),
                        Materialized.<String, Long, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("agg-store")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .toStream(org.apache.kafka.streams.kstream.Named.as("agg-to-stream"))
                .to("aggregated-events",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.Long()).withName("aggregated-sink"));
        return b.build();
    }

    // RULE: STREAMS_IN_MEMORY_KV_STORE — Stores.inMemoryKeyValueStore/inMemoryWindowStore/inMemorySessionStore.
    public Topology inMemoryKeyValueStore() {
        StreamsBuilder b = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, String>> kvBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("ephemeral-kv-store"),
                Serdes.String(), Serdes.String());
        b.addStateStore(kvBuilder);
        // also exercise the windowed and session in-memory variants
        Stores.inMemoryWindowStore("ephemeral-window-store", Duration.ofHours(1), Duration.ofMinutes(5), false);
        Stores.inMemorySessionStore("ephemeral-session-store", Duration.ofHours(1));
        return b.build();
    }

    // RULE: STREAMS_BUILDER_BUILD_NO_PROPERTIES — StreamsBuilder.build() with no Properties → topology.optimization ignored.
    public Topology builderBuildNoProperties() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("input",
                        org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("input-source"))
                .to("output", org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("output-sink"));
        // Bug: no-args build() — REUSE_KTABLE_SOURCE_TOPICS, MERGE_REPARTITION_TOPICS, and every other
        // topology.optimization rewrite is silently skipped. The accompanying Properties (with
        // topology.optimization=all) has no effect because it is never passed in.
        return b.build();
    }

    // RULE: STREAMS_FOREIGN_KEY_JOIN_NO_TABLE_JOINED — KTable.join FK without TableJoined.
    public Topology foreignKeyJoinNoTableJoined() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> accounts =
                b.table("accounts-fk-no-tj",
                        Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("accounts-fk-no-tj-store"));
        org.apache.kafka.streams.kstream.KTable<String, String> transactions =
                b.table("transactions-fk-no-tj",
                        Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("transactions-fk-no-tj-store"));
        // Materialized is present (would silence the no-Materialized rule) — but TableJoined is missing,
        // so the subscription registration AND subscription response internal topics are graph-index-derived.
        accounts.join(transactions, (java.util.function.Function<String, String>) v -> v, (a, t) -> a + "|" + t,
                        Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("fk-result-store"))
                .toStream(org.apache.kafka.streams.kstream.Named.as("fk-no-tj-to-stream"))
                .to("enriched-fk-no-tj",
                        org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()).withName("enriched-fk-no-tj-sink"));
        return b.build();
    }

    // RULE: STREAMS_ALL_METADATA_FOR_STORE_DEPRECATED — KafkaStreams.allMetadataForStore()/allMetadata() deprecated (KIP-744, Kafka 3.0).
    public void allMetadataForStoreDeprecated(KafkaStreams streams) {
        // Both forms are flagged: post-KIP-744 they return the OLD-package StreamsMetadata,
        // which silently elides standby-replica info — IQ routers cannot fall over to a standby
        // while the active is restoring. Use streamsMetadataForStore() / metadataForAllStreamsClients().
        java.util.Collection<org.apache.kafka.streams.state.StreamsMetadata> perStore =
                streams.allMetadataForStore("my-store");
        java.util.Collection<org.apache.kafka.streams.state.StreamsMetadata> all =
                streams.allMetadata();
        if (perStore != null && all != null) {
            perStore.size();
            all.size();
        }
    }

    // RULE: STREAMS_WINDOWS_GRACE_DEPRECATED — TimeWindows/JoinWindows/SessionWindows.grace() chained instance method deprecated (KIP-633, Kafka 3.0).
    public Topology windowsGraceDeprecated() {
        StreamsBuilder b = new StreamsBuilder();
        // Bug 1: TimeWindows.ofSizeWithNoGrace(...).grace(...) — the chained .grace() is deprecated AND throws
        // IllegalStateException at runtime because the new factory marks grace-as-set.
        TimeWindows tw = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).grace(Duration.ofMinutes(1));
        // Bug 2: JoinWindows.ofTimeDifferenceWithNoGrace(...).grace(...) — same deprecated chain.
        JoinWindows jw = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)).grace(Duration.ofSeconds(30));
        // Bug 3: SessionWindows.ofInactivityGapWithNoGrace(...).grace(...) — same deprecated chain.
        SessionWindows sw = SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)).grace(Duration.ofSeconds(10));
        b.<String, String>stream("input-windows-grace",
                        org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("input-windows-grace-source"))
                .groupByKey(org.apache.kafka.streams.kstream.Grouped.with("windows-grace-grouped", Serdes.String(), Serdes.String()))
                .windowedBy(tw)
                .count(org.apache.kafka.streams.kstream.Named.as("windows-grace-count"),
                        Materialized.<String, Long, org.apache.kafka.streams.state.WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("windows-grace-store"));
        // Reference jw and sw so the compiler does not optimize them away.
        if (jw.gracePeriodMs() < 0 || sw.gracePeriodMs() < 0) {
            throw new IllegalStateException();
        }
        return b.build();
    }

    // RULE: STREAMS_LEGACY_PROCESSOR_API_DEPRECATED — Topology.addProcessor / addGlobalStore / StreamsBuilder.addGlobalStore
    // taking the legacy org.apache.kafka.streams.processor.ProcessorSupplier (pre-KIP-820, deprecated since Kafka 3.3).
    public Topology legacyProcessorApiDeprecated() {
        StreamsBuilder b = new StreamsBuilder();
        Topology t = b.build();
        // Bug 1: Topology.addProcessor with LEGACY ProcessorSupplier from org.apache.kafka.streams.processor.
        // The legacy Processor<K, V> interface has process(K, V) and ProcessorContext.forward(K, V) — replaced by
        // org.apache.kafka.streams.processor.api.ProcessorSupplier returning Processor<KIn, VIn, KOut, VOut>
        // with process(Record<KIn, VIn>) and typed ProcessorContext<KOut, VOut>.
        org.apache.kafka.streams.processor.ProcessorSupplier<String, String> legacySupplier =
                () -> new org.apache.kafka.streams.processor.AbstractProcessor<String, String>() {
                    @Override public void process(String key, String value) { /* legacy API */ }
                };
        t.addSource("legacy-src", "legacy-input");
        t.addProcessor("legacy-proc", legacySupplier, "legacy-src");
        // Bug 2: StreamsBuilder.addGlobalStore(StoreBuilder, String, Consumed, ProcessorSupplier) with legacy supplier.
        StoreBuilder<KeyValueStore<String, String>> globalStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("legacy-global-store"),
                        Serdes.String(), Serdes.String());
        b.addGlobalStore(globalStoreBuilder, "legacy-global-input",
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()).withName("legacy-global-source"),
                legacySupplier);
        // Bug 3: Topology.addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier).
        StoreBuilder<KeyValueStore<String, String>> globalStoreBuilder2 =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("legacy-global-store-2"),
                        Serdes.String(), Serdes.String());
        t.addGlobalStore(globalStoreBuilder2, "legacy-global-source-2",
                Serdes.String().deserializer(), Serdes.String().deserializer(),
                "legacy-global-input-2", "legacy-global-proc-2", legacySupplier);
        return t;
    }

}
