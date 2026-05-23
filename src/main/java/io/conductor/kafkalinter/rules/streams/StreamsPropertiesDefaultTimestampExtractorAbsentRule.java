package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires on a Kafka Streams .properties file
 * (identified by top-level {@code application.id}, excluding Connect
 * configs via {@code connector.class}) that does NOT set
 * {@code default.timestamp.extractor}.
 *
 * <p>Streams declares {@code default.timestamp.extractor} with default
 * {@code org.apache.kafka.streams.processor.FailOnInvalidTimestamp}
 * in {@code StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG}.
 * The default throws {@code StreamsException} on any record with
 * {@code timestamp=-1}, which crashes the StreamThread.
 *
 * <p>The default is right for Kafka-native producers writing to
 * LogAppendTime-typed topics, but wrong for replay tools, CDC,
 * mixed-version clusters, MirrorMaker2-mirrored topics, custom Connect
 * transforms that strip timestamps, IoT/sensor workloads, and any
 * event-sourcing payload where the event time is in a value field
 * (not the Kafka envelope).
 *
 * <p>INFO severity: the default is defensible for typical workloads
 * but is workload-dependent; the linter cannot infer the workload's
 * time semantics from .properties alone.
 *
 * <p>Sibling rule: {@code STREAMS_PROPERTIES_MAX_TASK_IDLE_MS_ABSENT}
 * (the multi-source-ordering companion — {@code max.task.idle.ms}
 * only makes sense in the context of how timestamps are extracted).
 */
public final class StreamsPropertiesDefaultTimestampExtractorAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEFAULT_TIMESTAMP_EXTRACTOR = "default.timestamp.extractor";

    private final Severity severity;

    public StreamsPropertiesDefaultTimestampExtractorAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_DEFAULT_TIMESTAMP_EXTRACTOR_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(DEFAULT_TIMESTAMP_EXTRACTOR))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_DEFAULT_TIMESTAMP_EXTRACTOR_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + DEFAULT_TIMESTAMP_EXTRACTOR, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + DEFAULT_TIMESTAMP_EXTRACTOR + "`. Streams defaults `"
                            + DEFAULT_TIMESTAMP_EXTRACTOR + "` to `org.apache."
                            + "kafka.streams.processor.FailOnInvalidTimestamp` "
                            + "(declared in `StreamsConfig.DEFAULT_TIMESTAMP_"
                            + "EXTRACTOR_CLASS_CONFIG` with `ConfigDef.Type."
                            + "CLASS`, importance MEDIUM). The default's "
                            + "contract: for every record polled from a source "
                            + "topic, the extractor returns `record.timestamp()` "
                            + "if `>= 0`; otherwise throws `StreamsException("
                            + "\"Input record has invalid (negative) "
                            + "timestamp\")`. The exception propagates through "
                            + "the StreamThread to the configured `Thread."
                            + "UncaughtExceptionHandler` (default `SHUTDOWN_"
                            + "CLIENT` — kills the entire KafkaStreams "
                            + "instance). The bug shape: a topology that works "
                            + "fine for months suddenly crashes when a single "
                            + "upstream record with `timestamp=-1` arrives — "
                            + "typically from a legacy producer that was never "
                            + "upgraded to the 0.10+ record format, a replay "
                            + "tool that re-publishes historical records "
                            + "preserving the original `-1` timestamps, "
                            + "MirrorMaker2 mirroring records from a remote "
                            + "cluster where producer versions are mixed, a "
                            + "Debezium CDC connector that doesn't have a "
                            + "reliable transaction commit-time and emits "
                            + "records with `timestamp=-1`, or a custom "
                            + "Connect transform that explicitly sets "
                            + "`timestamp=null`. Specific bug shapes: (a) "
                            + "**MirrorMaker2 mirroring records from older "
                            + "cluster — record timestamps preserved as -1** — "
                            + "the Streams topology consumes mirrored records "
                            + "for months mostly with valid timestamps; one "
                            + "day a backfill replay tool re-publishes some "
                            + "old records to the source cluster; "
                            + "MirrorMaker2 mirrors them; the Streams "
                            + "topology receives a record with `timestamp=-1`; "
                            + "`FailOnInvalidTimestamp` throws "
                            + "`StreamsException`; the StreamThread dies; the "
                            + "topology is in CrashLoopBackOff until the "
                            + "operator manually skips the bad record. "
                            + "Setting `" + DEFAULT_TIMESTAMP_EXTRACTOR
                            + "=org.apache.kafka.streams.processor.LogAndSkip"
                            + "OnInvalidTimestamp` skips the bad record and "
                            + "alerts via metrics instead of crashing. (b) "
                            + "**Replay tool re-publishes historical archives "
                            + "— original -1 timestamps preserved** — operator "
                            + "runs a replay tool that re-publishes records "
                            + "from a long-term archive (S3 backup, separate "
                            + "cluster) to a 'replay' Kafka topic; the replay "
                            + "tool preserves the original record metadata, "
                            + "including `timestamp=-1` for any pre-0.10-"
                            + "format records; a downstream Streams topology "
                            + "consumes the replay topic and crashes on the "
                            + "first `-1` record. (c) **Debezium CDC pipeline "
                            + "— source DB doesn't have reliable transaction "
                            + "time** — Debezium's MySQL connector emits "
                            + "records with `record.timestamp = transaction_"
                            + "commit_ts`; for some configurations (e.g., "
                            + "row-based binlog without timestamps, MySQL "
                            + "`binlog_row_image=MINIMAL`), the commit time "
                            + "may be missing; Debezium falls back to `-1`; "
                            + "the downstream Streams topology crashes on the "
                            + "first missing-commit-time record. Fix: either "
                            + "upgrade the source DB to log commit times, OR "
                            + "set `" + DEFAULT_TIMESTAMP_EXTRACTOR + "` to a "
                            + "custom extractor that pulls the timestamp from "
                            + "the CDC payload's `source.ts_ms` field. (d) "
                            + "**IoT sensor data — sensor clock unsynced; "
                            + "producer sends timestamp=-1 to defer to "
                            + "broker** — an IoT deployment has thousands of "
                            + "embedded sensors; some don't have NTP and "
                            + "their clocks drift; the IoT-gateway producer "
                            + "passes `timestamp=null` to the ProducerRecord "
                            + "constructor; the broker's `message.timestamp."
                            + "type=CreateTime` preserves `null` as `-1`; the "
                            + "downstream Streams topology crashes on every "
                            + "record. Fix: either change the topic's "
                            + "`message.timestamp.type=LogAppendTime`, OR "
                            + "set `" + DEFAULT_TIMESTAMP_EXTRACTOR + "=org."
                            + "apache.kafka.streams.processor.WallclockTime"
                            + "stampExtractor` (use consumer-side wall-clock "
                            + "as the event time). (e) **Custom Connect "
                            + "transform setting timestamp=null** — a custom "
                            + "Single Message Transform strips the record "
                            + "timestamp (perhaps for compliance — 'don't "
                            + "expose producer-side wall-clock'); the "
                            + "transform outputs records with `timestamp="
                            + "null`; downstream Streams crashes. (f) "
                            + "**Schema-registry-driven Avro/Protobuf payload "
                            + "contains explicit event-time field; Kafka "
                            + "envelope timestamp is the broker arrival "
                            + "time** — most event-sourcing systems carry an "
                            + "explicit `event_time` field in the value; the "
                            + "Kafka envelope timestamp is the broker arrival "
                            + "time which may differ from the event time by "
                            + "seconds, minutes, or hours; the Streams "
                            + "topology's windowing should use the event_time "
                            + "field, NOT the envelope timestamp; without "
                            + "setting `" + DEFAULT_TIMESTAMP_EXTRACTOR
                            + "`, the topology silently uses the envelope "
                            + "timestamp and the windowing is wrong — a "
                            + "1-hour tumbling window might contain records "
                            + "whose event_time spans 5 hours because of "
                            + "producer-side batching. The bug is silent: "
                            + "the topology works, but the analytics are "
                            + "wrong. (g) **Spring Boot Streams autoconfig "
                            + "— org-wide default leaks through** — Spring "
                            + "Boot's Streams autoconfiguration provides a "
                            + "`KafkaStreamsConfiguration` bean from "
                            + "`application.yml`; the default Spring "
                            + "template omits `" + DEFAULT_TIMESTAMP_EXTRACTOR
                            + "`; every Spring Boot Streams app inherits the "
                            + "framework default `FailOnInvalidTimestamp`. "
                            + "Fix: ONE LINE. For Kafka-native producers "
                            + "writing to LogAppendTime-typed topics: `"
                            + DEFAULT_TIMESTAMP_EXTRACTOR + "=org.apache."
                            + "kafka.streams.processor.FailOnInvalidTimestamp` "
                            + "(explicit default — documents the deliberate "
                            + "choice; insulates from future framework "
                            + "default changes). For replay / CDC / "
                            + "MirrorMaker2-mirrored / custom-transformed "
                            + "sources: `" + DEFAULT_TIMESTAMP_EXTRACTOR
                            + "=org.apache.kafka.streams.processor.LogAndSkip"
                            + "OnInvalidTimestamp` (skip-and-alert instead of "
                            + "crash). For windowing causality on "
                            + "intermittently-invalid timestamps: `"
                            + DEFAULT_TIMESTAMP_EXTRACTOR + "=org.apache."
                            + "kafka.streams.processor.UsePreviousTimeOn"
                            + "InvalidTimestamp` (returns the previously "
                            + "processed valid record's timestamp). For "
                            + "IoT/sensor or ingestion-time-windowed "
                            + "analytics: `" + DEFAULT_TIMESTAMP_EXTRACTOR
                            + "=org.apache.kafka.streams.processor.Wallclock"
                            + "TimestampExtractor` (broker arrival time IS "
                            + "the event time). For event-sourcing with "
                            + "explicit event_time field: implement a custom "
                            + "`TimestampExtractor` that pulls the timestamp "
                            + "from a field within the value. Sibling rule "
                            + "[[streams-properties-max-task-idle-ms-absent]] "
                            + "(the multi-source-ordering companion — `max."
                            + "task.idle.ms` only makes sense in the context "
                            + "of how timestamps are extracted)."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
