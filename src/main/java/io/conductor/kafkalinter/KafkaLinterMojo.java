package io.conductor.kafkalinter;

import io.conductor.kafkalinter.report.Reporter;
import io.conductor.kafkalinter.rules.ConsumerAutoCommitTrueRule;
import io.conductor.kafkalinter.rules.ConsumerCommitPerRecordRule;
import io.conductor.kafkalinter.rules.ConsumerPollZeroRule;
import io.conductor.kafkalinter.rules.ProducerFlushInLoopRule;
import io.conductor.kafkalinter.rules.ProducerInLoopRule;
import io.conductor.kafkalinter.rules.ProducerNoCompressionRule;
import io.conductor.kafkalinter.rules.ProducerSendBlockingGetRule;
import io.conductor.kafkalinter.rules.ProducerSendNoCallbackRule;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.rules.clients.ConsumerAssignAndSubscribeRule;
import io.conductor.kafkalinter.rules.clients.KafkaClientTypoGroupIdRule;
import io.conductor.kafkalinter.rules.clients.ProducerMaxInFlightTooHighRule;
import io.conductor.kafkalinter.rules.clients.ProducerTxnIdWithoutIdempotenceRule;
import io.conductor.kafkalinter.rules.config.ConfigKeyValueRule;
import io.conductor.kafkalinter.rules.config.MethodCallRule;
import io.conductor.kafkalinter.rules.config.PropertyFileRule;
import io.conductor.kafkalinter.rules.observability.JacksonDefaultTypingRule;
import io.conductor.kafkalinter.rules.observability.SchemaRegistryUrlMissingRule;
import io.conductor.kafkalinter.rules.quarkus.QkBlockingMissingOnIncomingRule;
import io.conductor.kafkalinter.rules.quarkus.QkDevservicesInProdRule;
import io.conductor.kafkalinter.rules.quarkus.SmallRyeChannelConfigRule;
import io.conductor.kafkalinter.rules.spring.SpringErrorHandlingDeserializerNoDelegatesRule;
import io.conductor.kafkalinter.rules.spring.SpringListenerAsyncRule;
import io.conductor.kafkalinter.rules.version.JavaVersionTooLowRule;
import io.conductor.kafkalinter.rules.version.KafkaClientsCveJndiLdapRule;
import io.conductor.kafkalinter.rules.version.KafkaClientsCveRule;
import io.conductor.kafkalinter.rules.version.KafkaClientsCveSaslOAuthRule;
import io.conductor.kafkalinter.rules.version.KafkaClientsEolRule;
import io.conductor.kafkalinter.rules.version.SemVer;
import io.conductor.kafkalinter.rules.version.QuarkusKafkaExtensionRenamedRule;
import io.conductor.kafkalinter.rules.version.SpringKafkaBootMismatchRule;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.ProjectContext;
import io.conductor.kafkalinter.scanner.ProjectRuleRunner;
import io.conductor.kafkalinter.scanner.ProjectScanner;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Mojo(name = "check", defaultPhase = LifecyclePhase.VERIFY, threadSafe = true,
      requiresDependencyResolution = org.apache.maven.plugins.annotations.ResolutionScope.COMPILE_PLUS_RUNTIME)
public class KafkaLinterMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    /** Directory of compiled classes to scan. Defaults to ${project.build.outputDirectory}. */
    @Parameter(property = "kafka-linter.classesDirectory",
               defaultValue = "${project.build.outputDirectory}")
    private String classesDirectory;

    /** Skip the check entirely. */
    @Parameter(property = "kafka-linter.skip", defaultValue = "false")
    private boolean skip;

    /**
     * Per-rule severity overrides. Keys are RuleId names (e.g. PRODUCER_IN_LOOP),
     * values are ERROR, WARNING, INFO, or OFF.
     */
    @Parameter
    private Map<String, String> severities = new HashMap<>();

    /** Reporter flavor: "simple" (default) or "verbose". */
    @Parameter(property = "kafka-linter.reporter", defaultValue = "simple")
    private String reporter;

    @Override
    public void execute() throws MojoFailureException {
        if (skip) {
            getLog().info("kafka-linter: skipped via configuration.");
            return;
        }

        Map<RuleId, Severity> resolved = resolveSeverities();
        List<Violation> violations = new ArrayList<>();

        Path classesDir = Paths.get(classesDirectory);
        if (classesDir.toFile().isDirectory()) {
            try {
                violations.addAll(new ProjectScanner(buildRules(resolved)).scanDirectory(classesDir));
            } catch (Exception e) {
                throw new MojoFailureException("kafka-linter: failed to scan " + classesDir, e);
            }
        } else {
            getLog().info("kafka-linter: no classes directory at " + classesDir + " — skipping bytecode scan.");
        }

        try {
            ProjectContext pctx = new ProjectContext(project);
            violations.addAll(new ProjectRuleRunner(buildProjectRules(resolved)).run(pctx));
        } catch (Exception e) {
            getLog().warn("kafka-linter: project-scoped rules failed: " + e.getMessage());
        }

        Reporter.of(reporter).report(violations, getLog());

        long errors = violations.stream().filter(v -> v.severity() == Severity.ERROR).count();
        if (errors > 0) {
            throw new MojoFailureException("kafka-linter: " + errors + " error-severity violation(s) found.");
        }
    }

    private Map<RuleId, Severity> resolveSeverities() {
        Map<RuleId, Severity> map = new LinkedHashMap<>();
        for (RuleId r : RuleId.values()) {
            map.put(r, r.defaultSeverity());
        }
        for (Map.Entry<String, String> e : severities.entrySet()) {
            try {
                RuleId id = RuleId.valueOf(e.getKey());
                Severity sev = Severity.valueOf(e.getValue().toUpperCase());
                map.put(id, sev);
            } catch (IllegalArgumentException ex) {
                getLog().warn("kafka-linter: unknown rule or severity in configuration: " + e.getKey() + "=" + e.getValue());
            }
        }
        return map;
    }

    private List<Rule> buildRules(Map<RuleId, Severity> sev) {
        List<Rule> rules = new ArrayList<>();

        // ── kafka-clients ──────────────────────────────────────────────────────
        addIfEnabled(rules, sev, RuleId.PRODUCER_IN_LOOP,
                s -> new ProducerInLoopRule(RuleId.PRODUCER_IN_LOOP, s, Set.of(KafkaTypes.KAFKA_PRODUCER)));
        addIfEnabled(rules, sev, RuleId.CONSUMER_IN_LOOP,
                s -> new ProducerInLoopRule(RuleId.CONSUMER_IN_LOOP, s, Set.of(KafkaTypes.KAFKA_CONSUMER)));
        addIfEnabled(rules, sev, RuleId.PRODUCER_NO_COMPRESSION, ProducerNoCompressionRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_SEND_BLOCKING_GET, ProducerSendBlockingGetRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_SEND_NO_CALLBACK, ProducerSendNoCallbackRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_FLUSH_IN_LOOP, ProducerFlushInLoopRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_AUTO_COMMIT_TRUE, ConsumerAutoCommitTrueRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_COMMIT_PER_RECORD, ConsumerCommitPerRecordRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_POLL_ZERO, ConsumerPollZeroRule::new);

        addIfEnabled(rules, sev, RuleId.PRODUCER_ACKS_ZERO, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_ACKS_ZERO, s, KafkaTypes.ACKS_KEY, "0",
                "acks=0 — producer does not wait for broker acknowledgement. Records may be silently lost on any broker hiccup."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_ACKS_ONE, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_ACKS_ONE, s, KafkaTypes.ACKS_KEY, "1",
                "acks=1 — only the partition leader has acknowledged. A leader failover before replication loses the record."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_RETRIES_ZERO, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_RETRIES_ZERO, s, KafkaTypes.RETRIES_KEY, "0",
                "retries=0 — transient broker errors become permanent send failures. The default Integer.MAX_VALUE + delivery.timeout.ms bound is almost always correct."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_COMPRESSION_NONE_EXPLICIT, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_COMPRESSION_NONE_EXPLICIT, s, KafkaTypes.COMPRESSION_TYPE_KEY, "none",
                "compression.type=none — explicitly opting out of compression. 3-5× more bytes on wire/disk than zstd or lz4."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_LINGER_ZERO_NO_BATCH, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_LINGER_ZERO_NO_BATCH, s, KafkaTypes.LINGER_MS_KEY, "0",
                "linger.ms=0 — sender thread sends every record immediately, no accumulator batching. One ProduceRequest per record under steady load."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_AUTO_OFFSET_RESET_LATEST, s -> ConfigKeyValueRule.literal(
                RuleId.CONSUMER_AUTO_OFFSET_RESET_LATEST, s, KafkaTypes.AUTO_OFFSET_RESET_KEY, "latest",
                "auto.offset.reset=latest — fresh consumer groups skip every record produced before they started. Prefer 'earliest' unless this is a heartbeat/health consumer."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE, s -> ConfigKeyValueRule.literal(
                RuleId.CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE, s, KafkaTypes.ALLOW_AUTO_CREATE_TOPICS_KEY, "true",
                "allow.auto.create.topics=true — a typo can permanently create a one-partition, default-RF topic."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_MAX_POLL_RECORDS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_MAX_POLL_RECORDS_TOO_HIGH, s, KafkaTypes.MAX_POLL_RECORDS_KEY,
                v -> parseIntOrZero(v) > 1000,
                "max.poll.records={value} — a batch this size will not fit inside the default max.poll.interval.ms for any non-trivial processing cost. Rebalance storm risk."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL, s, KafkaTypes.DELIVERY_TIMEOUT_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 30000; },
                "delivery.timeout.ms={value} — shorter than 30s leaves no headroom for routine broker leader-elections. The producer will fail records that would have been delivered by retry."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_IDEMPOTENCE_DISABLED, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_IDEMPOTENCE_DISABLED, s, KafkaTypes.ENABLE_IDEMPOTENCE_KEY, "false",
                "enable.idempotence=false — explicit opt-out of the default (true since Kafka 3.0). Reintroduces duplicate / out-of-order writes on retry."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_BUFFER_MEMORY_TOO_SMALL, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_BUFFER_MEMORY_TOO_SMALL, s, KafkaTypes.BUFFER_MEMORY_KEY,
                v -> { long n = parseLongOrZero(v); return n > 0 && n < 16_777_216L; },
                "buffer.memory={value} — below 16 MiB. Sender thread cannot keep up with bursts; producer.send() will block on the hot path."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_REQUEST_TIMEOUT_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_REQUEST_TIMEOUT_TOO_LOW, s, KafkaTypes.REQUEST_TIMEOUT_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 10000; },
                "request.timeout.ms={value} — below 10 s. Routine cross-AZ produce times burn through retries; bound the application call instead."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_SESSION_TIMEOUT_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_SESSION_TIMEOUT_TOO_LOW, s, KafkaTypes.SESSION_TIMEOUT_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 10000; },
                "session.timeout.ms={value} — below 10 s. Routine GC pauses will trigger spurious rebalances. Default 45 s is almost always right."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_MAX_POLL_INTERVAL_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_MAX_POLL_INTERVAL_MS_TOO_LOW, s, KafkaTypes.MAX_POLL_INTERVAL_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 60000; },
                "max.poll.interval.ms={value} — below 60 s. Any single poll cycle that overruns this triggers a rebalance; the consumer enters a re-poll/re-evict loop."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_BATCH_SIZE_TOO_SMALL, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_BATCH_SIZE_TOO_SMALL, s, KafkaTypes.BATCH_SIZE_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Integer.parseInt(v.trim()) < 16384; }
                    catch (NumberFormatException e) { return false; }
                },
                "batch.size={value} — below the 16384-byte default. The producer sends one ProduceRequest per few records; broker request rate explodes."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_LINGER_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_LINGER_MS_TOO_HIGH, s, KafkaTypes.LINGER_MS_KEY,
                v -> parseIntOrZero(v) > 60000,
                "linger.ms={value} — above 60 s. Every record sits in the accumulator that long before send; almost certainly a units/typo mistake."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_PARTITIONER_CLASS_DEPRECATED, s -> ConfigKeyValueRule.literalAny(
                RuleId.PRODUCER_PARTITIONER_CLASS_DEPRECATED, s, KafkaTypes.PARTITIONER_CLASS_KEY,
                KafkaTypes.PARTITIONER_DEPRECATED_FQCNS,
                "partitioner.class={value} — deprecated by KIP-794. Delete this line; the built-in strategy (queue+RTT aware) is strictly better."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_FETCH_MAX_BYTES_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_FETCH_MAX_BYTES_TOO_LOW, s, KafkaTypes.FETCH_MAX_BYTES_KEY,
                v -> { long n = parseLongOrZero(v); return n > 0 && n < 1_048_576L; },
                "fetch.max.bytes={value} — below 1 MiB. The consumer cannot pull a single max-sized batch; FetchRequest rate explodes and effective throughput is capped at ~ value/RTT."));
        addIfEnabled(rules, sev, RuleId.STREAMS_APPLICATION_ID_GENERIC, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_APPLICATION_ID_GENERIC, s, KafkaTypes.STREAMS_APPLICATION_ID_KEY,
                v -> v != null && KafkaTypes.STREAMS_GENERIC_APPLICATION_IDS.contains(v.trim().toLowerCase()),
                "application.id={value} — a generic placeholder. Two apps with this id will collide on consumer-group, changelogs, and state. Use service-name-vN."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_CHECK_CRCS_FALSE, s -> ConfigKeyValueRule.literal(
                RuleId.CONSUMER_CHECK_CRCS_FALSE, s, KafkaTypes.CHECK_CRCS_KEY, "false",
                "check.crcs=false — consumer accepts records without verifying the on-the-wire CRC. Corrupt records reach the application as valid."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_MAX_BLOCK_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_MAX_BLOCK_MS_TOO_LOW, s, KafkaTypes.MAX_BLOCK_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 10000; },
                "max.block.ms={value} — below 10 s. Routine metadata fetches bubble up as TimeoutException from send(); bound the upstream call instead."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_LOW, s, KafkaTypes.DEFAULT_API_TIMEOUT_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 30000; },
                "default.api.timeout.ms={value} — below 30 s. Routine commitSync/position/metadata calls bubble up TimeoutException on normal broker hiccups."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_GROUP_ID_GENERIC, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_GROUP_ID_GENERIC, s, KafkaTypes.GROUP_ID_KEY,
                v -> v != null && KafkaTypes.CONSUMER_GENERIC_GROUP_IDS.contains(v.trim().toLowerCase()),
                "group.id={value} — a generic placeholder. Two apps with this group.id will collide on partition assignment and offset commits."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_DELIVERY_TIMEOUT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_DELIVERY_TIMEOUT_MS_TOO_HIGH, s, KafkaTypes.DELIVERY_TIMEOUT_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 600_000L; },
                "delivery.timeout.ms={value} — above 10 minutes. Stuck records occupy buffer.memory for that long while the application can't see the failure."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_FETCH_MIN_BYTES_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_FETCH_MIN_BYTES_TOO_HIGH, s, KafkaTypes.FETCH_MIN_BYTES_KEY,
                v -> { long n = parseLongOrZero(v); return n > 10_485_760L; },
                "fetch.min.bytes={value} — above 10 MiB. Every poll waits up to fetch.max.wait.ms for that much data to accumulate; latency cliff."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_LOW, s, KafkaTypes.HEARTBEAT_INTERVAL_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 1000; },
                "heartbeat.interval.ms={value} — below 1 s. Floods the group coordinator with no failure-detection upside; tune session.timeout.ms if you need faster eviction."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_RECONNECT_BACKOFF_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_RECONNECT_BACKOFF_MS_TOO_LOW, s, KafkaTypes.RECONNECT_BACKOFF_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Integer.parseInt(v.trim()) < 100; }
                    catch (NumberFormatException e) { return false; }
                },
                "reconnect.backoff.ms={value} — below 100 ms. Broker outage becomes a tight reconnect loop; raise this, don't lower it."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_RETRY_BACKOFF_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_RETRY_BACKOFF_MS_TOO_LOW, s, KafkaTypes.RETRY_BACKOFF_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Integer.parseInt(v.trim()) < 50; }
                    catch (NumberFormatException e) { return false; }
                },
                "retry.backoff.ms={value} — below 50 ms. Producer retries pound the broker before it can recover; raise this, don't lower it."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_HIGH, s, KafkaTypes.AUTO_COMMIT_INTERVAL_MS_KEY,
                v -> parseIntOrZero(v) > 60000,
                "auto.commit.interval.ms={value} — above 60 s. Combined with enable.auto.commit=true, that's a 60 s+ duplicate-window after every crash or rebalance."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_MAX_REQUEST_SIZE_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_MAX_REQUEST_SIZE_TOO_HIGH, s, KafkaTypes.MAX_REQUEST_SIZE_KEY,
                v -> parseLongOrZero(v) > 10_485_760L,
                "max.request.size={value} — above 10 MiB. Without matching broker message.max.bytes and consumer fetch settings, oversize records die server-side."));
        addIfEnabled(rules, sev, RuleId.KAFKA_METADATA_MAX_AGE_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_METADATA_MAX_AGE_MS_TOO_LOW, s, KafkaTypes.METADATA_MAX_AGE_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 30000; },
                "metadata.max.age.ms={value} — below 30 s. Stale-metadata refresh is already triggered by leader-move errors; this only adds idle broker load."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_FETCH_MAX_WAIT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_FETCH_MAX_WAIT_MS_TOO_HIGH, s, KafkaTypes.FETCH_MAX_WAIT_MS_KEY,
                v -> parseIntOrZero(v) > 5000,
                "fetch.max.wait.ms={value} — above 5 s. Idle topics turn every poll() into a latency cliff; raise fetch.min.bytes instead if you want batching."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_TRANSACTIONAL_ID_GENERIC, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_TRANSACTIONAL_ID_GENERIC, s, KafkaTypes.TRANSACTIONAL_ID_KEY,
                v -> v != null && KafkaTypes.PRODUCER_GENERIC_TRANSACTIONAL_IDS.contains(v.trim().toLowerCase()),
                "transactional.id={value} — a generic placeholder. Two producers with this id will fence each other across restarts."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_AUTO_OFFSET_RESET_NONE_EXPLICIT, s -> ConfigKeyValueRule.literal(
                RuleId.CONSUMER_AUTO_OFFSET_RESET_NONE_EXPLICIT, s, KafkaTypes.AUTO_OFFSET_RESET_KEY, "none",
                "auto.offset.reset=none — fresh consumer groups refuse to start (NoOffsetForPartitionException). Sometimes intentional; verify runbook."));
        addIfEnabled(rules, sev, RuleId.KAFKA_CLIENT_ID_GENERIC, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_CLIENT_ID_GENERIC, s, KafkaTypes.CLIENT_ID_KEY,
                v -> v != null && KafkaTypes.KAFKA_GENERIC_CLIENT_IDS.contains(v.trim().toLowerCase()),
                "client.id={value} — generic placeholder. Broker metrics, quotas and audit logs cannot attribute traffic; use a service-qualified id."));
        addIfEnabled(rules, sev, RuleId.KAFKA_BOOTSTRAP_SERVERS_LOCALHOST, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_BOOTSTRAP_SERVERS_LOCALHOST, s, KafkaTypes.BOOTSTRAP_SERVERS_KEY,
                v -> v != null && (v.contains("localhost") || v.contains("127.0.0.1") || v.contains("0.0.0.0")),
                "bootstrap.servers={value} — points at the pod's own loopback. Externalize via env var or profile override."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_TXN_TIMEOUT_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_TXN_TIMEOUT_TOO_LOW, s, KafkaTypes.TRANSACTION_TIMEOUT_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 10000; },
                "transaction.timeout.ms={value} — below 10 s. Routine processing pauses will fence the producer. Default 60 s is the right starting point."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_TXN_TIMEOUT_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_TXN_TIMEOUT_TOO_HIGH, s, KafkaTypes.TRANSACTION_TIMEOUT_MS_KEY,
                v -> parseIntOrZero(v) > 900000,
                "transaction.timeout.ms={value} — above the broker default cap of 900000. initTransactions() will fail with INVALID_TRANSACTION_TIMEOUT."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_ISOLATION_LEVEL_READ_UNCOMMITTED_EXPLICIT, s -> ConfigKeyValueRule.literal(
                RuleId.CONSUMER_ISOLATION_LEVEL_READ_UNCOMMITTED_EXPLICIT, s, KafkaTypes.ISOLATION_LEVEL_KEY, "read_uncommitted",
                "isolation.level=read_uncommitted — consumer reads aborted/in-flight transactional records. On a transactional topic, use read_committed."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE, ProducerTxnIdWithoutIdempotenceRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_MAX_IN_FLIGHT_TOO_HIGH, ProducerMaxInFlightTooHighRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_ASSIGN_AND_SUBSCRIBE, ConsumerAssignAndSubscribeRule::new);
        addIfEnabled(rules, sev, RuleId.KAFKA_CLIENT_TYPO_GROUP_ID, KafkaClientTypoGroupIdRule::new);

        // ── kafka-streams ──────────────────────────────────────────────────────
        addIfEnabled(rules, sev, RuleId.STREAMS_REPLICATION_FACTOR_ONE, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_REPLICATION_FACTOR_ONE, s, KafkaTypes.STREAMS_REPLICATION_FACTOR_KEY, "1",
                "Streams replication.factor=1 — internal changelog/repartition topics become single-points-of-failure."));
        addIfEnabled(rules, sev, RuleId.STREAMS_REPLICATION_FACTOR_TWO, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_REPLICATION_FACTOR_TWO, s, KafkaTypes.STREAMS_REPLICATION_FACTOR_KEY, "2",
                "Streams replication.factor=2 — losing one broker leaves only one replica; production target is 3 with min.insync.replicas=2."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TOPOLOGY_OPTIMIZATION_NONE, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_TOPOLOGY_OPTIMIZATION_NONE, s, KafkaTypes.STREAMS_TOPOLOGY_OPTIMIZATION_KEY, "none",
                "topology.optimization=none — extra repartition/changelog topics that 'all' would eliminate. For new apps, switch to 'all'."));
        addIfEnabled(rules, sev, RuleId.STREAMS_STATE_DIR_TMP, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_STATE_DIR_TMP, s, KafkaTypes.STREAMS_STATE_DIR_KEY,
                v -> v != null && (v.startsWith("/tmp") || v.startsWith("/var/tmp")),
                "state.dir={value} — Streams state on ephemeral /tmp; restart triggers full changelog rebuild."));
        addIfEnabled(rules, sev, RuleId.STREAMS_EOS_V1_DEPRECATED, s -> ConfigKeyValueRule.literalAny(
                RuleId.STREAMS_EOS_V1_DEPRECATED, s, KafkaTypes.STREAMS_PROCESSING_GUARANTEE_KEY,
                KafkaTypes.STREAMS_EOS_V1_VALUES,
                "processing.guarantee={value} — EOS-v1 was deprecated by KIP-732 and removed in Kafka 4.0. Use exactly_once_v2."));
        addIfEnabled(rules, sev, RuleId.STREAMS_COMMIT_INTERVAL_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_COMMIT_INTERVAL_TOO_LOW, s, KafkaTypes.STREAMS_COMMIT_INTERVAL_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 100; },
                "commit.interval.ms={value} — well below the 100 ms documented floor. Broker write rate (offset commits + changelog flush) goes pathological."));
        addIfEnabled(rules, sev, RuleId.STREAMS_CACHE_DISABLED, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_CACHE_DISABLED, s, KafkaTypes.STREAMS_CACHE_MAX_BYTES_BUFFERING_KEY,
                "0"::equals,
                "cache.max.bytes.buffering=0 — every state-store update is forwarded; changelog write rate explodes."));
        addIfEnabled(rules, sev, RuleId.STREAMS_CLEANUP_IN_PROD, s -> new MethodCallRule(
                RuleId.STREAMS_CLEANUP_IN_PROD, s, Set.of(KafkaTypes.KAFKA_STREAMS), Set.of("cleanUp"),
                "KafkaStreams.cleanUp() — wipes local state. Acceptable in tests; in prod it forces full changelog rebuild."));
        addIfEnabled(rules, sev, RuleId.STREAMS_THROUGH_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_THROUGH_DEPRECATED, s, Set.of(KafkaTypes.KSTREAM), Set.of("through"),
                "KStream.through() is deprecated since Kafka 2.6 — use repartition() or an explicit to()/stream() pair."));
        addIfEnabled(rules, sev, RuleId.STREAMS_NUM_STANDBY_REPLICAS_ZERO, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_NUM_STANDBY_REPLICAS_ZERO, s, KafkaTypes.STREAMS_NUM_STANDBY_REPLICAS_KEY, "0",
                "num.standby.replicas=0 — any instance failure forces a full changelog restore on a peer (minutes-to-hours of recovery)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_DESER_HANDLER_LOG_AND_CONTINUE, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_DESER_HANDLER_LOG_AND_CONTINUE, s, KafkaTypes.STREAMS_DEFAULT_DESER_HANDLER_KEY,
                v -> v != null && v.endsWith("LogAndContinueExceptionHandler"),
                "default.deserialization.exception.handler={value} — silently drops undeserializable records. Use a DLQ-based handler or the default fail-fast handler."));
        addIfEnabled(rules, sev, RuleId.STREAMS_CACHE_KEY_DEPRECATED, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_CACHE_KEY_DEPRECATED, s, KafkaTypes.STREAMS_CACHE_MAX_BYTES_BUFFERING_KEY,
                v -> v != null && !v.isEmpty(),
                "cache.max.bytes.buffering={value} — deprecated since Kafka 3.4. Rename to statestore.cache.max.bytes."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TASK_TIMEOUT_MS_ZERO, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_TASK_TIMEOUT_MS_ZERO, s, KafkaTypes.STREAMS_TASK_TIMEOUT_MS_KEY, "0",
                "task.timeout.ms=0 — first transient broker error kills the task. Default 300000 ms is the right starting point."));
        addIfEnabled(rules, sev, RuleId.STREAMS_KSTREAM_PRINT, s -> new MethodCallRule(
                RuleId.STREAMS_KSTREAM_PRINT, s, Set.of(KafkaTypes.KSTREAM), Set.of("print"),
                "KStream.print() — debugging operator left in production topology. Pipes every record through System.out; use peek() with a counter or a dedicated debug topic."));
        addIfEnabled(rules, sev, RuleId.STREAMS_MAX_TASK_IDLE_MS_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_MAX_TASK_IDLE_MS_HIGH, s, KafkaTypes.STREAMS_MAX_TASK_IDLE_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 30000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "max.task.idle.ms={value} — above 30 s. Topology stalls waiting on quiet partitions, inflating end-to-end latency. Default 0 is the right starting point."));
        addIfEnabled(rules, sev, RuleId.STREAMS_PRODUCTION_EXCEPTION_HANDLER_ALWAYS_CONTINUE, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_PRODUCTION_EXCEPTION_HANDLER_ALWAYS_CONTINUE, s, KafkaTypes.STREAMS_DEFAULT_PRODUCTION_HANDLER_KEY,
                v -> v != null && v.endsWith("AlwaysContinueProductionExceptionHandler"),
                "default.production.exception.handler={value} — silently drops every failed produce. Use the default fail-fast handler or a selective one with DLQ."));
        addIfEnabled(rules, sev, RuleId.STREAMS_NUM_STREAM_THREADS_ONE, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_NUM_STREAM_THREADS_ONE, s, KafkaTypes.STREAMS_NUM_STREAM_THREADS_KEY, "1",
                "num.stream.threads=1 — single-thread topology cannot use multi-core pods. Set to min(input-partitions/instances, cpu-cores)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_NUM_STREAM_THREADS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_NUM_STREAM_THREADS_TOO_HIGH, s, KafkaTypes.STREAMS_NUM_STREAM_THREADS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Integer.parseInt(v.trim()) > 64; }
                    catch (NumberFormatException e) { return false; }
                },
                "num.stream.threads={value} — above 64. Threads beyond the assignable task count sit idle, claiming heap and metric overhead."));
        addIfEnabled(rules, sev, RuleId.STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_WALL_CLOCK, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_WALL_CLOCK, s, KafkaTypes.STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_KEY,
                v -> v != null && v.endsWith("WallclockTimestampExtractor"),
                "default.timestamp.extractor={value} — wall clock replaces record event time; windows and joins silently bucket into the wrong window on replay."));
        addIfEnabled(rules, sev, RuleId.KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_LOW, s, KafkaTypes.CONNECTIONS_MAX_IDLE_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) < 30000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "connections.max.idle.ms={value} — below 30 s. Every brief lull triggers a full TCP/TLS/SASL re-handshake. Default 540000 ms is right."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_HIGH, s, KafkaTypes.MAX_PARTITION_FETCH_BYTES_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 16L * 1024 * 1024; }
                    catch (NumberFormatException e) { return false; }
                },
                "max.partition.fetch.bytes={value} — above 16 MiB. Per-partition memory cost scales with assigned partitions; raises rebalance/GC pressure."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TASK_TIMEOUT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_TASK_TIMEOUT_MS_TOO_HIGH, s, KafkaTypes.STREAMS_TASK_TIMEOUT_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 1_800_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "task.timeout.ms={value} — above 30 minutes. Stuck tasks swallow broker errors silently instead of failing fast and triggering recovery."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_GROUP_INSTANCE_ID_GENERIC, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_GROUP_INSTANCE_ID_GENERIC, s, KafkaTypes.GROUP_INSTANCE_ID_KEY,
                v -> v != null && KafkaTypes.KAFKA_GENERIC_CLIENT_IDS.contains(v.trim().toLowerCase()),
                "group.instance.id={value} — a generic placeholder. Static membership requires per-pod uniqueness; a shared literal makes replicas fence each other with FencedInstanceIdException."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_SEND_BUFFER_BYTES_TOO_SMALL, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_SEND_BUFFER_BYTES_TOO_SMALL, s, KafkaTypes.SEND_BUFFER_BYTES_KEY,
                v -> {
                    if (v == null) return false;
                    try {
                        long n = Long.parseLong(v.trim());
                        return n > 0 && n <= 16384L;
                    } catch (NumberFormatException e) { return false; }
                },
                "send.buffer.bytes={value} — at or below 16 KiB. Caps TCP throughput to buffer/RTT; defeats OS autotuning. Use -1 or leave default (128 KiB)."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_RECEIVE_BUFFER_BYTES_TOO_SMALL, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_RECEIVE_BUFFER_BYTES_TOO_SMALL, s, KafkaTypes.RECEIVE_BUFFER_BYTES_KEY,
                v -> {
                    if (v == null) return false;
                    try {
                        long n = Long.parseLong(v.trim());
                        return n > 0 && n <= 16384L;
                    } catch (NumberFormatException e) { return false; }
                },
                "receive.buffer.bytes={value} — at or below 16 KiB. Caps TCP throughput to buffer/RTT; defeats OS autotuning. Use -1 or leave default (64 KiB)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_REPLICATION_FACTOR_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_REPLICATION_FACTOR_TOO_HIGH, s, KafkaTypes.STREAMS_REPLICATION_FACTOR_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Integer.parseInt(v.trim()) > 5; }
                    catch (NumberFormatException e) { return false; }
                },
                "replication.factor={value} — above 5. Internal-topic disk and follower-fetch bandwidth scale linearly; RF=3 already survives any single AZ outage."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_LOW, s, KafkaTypes.AUTO_COMMIT_INTERVAL_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try {
                        long n = Long.parseLong(v.trim());
                        return n > 0 && n < 1000L;
                    } catch (NumberFormatException e) { return false; }
                },
                "auto.commit.interval.ms={value} — below 1 s. Floods the group coordinator and inflates commit latency for every group sharing it. Default 5000 is right."));
        addIfEnabled(rules, sev, RuleId.STREAMS_NUM_STANDBY_REPLICAS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_NUM_STANDBY_REPLICAS_TOO_HIGH, s, KafkaTypes.STREAMS_NUM_STANDBY_REPLICAS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Integer.parseInt(v.trim()) > 3; }
                    catch (NumberFormatException e) { return false; }
                },
                "num.standby.replicas={value} — above 3. Each standby is a full RocksDB copy on a peer; disk and changelog bandwidth scale linearly with diminishing recovery benefit."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_BATCH_SIZE_TOO_LARGE, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_BATCH_SIZE_TOO_LARGE, s, KafkaTypes.BATCH_SIZE_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 1_048_576L; }
                    catch (NumberFormatException e) { return false; }
                },
                "batch.size={value} — above 1 MiB. Per-partition pre-allocated buffers explode buffer.memory under fanout; send() stalls or throws BufferExhaustedException."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_REQUEST_TIMEOUT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_REQUEST_TIMEOUT_MS_TOO_HIGH, s, KafkaTypes.REQUEST_TIMEOUT_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 300_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "request.timeout.ms={value} — above 5 minutes. Sender thread stalls on a slow broker for the full window instead of retrying; cluster wobble cascades into app outages."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_MAX_POLL_INTERVAL_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_MAX_POLL_INTERVAL_MS_TOO_HIGH, s, KafkaTypes.MAX_POLL_INTERVAL_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 1_800_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "max.poll.interval.ms={value} — above 30 minutes. A hung consumer holds its partition assignment for the whole window; peers cannot pick up its work."));
        addIfEnabled(rules, sev, RuleId.STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_HIGH, s, KafkaTypes.STREAMS_BUFFERED_RECORDS_PER_PARTITION_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 100_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "buffered.records.per.partition={value} — above 100k. Removes the back-pressure ceiling; one skewed partition can OOM the JVM. Default 1000 is right."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_FETCH_MAX_BYTES_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_FETCH_MAX_BYTES_TOO_HIGH, s, KafkaTypes.FETCH_MAX_BYTES_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 100L * 1024 * 1024; }
                    catch (NumberFormatException e) { return false; }
                },
                "fetch.max.bytes={value} — above 100 MiB. A single FetchResponse can stall the consumer for seconds and pin that many bytes in heap per request; defaults around 50 MiB are safer."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_SESSION_TIMEOUT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_SESSION_TIMEOUT_MS_TOO_HIGH, s, KafkaTypes.SESSION_TIMEOUT_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 60_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "session.timeout.ms={value} — above 60 s. Most managed brokers cap this at 60 s (group.max.session.timeout.ms); past that the consumer's JoinGroup is rejected at startup. Even when accepted, a crashed pod's partitions stay frozen for the full window before rebalancing."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_HIGH, s, KafkaTypes.HEARTBEAT_INTERVAL_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) >= 15_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "heartbeat.interval.ms={value} — at/above session.timeout.ms / 3. One missed heartbeat (GC pause, network blip) now triggers an eviction and a group-wide rebalance. Keep it ≤ 1/3 of session.timeout.ms."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_TRANSACTION_TIMEOUT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_TRANSACTION_TIMEOUT_MS_TOO_HIGH, s, KafkaTypes.TRANSACTION_TIMEOUT_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 900_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "transaction.timeout.ms={value} — above 15 min. A crashed transactional producer blocks the LSO on every partition it wrote to for the full window; downstream read_committed consumers stall with zero error signal."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_MAX_POLL_RECORDS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_MAX_POLL_RECORDS_TOO_LOW, s, KafkaTypes.MAX_POLL_RECORDS_KEY,
                v -> {
                    if (v == null) return false;
                    try { long n = Long.parseLong(v.trim()); return n >= 1 && n <= 5; }
                    catch (NumberFormatException e) { return false; }
                },
                "max.poll.records={value} — at or below 5. Each poll round-trip returns at most a handful of records; per-poll overhead dominates and effective throughput collapses. Default 500 is right."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_MAX_REQUEST_SIZE_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_MAX_REQUEST_SIZE_TOO_LOW, s, KafkaTypes.MAX_REQUEST_SIZE_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) < 65536L; }
                    catch (NumberFormatException e) { return false; }
                },
                "max.request.size={value} — below 64 KiB. Defeats batching, and any single record above the cap throws RecordTooLargeException at send time with no retry."));
        addIfEnabled(rules, sev, RuleId.KAFKA_METADATA_MAX_AGE_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_METADATA_MAX_AGE_MS_TOO_HIGH, s, KafkaTypes.METADATA_MAX_AGE_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 600_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "metadata.max.age.ms={value} — above 10 min. Leader transitions and partition reassignments take that long to be noticed; sends to moved partitions retry against the stale leader until the cache refreshes."));
        addIfEnabled(rules, sev, RuleId.KAFKA_BOOTSTRAP_SERVERS_SINGLE_BROKER, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_BOOTSTRAP_SERVERS_SINGLE_BROKER, s, KafkaTypes.BOOTSTRAP_SERVERS_KEY,
                v -> {
                    if (v == null) return false;
                    String t = v.trim();
                    if (t.isEmpty()) return false;
                    if (t.contains("localhost") || t.contains("127.0.0.1") || t.contains("0.0.0.0")) return false;
                    return !t.contains(",");
                },
                "bootstrap.servers={value} — one entry only. Any single broker outage during pod startup leaves the client unable to fetch cluster metadata. List 3+ brokers."));
        addIfEnabled(rules, sev, RuleId.SCHEMA_REGISTRY_URL_LOCALHOST, s -> new ConfigKeyValueRule(
                RuleId.SCHEMA_REGISTRY_URL_LOCALHOST, s, KafkaTypes.SCHEMA_REGISTRY_URL_KEY,
                v -> v != null && (v.contains("localhost") || v.contains("127.0.0.1") || v.contains("0.0.0.0")),
                "schema.registry.url={value} — points at the pod's loopback. Every (de)serializer call will fail with Connection refused in any non-local environment."));
        addIfEnabled(rules, sev, RuleId.STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_NONE, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_NONE, s, KafkaTypes.STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_KEY, "none",
                "rack.aware.assignment.strategy=none — explicitly disables rack-aware task placement. Standby tasks may land in the same AZ as their active; an AZ outage kills both."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_LOW, s, KafkaTypes.MAX_PARTITION_FETCH_BYTES_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) < 1_048_576L; }
                    catch (NumberFormatException e) { return false; }
                },
                "max.partition.fetch.bytes={value} — below 1 MiB (the broker's default max.message.bytes). Any record above this cap stalls the partition or wastes a round-trip; default 1 MiB matches the broker's default by design."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_HIGH, s, KafkaTypes.DEFAULT_API_TIMEOUT_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 300_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "default.api.timeout.ms={value} — above 5 min. Blocking calls like commitSync()/position()/listTopics() hang the calling thread for the whole window before throwing; graceful shutdown stalls past the pod's terminationGracePeriod."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_MAX_BLOCK_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_MAX_BLOCK_MS_TOO_HIGH, s, KafkaTypes.MAX_BLOCK_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 60_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "max.block.ms={value} — above 60 s. producer.send() and partitionsFor() block the calling thread for the full window when the buffer is full or metadata is stale; request-handler thread pools drain within seconds during transient broker hiccups."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_FETCH_MAX_WAIT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_FETCH_MAX_WAIT_MS_TOO_LOW, s, KafkaTypes.FETCH_MAX_WAIT_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { long n = Long.parseLong(v.trim()); return n > 0 && n < 50L; }
                    catch (NumberFormatException e) { return false; }
                },
                "fetch.max.wait.ms={value} — below 50 ms. Broker returns immediately even when fetch.min.bytes is not satisfied; consumer spins in a tight empty-fetch loop, wasting broker and client CPU."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_RETRY_BACKOFF_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_RETRY_BACKOFF_MS_TOO_HIGH, s, KafkaTypes.RETRY_BACKOFF_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { return Long.parseLong(v.trim()) > 30_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "retry.backoff.ms={value} — above 30 s. Producer waits this long between retries of retriable errors; routine leader-move recovery that the default (100 ms) handles in seconds now takes minutes."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_PARTITIONER_IGNORE_KEYS_TRUE, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_PARTITIONER_IGNORE_KEYS_TRUE, s, KafkaTypes.PARTITIONER_IGNORE_KEYS_KEY, "true",
                "partitioner.ignore.keys=true — KIP-794 partitioner ignores record keys; two records with the same key route to different partitions. Compacted topics, joins and per-key ordering break silently."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_CLIENT_DNS_LOOKUP_DEFAULT, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_CLIENT_DNS_LOOKUP_DEFAULT, s, KafkaTypes.CLIENT_DNS_LOOKUP_KEY, "default",
                "client.dns.lookup=default — deprecated in 2.6 and removed in 3.0 (the client throws ConfigException at startup). Only the first A-record IP is used; multi-IP broker DNS reconnect-failover stops working. Remove the override (default is now use_all_dns_ips)."));
        addIfEnabled(rules, sev, RuleId.KAFKA_AUTO_INCLUDE_JMX_REPORTER_FALSE, s -> ConfigKeyValueRule.literal(
                RuleId.KAFKA_AUTO_INCLUDE_JMX_REPORTER_FALSE, s, KafkaTypes.AUTO_INCLUDE_JMX_REPORTER_KEY, "false",
                "auto.include.jmx.reporter=false — disables the built-in JmxReporter. If metric.reporters is empty, every kafka-clients metric (consumer-lag, request-latency, in-flight) disappears from observability. Only set when a replacement reporter is verified end-to-end."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_AUTO_OFFSET_RESET_INVALID, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_AUTO_OFFSET_RESET_INVALID, s, KafkaTypes.AUTO_OFFSET_RESET_KEY,
                v -> {
                    if (v == null) return false;
                    String t = v.trim().toLowerCase();
                    if (t.isEmpty()) return false;
                    return !KafkaTypes.CONSUMER_AUTO_OFFSET_RESET_VALID_VALUES.contains(t);
                },
                "auto.offset.reset={value} — not one of earliest/latest/none. The consumer throws ConfigException at construction; the pod crash-loops on first deploy."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_ACKS_INVALID, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_ACKS_INVALID, s, KafkaTypes.ACKS_KEY,
                v -> {
                    if (v == null) return false;
                    String t = v.trim().toLowerCase();
                    if (t.isEmpty()) return false;
                    return !KafkaTypes.PRODUCER_ACKS_VALID_VALUES.contains(t);
                },
                "acks={value} — not one of 0/1/-1/all. The producer throws ConfigException at construction; the pod crash-loops on first deploy."));
        addIfEnabled(rules, sev, RuleId.STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_LOW, s, KafkaTypes.STREAMS_PROBING_REBALANCE_INTERVAL_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { long n = Long.parseLong(v.trim()); return n > 0 && n < 60_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "probing.rebalance.interval.ms={value} — below 60 s. Every probe triggers a group-wide cooperative rebalance just to ask if standby tasks are caught up; topology spends more time rebalancing than processing. Default 600000 is right."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_INTERCEPTOR_CLASSES_LEGACY, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_INTERCEPTOR_CLASSES_LEGACY, s, KafkaTypes.INTERCEPTOR_CLASSES_KEY,
                v -> v != null && KafkaTypes.LEGACY_MONITORING_INTERCEPTOR_FQCNS.stream().anyMatch(v::contains),
                "interceptor.classes={value} — wires Confluent's legacy MonitoringConsumerInterceptor/MonitoringProducerInterceptor. On managed Kafka the monitoring topic doesn't exist; writes fail silently while per-record CPU is still paid. Migrate to Confluent Health+ and remove."));
        addIfEnabled(rules, sev, RuleId.STREAMS_STATESTORE_CACHE_MAX_BYTES_ZERO, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_STATESTORE_CACHE_MAX_BYTES_ZERO, s, KafkaTypes.STREAMS_STATESTORE_CACHE_MAX_BYTES_KEY, "0",
                "statestore.cache.max.bytes=0 — Streams state-store cache (KIP-770, replaces cache.max.bytes.buffering) disabled. Every put() flushes to RocksDB, the changelog and downstream; 10-100× write amplification on aggregations/KTables."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_GROUP_PROTOCOL_CLASSIC, s -> ConfigKeyValueRule.literal(
                RuleId.CONSUMER_GROUP_PROTOCOL_CLASSIC, s, KafkaTypes.GROUP_PROTOCOL_KEY, KafkaTypes.GROUP_PROTOCOL_CLASSIC_VALUE,
                "group.protocol=classic — pins the consumer to the legacy heartbeat/JoinGroup protocol. Misses KIP-848's broker-side rebalance, decoupled heartbeat and incremental assignment. Remove the override on Kafka 4.0+."));
        addIfEnabled(rules, sev, RuleId.KAFKA_ENABLE_METRICS_PUSH_FALSE, s -> ConfigKeyValueRule.literal(
                RuleId.KAFKA_ENABLE_METRICS_PUSH_FALSE, s, KafkaTypes.ENABLE_METRICS_PUSH_KEY, "false",
                "enable.metrics.push=false — disables KIP-714 client telemetry to the broker. Cluster operators lose visibility into this client's latency/throughput/error metrics during incidents. Default true; only disable when explicitly required."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_GROUP_ID_PLACEHOLDER, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_GROUP_ID_PLACEHOLDER, s, KafkaTypes.GROUP_ID_KEY,
                v -> looksLikeUnresolvedPlaceholder(v),
                "group.id={value} — looks like an unresolved placeholder (${...}). Plain Java string literals never go through env-var or Spring property substitution; the consumer joins a group literally named with the placeholder text. Resolve via @Value/System.getenv/ConfigProvider before constructing the consumer."));
        addIfEnabled(rules, sev, RuleId.KAFKA_SASL_LOGIN_CONNECT_TIMEOUT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SASL_LOGIN_CONNECT_TIMEOUT_MS_TOO_LOW, s, KafkaTypes.SASL_LOGIN_CONNECT_TIMEOUT_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 5000; },
                "sasl.login.connect.timeout.ms={value} — below 5000 ms. The TCP-connect timeout to the IdP (OAuth/OIDC token endpoint, Kerberos KDC) is tighter than realistic cold-start latency; every login attempt fails before the very first handshake completes. Raise to >=5000 (10000+ for cloud IdPs)."));
        addIfEnabled(rules, sev, RuleId.KAFKA_SASL_LOGIN_READ_TIMEOUT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SASL_LOGIN_READ_TIMEOUT_MS_TOO_LOW, s, KafkaTypes.SASL_LOGIN_READ_TIMEOUT_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 5000; },
                "sasl.login.read.timeout.ms={value} — below 5000 ms. The socket-read timeout to the IdP is tighter than realistic JWT-issuance latency; client closes the socket mid-response and treats every cold-start as auth failure. Raise to >=5000 (10000+ for cloud IdPs)."));
        addIfEnabled(rules, sev, RuleId.SECURITY_SASL_OAUTHBEARER_TOKEN_ENDPOINT_HTTP, s -> new ConfigKeyValueRule(
                RuleId.SECURITY_SASL_OAUTHBEARER_TOKEN_ENDPOINT_HTTP, s, KafkaTypes.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_KEY,
                v -> v != null && v.trim().toLowerCase(java.util.Locale.ROOT).startsWith("http://"),
                "sasl.oauthbearer.token.endpoint.url={value} — uses http://. The OAuth2 client_credentials POST (with client_secret) and the issued bearer JWT both traverse the network in cleartext; any on-path observer captures and replays them. Change to https:// with a verified cert chain."));
        addIfEnabled(rules, sev, RuleId.KAFKA_RETRY_BACKOFF_MAX_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_RETRY_BACKOFF_MAX_MS_TOO_LOW, s, KafkaTypes.RETRY_BACKOFF_MAX_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 1000; },
                "retry.backoff.max.ms={value} — below the 1000 ms default. Collapses the KIP-580 exponential-backoff curve into a near-flat line at retry.backoff.ms; sustained retries pound the broker at the starting cadence. Raise to >=1000."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_DISABLED, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_DISABLED, s, KafkaTypes.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_KEY, "false",
                "partitioner.adaptive.partitioning.enable=false — opts out of KIP-794 adaptive partitioning. The built-in partitioner reverts to round-robin and keeps sending equal traffic to slow brokers, multiplying the impact of any single-broker degradation. Remove this line; the default (true) is strictly better."));
        addIfEnabled(rules, sev, RuleId.KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_TOO_LOW, s, KafkaTypes.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 30000; },
                "socket.connection.setup.timeout.max.ms={value} — below the 30 s default. Caps the exponential backoff between TCP+TLS+SASL setup retries too tight; clients re-handshake faster than the broker can complete prior negotiations. Raise to >=30000."));
        addIfEnabled(rules, sev, RuleId.JACKSON_DEFAULT_TYPING_ENABLED, s -> new MethodCallRule(
                RuleId.JACKSON_DEFAULT_TYPING_ENABLED, s, Set.of(KafkaTypes.OBJECT_MAPPER),
                Set.of(KafkaTypes.JACKSON_ENABLE_DEFAULT_TYPING_METHOD, KafkaTypes.JACKSON_ACTIVATE_DEFAULT_TYPING_METHOD),
                "ObjectMapper.enableDefaultTyping()/activateDefaultTyping() — RCE gadget. Inbound JSON with @class names an arbitrary class to instantiate; Jackson runs its constructor/setter graph. Use @JsonTypeInfo with @JsonSubTypes (closed-world) or a strict BasicPolymorphicTypeValidator allowlist; never LaissezFaireSubTypeValidator."));
        addIfEnabled(rules, sev, RuleId.STREAMS_LOCAL_THREADS_METADATA_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_LOCAL_THREADS_METADATA_DEPRECATED, s, Set.of(KafkaTypes.KAFKA_STREAMS), Set.of("localThreadsMetadata"),
                "KafkaStreams.localThreadsMetadata() is deprecated since Kafka Streams 3.0 — replaced by KafkaStreams.metadataForLocalThreads() (same return type Set<ThreadMetadata>, mechanical rename)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_FLAT_TRANSFORM_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_FLAT_TRANSFORM_DEPRECATED, s, Set.of(KafkaTypes.KSTREAM), Set.of("flatTransform", "flatTransformValues"),
                "KStream.flatTransform()/flatTransformValues() are deprecated since Kafka Streams 3.3 (KIP-820) — replaced by KStream.process(ProcessorSupplier) / processValues(FixedKeyProcessorSupplier) where fan-out is the default (call context.forward zero, one, or many times)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TIME_WINDOWS_OF_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_TIME_WINDOWS_OF_DEPRECATED, s, Set.of(KafkaTypes.TIME_WINDOWS), Set.of("of"),
                "TimeWindows.of(Duration) is deprecated since Kafka Streams 3.0 (KIP-633) — replaced by TimeWindows.ofSizeWithNoGrace(Duration) or ofSizeAndGrace(Duration, Duration). The legacy 24-hour default grace period silently inflated state-store size by ~1440×."));
        addIfEnabled(rules, sev, RuleId.STREAMS_JOIN_WINDOWS_OF_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_JOIN_WINDOWS_OF_DEPRECATED, s, Set.of(KafkaTypes.JOIN_WINDOWS), Set.of("of"),
                "JoinWindows.of(Duration) is deprecated since Kafka Streams 3.0 (KIP-633) — replaced by JoinWindows.ofTimeDifferenceWithNoGrace(Duration) or ofTimeDifferenceAndGrace(Duration, Duration). The legacy 24-hour default grace period inflated stream-stream join state-store sizes by ~1440×."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TRANSFORM_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_TRANSFORM_DEPRECATED, s, Set.of(KafkaTypes.KSTREAM), Set.of("transform"),
                "KStream.transform() is deprecated since Kafka Streams 3.3 (KIP-820) — replaced by KStream.process(ProcessorSupplier) with the new org.apache.kafka.streams.processor.api.Processor."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TRANSFORM_VALUES_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_TRANSFORM_VALUES_DEPRECATED, s, Set.of(KafkaTypes.KSTREAM), Set.of("transformValues"),
                "KStream.transformValues() is deprecated since Kafka Streams 3.3 (KIP-820) — replaced by KStream.processValues(FixedKeyProcessorSupplier) which type-enforces the key-invariant."));
        addIfEnabled(rules, sev, RuleId.STREAMS_BRANCH_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_BRANCH_DEPRECATED, s, Set.of(KafkaTypes.KSTREAM), Set.of("branch"),
                "KStream.branch(Predicate...) is deprecated since Kafka Streams 2.8 (KIP-418) — replaced by KStream.split().branch(..., Branched.as(name)) which supports named branches and a defaultBranch for unmatched records."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_GROUP_INSTANCE_ID_PLACEHOLDER, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_GROUP_INSTANCE_ID_PLACEHOLDER, s, KafkaTypes.GROUP_INSTANCE_ID_KEY,
                v -> looksLikeUnresolvedPlaceholder(v),
                "group.instance.id={value} — unresolved ${...} placeholder. Every pod registers the same static-membership identity; the second pod's JoinGroup fences the first off the group with FencedInstanceIdException and rolling deploys ping-pong. Resolve via @Value/System.getenv before putting."));
        addIfEnabled(rules, sev, RuleId.SECURITY_PROTOCOL_PLACEHOLDER, s -> new ConfigKeyValueRule(
                RuleId.SECURITY_PROTOCOL_PLACEHOLDER, s, KafkaTypes.SECURITY_PROTOCOL_KEY,
                v -> looksLikeUnresolvedPlaceholder(v),
                "security.protocol={value} — unresolved ${...} placeholder. Kafka's validator rejects any value outside {PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL}; the client throws ConfigException at construction and the pod crash-loops. Resolve before putting."));
        addIfEnabled(rules, sev, RuleId.KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_HIGH, s, KafkaTypes.CONNECTIONS_MAX_IDLE_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { long n = Long.parseLong(v.trim()); return n > 600_000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "connections.max.idle.ms={value} — above the broker default (600 000 ms) and typical LB/NAT idle timeouts. The broker closes the socket on idle while the client keeps it pooled; the next request fails with DisconnectException at the broker-idle cadence. Stay ≤ 540 000 ms."));
        addIfEnabled(rules, sev, RuleId.KAFKA_METRICS_RECORDING_LEVEL_DEBUG, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_METRICS_RECORDING_LEVEL_DEBUG, s, KafkaTypes.METRICS_RECORDING_LEVEL_KEY,
                v -> v != null && KafkaTypes.METRICS_RECORDING_LEVEL_VERBOSE_VALUES.contains(v.trim().toUpperCase()),
                "metrics.recording.level={value} — enables per-partition/per-thread (DEBUG) or per-record (TRACE) metric updates on the hot path. 5-30% throughput cost; usually left in place after a debugging session. Remove the override."));
        addIfEnabled(rules, sev, RuleId.KAFKA_RECONNECT_BACKOFF_MAX_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_RECONNECT_BACKOFF_MAX_MS_TOO_LOW, s, KafkaTypes.RECONNECT_BACKOFF_MAX_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { long n = Long.parseLong(v.trim()); return n > 0 && n < 1000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "reconnect.backoff.max.ms={value} — caps exponential reconnect backoff below 1 s. During broker rolling restarts every client hammers the restarting broker every few hundred ms; the broker takes 5-20× longer to rejoin. Default 1000 is right."));
        addIfEnabled(rules, sev, RuleId.KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MS_TOO_LOW, s, KafkaTypes.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_KEY,
                v -> {
                    if (v == null) return false;
                    try { long n = Long.parseLong(v.trim()); return n > 0 && n < 5000L; }
                    catch (NumberFormatException e) { return false; }
                },
                "socket.connection.setup.timeout.ms={value} — below 5 s. The entire TCP+TLS+SASL handshake must finish in this budget; cross-AZ or TLS-1.2 handshakes routinely take 2-5 s. Connections fail spuriously on healthy brokers."));
        addIfEnabled(rules, sev, RuleId.KAFKA_BOOTSTRAP_SERVERS_PLACEHOLDER, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_BOOTSTRAP_SERVERS_PLACEHOLDER, s, KafkaTypes.BOOTSTRAP_SERVERS_KEY,
                v -> looksLikeUnresolvedPlaceholder(v),
                "bootstrap.servers={value} — unresolved ${...} placeholder reaches ClientUtils.parseAndValidateAddresses; the client either throws ConfigException or DNS-fails on a literal hostname with brace characters. App crash-loops at startup. Resolve before putting."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_TRANSACTIONAL_ID_PLACEHOLDER, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_TRANSACTIONAL_ID_PLACEHOLDER, s, KafkaTypes.TRANSACTIONAL_ID_KEY,
                v -> looksLikeUnresolvedPlaceholder(v),
                "transactional.id={value} — unresolved ${...} placeholder. Every pod registers the same literal transactional.id; rolling deploys fence each other with ProducerFencedException. The transactional.id must be unique per producer instance — resolve before putting."));
        addIfEnabled(rules, sev, RuleId.STREAMS_APPLICATION_ID_PLACEHOLDER, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_APPLICATION_ID_PLACEHOLDER, s, KafkaTypes.STREAMS_APPLICATION_ID_KEY,
                v -> looksLikeUnresolvedPlaceholder(v),
                "application.id={value} — unresolved ${...} placeholder. Streams stamps the literal text on the consumer-group, every internal changelog/repartition topic and the EOS transactional.id. Two services with this bug share Kafka artifacts; data corruption follows. Resolve before constructing StreamsConfig."));
        addIfEnabled(rules, sev, RuleId.KAFKA_CLIENT_ID_PLACEHOLDER, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_CLIENT_ID_PLACEHOLDER, s, KafkaTypes.CLIENT_ID_KEY,
                v -> looksLikeUnresolvedPlaceholder(v),
                "client.id={value} — looks like an unresolved placeholder (${...}). Metrics, broker request-log and per-client-id quotas all use the literal text; every pod collapses onto one observability bucket. Resolve before putting into Properties."));
        addIfEnabled(rules, sev, RuleId.STREAMS_MAX_WARMUP_REPLICAS_ZERO, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_MAX_WARMUP_REPLICAS_ZERO, s, KafkaTypes.STREAMS_MAX_WARMUP_REPLICAS_KEY, "0",
                "max.warmup.replicas=0 — Streams cannot warm up tasks on new instances. Every scale-up freezes those partitions for the full changelog-restore duration (often 10-60 s per task). Default 2 bounds restore bandwidth; leave it."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_MIXED, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_MIXED, s, KafkaTypes.PARTITION_ASSIGNMENT_STRATEGY_KEY,
                v -> {
                    if (v == null) return false;
                    boolean hasEager = KafkaTypes.LEGACY_PARTITION_ASSIGNORS.stream().anyMatch(v::contains);
                    boolean hasCooperative = v.contains(KafkaTypes.COOPERATIVE_STICKY_ASSIGNOR_FQCN);
                    return hasEager && hasCooperative;
                },
                "partition.assignment.strategy={value} — mixes eager (Range/RoundRobin/Sticky) and cooperative (CooperativeSticky) assignors. KIP-429 migration is two-step: deploy with both lists, then remove the eager one. Keeping both forever traps the group in eager rebalances or, with heterogeneous member lists, deadlocks JoinGroup."));
        addIfEnabled(rules, sev, RuleId.STREAMS_APPLICATION_SERVER_LOCALHOST, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_APPLICATION_SERVER_LOCALHOST, s, KafkaTypes.STREAMS_APPLICATION_SERVER_KEY,
                v -> {
                    if (v == null) return false;
                    String host = v.trim().toLowerCase();
                    int colon = host.indexOf(':');
                    if (colon > 0) host = host.substring(0, colon);
                    return KafkaTypes.LOCALHOST_HOST_TOKENS.contains(host);
                },
                "application.server={value} — interactive queries from peer Streams instances dial localhost on their own host instead of this instance. Cross-instance state-store lookups time out silently. Resolve the advertised hostname at startup (k8s downward API or InetAddress.getLocalHost())."));

        // ── spring-kafka ───────────────────────────────────────────────────────
        addIfEnabled(rules, sev, RuleId.SPRING_LISTENER_ASYNC_ANNOTATION, SpringListenerAsyncRule::new);
        addIfEnabled(rules, sev, RuleId.SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES,
                SpringErrorHandlingDeserializerNoDelegatesRule::new);

        // ── quarkus-kafka ──────────────────────────────────────────────────────
        addIfEnabled(rules, sev, RuleId.QK_BLOCKING_MISSING_ON_BLOCKING_LISTENER,
                QkBlockingMissingOnIncomingRule::new);

        // ── observability/security ─────────────────────────────────────────────
        addIfEnabled(rules, sev, RuleId.DESER_JSON_TYPE_INFO_NO_ALLOWLIST, JacksonDefaultTypingRule::new);
        addIfEnabled(rules, sev, RuleId.SCHEMA_REGISTRY_URL_MISSING, SchemaRegistryUrlMissingRule::new);
        addIfEnabled(rules, sev, RuleId.SECURITY_PROTOCOL_PLAINTEXT, s -> ConfigKeyValueRule.literal(
                RuleId.SECURITY_PROTOCOL_PLAINTEXT, s, KafkaTypes.SECURITY_PROTOCOL_KEY, "PLAINTEXT",
                "security.protocol=PLAINTEXT — records, headers and offset commits travel unencrypted and the broker connection is unauthenticated. Use SASL_SSL on shared brokers."));
        addIfEnabled(rules, sev, RuleId.SECURITY_PROTOCOL_SASL_PLAINTEXT, s -> ConfigKeyValueRule.literal(
                RuleId.SECURITY_PROTOCOL_SASL_PLAINTEXT, s, KafkaTypes.SECURITY_PROTOCOL_KEY, "SASL_PLAINTEXT",
                "security.protocol=SASL_PLAINTEXT — SASL handshake (and record bytes) run over an unencrypted socket. Switch to SASL_SSL."));
        addIfEnabled(rules, sev, RuleId.SSL_ENDPOINT_IDENTIFICATION_DISABLED, s -> ConfigKeyValueRule.literal(
                RuleId.SSL_ENDPOINT_IDENTIFICATION_DISABLED, s, KafkaTypes.SSL_ENDPOINT_ID_ALGO_KEY, "",
                "ssl.endpoint.identification.algorithm=\"\" — hostname verification disabled. Any cert on the trusted chain is accepted regardless of CN/SAN (MITM vector)."));
        addIfEnabled(rules, sev, RuleId.SECURITY_SSL_PROTOCOL_LEGACY, s -> new ConfigKeyValueRule(
                RuleId.SECURITY_SSL_PROTOCOL_LEGACY, s, KafkaTypes.SSL_PROTOCOL_KEY,
                v -> v != null && KafkaTypes.SSL_PROTOCOL_LEGACY_VALUES.contains(v.trim()),
                "ssl.protocol={value} — legacy/broken TLS version. JDK 11+ disables these by default; remove the override and let JSSE negotiate ≥ TLSv1.2."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_EXCLUDE_INTERNAL_TOPICS_FALSE, s -> ConfigKeyValueRule.literal(
                RuleId.CONSUMER_EXCLUDE_INTERNAL_TOPICS_FALSE, s, KafkaTypes.EXCLUDE_INTERNAL_TOPICS_KEY, "false",
                "exclude.internal.topics=false — consumer can subscribe to __consumer_offsets/__transaction_state via regex. Almost always a leftover debug toggle."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_COMPRESSION_GZIP, s -> ConfigKeyValueRule.literal(
                RuleId.PRODUCER_COMPRESSION_GZIP, s, KafkaTypes.COMPRESSION_TYPE_KEY, "gzip",
                "compression.type=gzip — slowest codec for the ratio. Prefer lz4 (throughput), zstd (best ratio, 2.1+) or snappy (cheapest CPU)."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_PARTITION_ASSIGNMENT_LEGACY, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_PARTITION_ASSIGNMENT_LEGACY, s, KafkaTypes.PARTITION_ASSIGNMENT_STRATEGY_KEY,
                v -> {
                    if (v == null) return false;
                    if (v.contains(KafkaTypes.COOPERATIVE_STICKY_ASSIGNOR_FQCN)) return false;
                    return KafkaTypes.LEGACY_PARTITION_ASSIGNORS.stream().anyMatch(v::contains);
                },
                "partition.assignment.strategy={value} — legacy eager-rebalance assignor without CooperativeStickyAssignor. Every restart pauses the whole group."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_BUFFER_MEMORY_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_BUFFER_MEMORY_TOO_HIGH, s, KafkaTypes.BUFFER_MEMORY_KEY,
                v -> parseLongOrZero(v) > 268_435_456L,
                "buffer.memory={value} — above 256 MiB. Producer accumulator is pre-allocated; this can OOM the JVM or stack thundering-herd traffic on broker recovery."));
        addIfEnabled(rules, sev, RuleId.SECURITY_SASL_MECHANISM_PLAIN, s -> ConfigKeyValueRule.literal(
                RuleId.SECURITY_SASL_MECHANISM_PLAIN, s, KafkaTypes.SASL_MECHANISM_KEY, "PLAIN",
                "sasl.mechanism=PLAIN — password sent in cleartext during SASL exchange; switch to SCRAM-SHA-256/512 (or ensure SASL_SSL only)."));
        addIfEnabled(rules, sev, RuleId.SECURITY_SSL_KEYSTORE_TYPE_JKS, s -> ConfigKeyValueRule.literal(
                RuleId.SECURITY_SSL_KEYSTORE_TYPE_JKS, s, KafkaTypes.SSL_KEYSTORE_TYPE_KEY, "JKS",
                "ssl.keystore.type=JKS — proprietary keystore format; switch to PKCS12 (cross-tool, JDK 9+ default)."));
        addIfEnabled(rules, sev, RuleId.CRED_SASL_JAAS_LITERAL, s -> new ConfigKeyValueRule(
                RuleId.CRED_SASL_JAAS_LITERAL, s, KafkaTypes.SASL_JAAS_CONFIG_KEY,
                v -> isLiteralCredential(v) && v.toLowerCase().contains("password=") && !v.contains("password=\"${"),
                "sasl.jaas.config={value} — JAAS string contains an inline password literal. Inject via ${ENV_VAR} placeholder."));
        addIfEnabled(rules, sev, RuleId.CRED_BASIC_AUTH_USER_INFO_LITERAL, s -> new ConfigKeyValueRule(
                RuleId.CRED_BASIC_AUTH_USER_INFO_LITERAL, s, KafkaTypes.BASIC_AUTH_USER_INFO_KEY,
                v -> isLiteralCredential(v) && v.contains(":"),
                "basic.auth.user.info={value} — Schema Registry credentials in a literal user:password. Inject via ${ENV_VAR}."));
        addIfEnabled(rules, sev, RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL, s -> new ConfigKeyValueRule(
                RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL, s, KafkaTypes.SSL_KEYSTORE_PASSWORD_KEY,
                KafkaLinterMojo::isLiteralCredential,
                "ssl.keystore.password={value} — keystore password in source/config. Inject via ${ENV_VAR}."));
        addIfEnabled(rules, sev, RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL, s -> new ConfigKeyValueRule(
                RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL, s, KafkaTypes.SSL_TRUSTSTORE_PASSWORD_KEY,
                KafkaLinterMojo::isLiteralCredential,
                "ssl.truststore.password={value} — truststore password in source/config. Inject via ${ENV_VAR}."));
        addIfEnabled(rules, sev, RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL, s -> new ConfigKeyValueRule(
                RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL, s, KafkaTypes.SSL_KEY_PASSWORD_KEY,
                KafkaLinterMojo::isLiteralCredential,
                "ssl.key.password={value} — private key password in source/config. Inject via ${ENV_VAR}."));
        addIfEnabled(rules, sev, RuleId.SR_AUTO_REGISTER_SCHEMAS_TRUE, s -> ConfigKeyValueRule.literal(
                RuleId.SR_AUTO_REGISTER_SCHEMAS_TRUE, s, KafkaTypes.SR_AUTO_REGISTER_SCHEMAS_KEY, "true",
                "auto.register.schemas=true — producer registers new schemas to the Schema Registry on the fly. Set to false in non-dev environments and register schemas via CI."));
        addIfEnabled(rules, sev, RuleId.SR_USE_LATEST_VERSION_TRUE, s -> ConfigKeyValueRule.literal(
                RuleId.SR_USE_LATEST_VERSION_TRUE, s, KafkaTypes.SR_USE_LATEST_VERSION_KEY, "true",
                "use.latest.version=true — without latest.compatibility.strict=true the serializer can write records under an incompatible latest schema."));
        addIfEnabled(rules, sev, RuleId.SCHEMA_REGISTRY_URL_HTTP, s -> new ConfigKeyValueRule(
                RuleId.SCHEMA_REGISTRY_URL_HTTP, s, KafkaTypes.SCHEMA_REGISTRY_URL_KEY,
                v -> v != null && v.startsWith("http://"),
                "schema.registry.url={value} — over plain HTTP. Schemas and basic-auth credentials leak on the wire. Switch to https://."));

        return rules;
    }

    private List<ProjectScopedRule> buildProjectRules(Map<RuleId, Severity> sev) {
        List<ProjectScopedRule> rules = new ArrayList<>();
        if (sev.get(RuleId.KAFKA_CLIENTS_EOL) != Severity.OFF) {
            rules.add(new KafkaClientsEolRule(sev.get(RuleId.KAFKA_CLIENTS_EOL)));
        }
        if (sev.get(RuleId.KAFKA_CLIENTS_CVE_JNDI_LDAP) != Severity.OFF) {
            rules.add(new KafkaClientsCveJndiLdapRule(sev.get(RuleId.KAFKA_CLIENTS_CVE_JNDI_LDAP)));
        }
        if (sev.get(RuleId.KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER) != Severity.OFF) {
            rules.add(new KafkaClientsCveSaslOAuthRule(sev.get(RuleId.KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER)));
        }
        if (sev.get(RuleId.KAFKA_CLIENTS_CVE_CONFIG_PROVIDER) != Severity.OFF) {
            // Vulnerable: 2.3.0 through 3.5.x, 3.6.0–3.6.2, 3.7.0. Fixed in 3.6.3 / 3.7.1.
            rules.add(new KafkaClientsCveRule(
                    RuleId.KAFKA_CLIENTS_CVE_CONFIG_PROVIDER, sev.get(RuleId.KAFKA_CLIENTS_CVE_CONFIG_PROVIDER),
                    v -> v.lessThan(new SemVer(2, 3, 0)) ? false :
                         v.lessThan(new SemVer(3, 6, 3)) || (v.atLeast(new SemVer(3, 7, 0)) && v.lessThan(new SemVer(3, 7, 1))),
                    "is vulnerable to CVE-2024-31141 (ConfigProvider implicit resolution) — upgrade to >= 3.6.3 (3.6.x line) or >= 3.7.1 (3.7.x line)."));
        }
        if (sev.get(RuleId.KAFKA_CLIENTS_CVE_BUFFER_POOL) != Severity.OFF) {
            // Vulnerable: < 3.9.2, [4.0.0, 4.0.2), [4.1.0, 4.1.2). Fixed in 3.9.2 / 4.0.2 / 4.1.2 / 4.2.x.
            rules.add(new KafkaClientsCveRule(
                    RuleId.KAFKA_CLIENTS_CVE_BUFFER_POOL, sev.get(RuleId.KAFKA_CLIENTS_CVE_BUFFER_POOL),
                    v -> v.lessThan(new SemVer(3, 9, 2))
                       || (v.atLeast(new SemVer(4, 0, 0)) && v.lessThan(new SemVer(4, 0, 2)))
                       || (v.atLeast(new SemVer(4, 1, 0)) && v.lessThan(new SemVer(4, 1, 2))),
                    "is vulnerable to CVE-2026-35554 (BufferPool reuse can deliver records to the wrong topic) — upgrade to >= 3.9.2 / 4.0.2 / 4.1.2 / 4.2.x."));
        }
        if (sev.get(RuleId.KAFKA_CLIENTS_CVE_SCRAM_REPLAY) != Severity.OFF) {
            // Vulnerable: < 3.9.1. Fix landed in 3.9.1 and 4.x lines.
            rules.add(new KafkaClientsCveRule(
                    RuleId.KAFKA_CLIENTS_CVE_SCRAM_REPLAY, sev.get(RuleId.KAFKA_CLIENTS_CVE_SCRAM_REPLAY),
                    v -> v.lessThan(new SemVer(3, 9, 1)),
                    "has the SCRAM nonce-reuse flaw (CVE-2024-56128) — upgrade to >= 3.9.1 and prefer SASL_SSL on the wire."));
        }
        if (sev.get(RuleId.JAVA_VERSION_TOO_LOW) != Severity.OFF) {
            rules.add(new JavaVersionTooLowRule(sev.get(RuleId.JAVA_VERSION_TOO_LOW)));
        }
        if (sev.get(RuleId.QUARKUS_KAFKA_EXTENSION_RENAMED) != Severity.OFF) {
            rules.add(new QuarkusKafkaExtensionRenamedRule(sev.get(RuleId.QUARKUS_KAFKA_EXTENSION_RENAMED)));
        }
        if (sev.get(RuleId.SPRING_KAFKA_BOOT_MISMATCH) != Severity.OFF) {
            rules.add(new SpringKafkaBootMismatchRule(sev.get(RuleId.SPRING_KAFKA_BOOT_MISMATCH)));
        }
        if (sev.get(RuleId.QK_DEVSERVICES_IN_PROD) != Severity.OFF) {
            rules.add(new QkDevservicesInProdRule(sev.get(RuleId.QK_DEVSERVICES_IN_PROD)));
        }
        if (sev.get(RuleId.QK_COMMIT_STRATEGY_IGNORE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_COMMIT_STRATEGY_IGNORE, sev.get(RuleId.QK_COMMIT_STRATEGY_IGNORE),
                    "incoming", "commit-strategy", "ignore",
                    "mp.messaging.incoming.{channel}.commit-strategy=ignore — offsets never committed; restart re-reads everything."));
        }
        if (sev.get(RuleId.QK_FAILURE_STRATEGY_IGNORE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_FAILURE_STRATEGY_IGNORE, sev.get(RuleId.QK_FAILURE_STRATEGY_IGNORE),
                    "incoming", "failure-strategy", "ignore",
                    "mp.messaging.incoming.{channel}.failure-strategy=ignore — processing exceptions are swallowed and offsets advance. Use fail or dead-letter-queue."));
        }
        if (sev.get(RuleId.QK_AUTO_OFFSET_RESET_LATEST) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_AUTO_OFFSET_RESET_LATEST, sev.get(RuleId.QK_AUTO_OFFSET_RESET_LATEST),
                    "incoming", "auto.offset.reset", "latest",
                    "mp.messaging.incoming.{channel}.auto.offset.reset=latest — fresh consumer group skips existing backlog. Prefer 'earliest' for pipeline consumers."));
        }
        if (sev.get(RuleId.QK_INCOMING_AUTO_OFFSET_RESET_NONE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_INCOMING_AUTO_OFFSET_RESET_NONE, sev.get(RuleId.QK_INCOMING_AUTO_OFFSET_RESET_NONE),
                    "incoming", "auto.offset.reset", "none",
                    "mp.messaging.incoming.{channel}.auto.offset.reset=none — fresh consumer group throws NoOffsetForPartitionException and the pod crashloops on first deploy. Use 'earliest' or 'latest' unless you manage offsets manually."));
        }
        if (sev.get(RuleId.QK_BOOTSTRAP_SERVERS_LOCALHOST) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.QK_BOOTSTRAP_SERVERS_LOCALHOST, sev.get(RuleId.QK_BOOTSTRAP_SERVERS_LOCALHOST),
                    "kafka.bootstrap.servers",
                    v -> v != null && (v.contains("localhost") || v.contains("127.0.0.1") || v.contains("0.0.0.0")),
                    "kafka.bootstrap.servers={value} — points at the pod's own loopback. Externalize via env var or %prod-prefixed override.",
                    "io.quarkus", "quarkus-smallrye-reactive-messaging-kafka"));
        }
        if (sev.get(RuleId.QK_AUTO_COMMIT_ENABLED) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_AUTO_COMMIT_ENABLED, sev.get(RuleId.QK_AUTO_COMMIT_ENABLED),
                    "incoming", "enable.auto.commit", "true",
                    "mp.messaging.incoming.{channel}.enable.auto.commit=true — Kafka commits offsets on a timer regardless of ack. In-flight polled records are lost on crash."));
        }
        if (sev.get(RuleId.QK_TRACING_DISABLED) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_TRACING_DISABLED, sev.get(RuleId.QK_TRACING_DISABLED),
                    null, "tracing-enabled", "false",
                    "mp.messaging.{direction}.{channel}.tracing-enabled=false — traceparent header propagation off for this channel. Distributed traces will not cross this Kafka hop."));
        }
        if (sev.get(RuleId.QK_HEALTH_DISABLED) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_HEALTH_DISABLED, sev.get(RuleId.QK_HEALTH_DISABLED),
                    null, "health-enabled", "false",
                    "mp.messaging.{direction}.{channel}.health-enabled=false — channel excluded from health checks; k8s won't see it as unhealthy."));
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_HEALTH_DISABLED, sev.get(RuleId.QK_HEALTH_DISABLED),
                    null, "health-readiness-enabled", "false",
                    "mp.messaging.{direction}.{channel}.health-readiness-enabled=false — channel excluded from readiness; pod will stay Ready while the channel is broken."));
        }
        if (sev.get(RuleId.QK_GRACEFUL_SHUTDOWN_DISABLED) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_GRACEFUL_SHUTDOWN_DISABLED, sev.get(RuleId.QK_GRACEFUL_SHUTDOWN_DISABLED),
                    "incoming", "graceful-shutdown", "false",
                    "mp.messaging.incoming.{channel}.graceful-shutdown=false — pod termination drops in-flight polled records; rolling deploys produce duplicates."));
        }
        if (sev.get(RuleId.QK_RETRIES_ZERO) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_RETRIES_ZERO, sev.get(RuleId.QK_RETRIES_ZERO),
                    "outgoing", "retries", "0",
                    "mp.messaging.outgoing.{channel}.retries=0 — transient broker errors become permanent send failures. Default (effectively unbounded, capped by delivery.timeout.ms) is right."));
        }
        if (sev.get(RuleId.QK_OUTGOING_ACKS_NOT_ALL) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.predicate(
                    RuleId.QK_OUTGOING_ACKS_NOT_ALL, sev.get(RuleId.QK_OUTGOING_ACKS_NOT_ALL),
                    "outgoing", "acks",
                    v -> v != null && !"all".equalsIgnoreCase(v.trim()) && !"-1".equals(v.trim()),
                    "mp.messaging.outgoing.{channel}.acks={value} — partial durability. Set to 'all' (default since kafka-clients 3.0) or remove the override."));
        }
        if (sev.get(RuleId.QK_OUTGOING_WAIT_FOR_WRITE_COMPLETION_FALSE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_OUTGOING_WAIT_FOR_WRITE_COMPLETION_FALSE, sev.get(RuleId.QK_OUTGOING_WAIT_FOR_WRITE_COMPLETION_FALSE),
                    "outgoing", "waitForWriteCompletion", "false",
                    "mp.messaging.outgoing.{channel}.waitForWriteCompletion=false — channel acks upstream before the broker accepts the record. Broker failures become silent drops."));
        }
        if (sev.get(RuleId.QK_FAIL_ON_DESERIALIZATION_FAILURE_FALSE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_FAIL_ON_DESERIALIZATION_FAILURE_FALSE, sev.get(RuleId.QK_FAIL_ON_DESERIALIZATION_FAILURE_FALSE),
                    "incoming", "fail-on-deserialization-failure", "false",
                    "mp.messaging.incoming.{channel}.fail-on-deserialization-failure=false — undeserializable records are logged once at WARN, replaced by null, and the offset advances. Silent data loss; configure a dead-letter-queue strategy instead."));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_ACKS_NOT_ALL) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_ACKS_NOT_ALL, sev.get(RuleId.SPRING_BOOT_PRODUCER_ACKS_NOT_ALL),
                    "spring.kafka.producer.acks",
                    v -> "0".equals(v) || "1".equals(v),
                    "spring.kafka.producer.acks={value} — durability is partial. Use 'all' (or omit and rely on the kafka-clients default since 3.0).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST, sev.get(RuleId.SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST),
                    "spring.kafka.bootstrap-servers",
                    v -> v != null && (v.contains("localhost") || v.contains("127.0.0.1") || v.contains("0.0.0.0")),
                    "spring.kafka.bootstrap-servers={value} — points at localhost. Externalize via env var or profile-suffixed override.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_ENABLE_AUTO_COMMIT_TRUE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_ENABLE_AUTO_COMMIT_TRUE, sev.get(RuleId.SPRING_BOOT_ENABLE_AUTO_COMMIT_TRUE),
                    "spring.kafka.consumer.enable-auto-commit", "true",
                    "spring.kafka.consumer.enable-auto-commit=true — disables the listener container's ack-mode commit machinery. Set to false and use manual ack-mode.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_AUTO_OFFSET_RESET_LATEST) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_AUTO_OFFSET_RESET_LATEST, sev.get(RuleId.SPRING_BOOT_AUTO_OFFSET_RESET_LATEST),
                    "spring.kafka.consumer.auto-offset-reset", "latest",
                    "spring.kafka.consumer.auto-offset-reset=latest — fresh consumer groups skip everything currently in the topic. Prefer 'earliest' for pipeline consumers.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_COMPRESSION_NONE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_PRODUCER_COMPRESSION_NONE, sev.get(RuleId.SPRING_BOOT_PRODUCER_COMPRESSION_NONE),
                    "spring.kafka.producer.compression-type", "none",
                    "spring.kafka.producer.compression-type=none — explicit opt-out of compression. Use zstd or lz4.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_RETRIES_ZERO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_PRODUCER_RETRIES_ZERO, sev.get(RuleId.SPRING_BOOT_PRODUCER_RETRIES_ZERO),
                    "spring.kafka.producer.retries", "0",
                    "spring.kafka.producer.retries=0 — transient broker errors become permanent send failures. Remove the override; defaults are correct.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_AUTO_COMMIT_INTERVAL_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_AUTO_COMMIT_INTERVAL_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_AUTO_COMMIT_INTERVAL_TOO_HIGH),
                    "spring.kafka.consumer.auto-commit-interval",
                    v -> { try { return v != null && Long.parseLong(v.trim()) > 60000L; } catch (NumberFormatException e) { return false; } },
                    "spring.kafka.consumer.auto-commit-interval={value} — above 60 s. Paired with auto-commit=true the loss window after a crash is this many ms.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_GENERIC) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_GENERIC,
                    sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_GENERIC),
                    "spring.kafka.producer.transaction-id-prefix",
                    v -> {
                        if (v == null) return false;
                        String trimmed = v.trim().toLowerCase();
                        if (trimmed.endsWith("-") || trimmed.endsWith(".")) {
                            trimmed = trimmed.substring(0, trimmed.length() - 1);
                        }
                        return KafkaTypes.PRODUCER_GENERIC_TRANSACTIONAL_IDS.contains(trimmed);
                    },
                    "spring.kafka.producer.transaction-id-prefix={value} — a generic placeholder. Two apps with this prefix will fence each other across restarts.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_GROUP_ID_GENERIC) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_GROUP_ID_GENERIC, sev.get(RuleId.SPRING_BOOT_CONSUMER_GROUP_ID_GENERIC),
                    "spring.kafka.consumer.group-id",
                    v -> v != null && KafkaTypes.CONSUMER_GENERIC_GROUP_IDS.contains(v.trim().toLowerCase()),
                    "spring.kafka.consumer.group-id={value} — a generic placeholder. Two apps that share this group-id will collide on partition assignment and silently corrupt each other's offsets.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_RECORDS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_RECORDS_TOO_LOW, sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_RECORDS_TOO_LOW),
                    "spring.kafka.consumer.max-poll-records",
                    v -> { try { long n = v == null ? 0 : Long.parseLong(v.trim()); return n >= 1 && n <= 5; } catch (NumberFormatException e) { return false; } },
                    "spring.kafka.consumer.max-poll-records={value} — at or below 5. Each KafkaListener poll returns at most a handful of records; per-poll listener-container overhead dominates and throughput collapses 50-100× vs the default 500.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_IDEMPOTENCE_FALSE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_PRODUCER_IDEMPOTENCE_FALSE, sev.get(RuleId.SPRING_BOOT_PRODUCER_IDEMPOTENCE_FALSE),
                    "spring.kafka.producer.properties.enable.idempotence", "false",
                    "spring.kafka.producer.properties.enable.idempotence=false — explicit opt-out of the idempotent producer (default since kafka-clients 3.0). Retries can write duplicates and reorder records. Remove the override.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_AUTO_STARTUP_FALSE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_LISTENER_AUTO_STARTUP_FALSE, sev.get(RuleId.SPRING_BOOT_LISTENER_AUTO_STARTUP_FALSE),
                    "spring.kafka.listener.auto-startup", "false",
                    "spring.kafka.listener.auto-startup=false — every @KafkaListener container is constructed but idle until application code calls KafkaListenerEndpointRegistry.start(). Records pile up on Kafka with no consumer activity and no error logged. Usually a leaked test config.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_ACK_MODE_RECORD) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_LISTENER_ACK_MODE_RECORD, sev.get(RuleId.SPRING_BOOT_LISTENER_ACK_MODE_RECORD),
                    "spring.kafka.listener.ack-mode",
                    v -> v != null && "record".equalsIgnoreCase(v.trim()),
                    "spring.kafka.listener.ack-mode={value} — Spring commits the offset synchronously after every record. Per-record commit RTT (1-3 ms) becomes the dominant cost; throughput collapses 10-100× vs the default BATCH mode while the duplicate-on-crash window narrows by milliseconds. Use idempotent handlers instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_CONCURRENCY_ZERO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_LISTENER_CONCURRENCY_ZERO, sev.get(RuleId.SPRING_BOOT_LISTENER_CONCURRENCY_ZERO),
                    "spring.kafka.listener.concurrency", "0",
                    "spring.kafka.listener.concurrency=0 — listener container creates zero consumer threads. Almost always a typo or env-substitution bug.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_TEMPLATE_OBSERVATION_DISABLED) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_TEMPLATE_OBSERVATION_DISABLED, sev.get(RuleId.SPRING_BOOT_TEMPLATE_OBSERVATION_DISABLED),
                    "spring.kafka.template.observation-enabled", "false",
                    "spring.kafka.template.observation-enabled=false — KafkaTemplate produces drop out of distributed traces. Remove the override; Spring Boot 3.2+ default is true.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_OBSERVATION_DISABLED) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_LISTENER_OBSERVATION_DISABLED, sev.get(RuleId.SPRING_BOOT_LISTENER_OBSERVATION_DISABLED),
                    "spring.kafka.listener.observation-enabled", "false",
                    "spring.kafka.listener.observation-enabled=false — @KafkaListener invocations drop out of distributed traces. Remove the override; Spring Boot 3.2+ default is true.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_BATCH_SIZE_TOO_SMALL) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_BATCH_SIZE_TOO_SMALL, sev.get(RuleId.SPRING_BOOT_PRODUCER_BATCH_SIZE_TOO_SMALL),
                    "spring.kafka.producer.batch-size",
                    v -> {
                        if (v == null) return false;
                        try { return Integer.parseInt(v.trim()) < 16384; }
                        catch (NumberFormatException e) { return false; }
                    },
                    "spring.kafka.producer.batch-size={value} — below the 16 KiB default. Under-batched produces lose compression and inflate broker request rate.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_JSON_TRUSTED_PACKAGES_WILDCARD) != Severity.OFF) {
            Severity s = sev.get(RuleId.SPRING_BOOT_JSON_TRUSTED_PACKAGES_WILDCARD);
            String detail = "spring.json.trusted.packages={value} — wildcard trust lets a producer load any FQCN via __TypeId__ header. Set to an explicit allow-list of your own packages.";
            for (String key : new String[]{
                    "spring.kafka.consumer.properties.spring.json.trusted.packages",
                    "spring.kafka.producer.properties.spring.json.trusted.packages",
                    "spring.kafka.properties.spring.json.trusted.packages"}) {
                rules.add(PropertyFileRule.literal(
                        RuleId.SPRING_BOOT_JSON_TRUSTED_PACKAGES_WILDCARD, s,
                        key, "*", detail,
                        "org.springframework.kafka", "spring-kafka"));
            }
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_LINGER_MS_ZERO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_PRODUCER_LINGER_MS_ZERO, sev.get(RuleId.SPRING_BOOT_PRODUCER_LINGER_MS_ZERO),
                    "spring.kafka.producer.properties.linger.ms", "0",
                    "spring.kafka.producer.properties.linger.ms=0 — explicit no-batching. Even linger.ms=5 keeps p99 latency flat while restoring batch efficiency.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SECURITY_PROTOCOL_PLAINTEXT) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SECURITY_PROTOCOL_PLAINTEXT, sev.get(RuleId.SECURITY_PROTOCOL_PLAINTEXT),
                    "spring.kafka.properties.security.protocol", "PLAINTEXT",
                    "spring.kafka.properties.security.protocol=PLAINTEXT — Kafka traffic is unencrypted and unauthenticated. Use SASL_SSL on shared brokers.",
                    "org.springframework.kafka", "spring-kafka"));
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.SECURITY_PROTOCOL_PLAINTEXT, sev.get(RuleId.SECURITY_PROTOCOL_PLAINTEXT),
                    null, "security.protocol", "PLAINTEXT",
                    "mp.messaging.{direction}.{channel}.security.protocol=PLAINTEXT — channel speaks unencrypted Kafka. Use SASL_SSL."));
        }
        if (sev.get(RuleId.SECURITY_PROTOCOL_SASL_PLAINTEXT) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SECURITY_PROTOCOL_SASL_PLAINTEXT, sev.get(RuleId.SECURITY_PROTOCOL_SASL_PLAINTEXT),
                    "spring.kafka.properties.security.protocol", "SASL_PLAINTEXT",
                    "spring.kafka.properties.security.protocol=SASL_PLAINTEXT — SASL credentials and records ride an unencrypted socket. Switch to SASL_SSL.",
                    "org.springframework.kafka", "spring-kafka"));
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.SECURITY_PROTOCOL_SASL_PLAINTEXT, sev.get(RuleId.SECURITY_PROTOCOL_SASL_PLAINTEXT),
                    null, "security.protocol", "SASL_PLAINTEXT",
                    "mp.messaging.{direction}.{channel}.security.protocol=SASL_PLAINTEXT — channel uses SASL over plaintext. Switch to SASL_SSL."));
        }
        if (sev.get(RuleId.SSL_ENDPOINT_IDENTIFICATION_DISABLED) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SSL_ENDPOINT_IDENTIFICATION_DISABLED, sev.get(RuleId.SSL_ENDPOINT_IDENTIFICATION_DISABLED),
                    "spring.kafka.properties.ssl.endpoint.identification.algorithm", "",
                    "spring.kafka.properties.ssl.endpoint.identification.algorithm=\"\" — hostname verification turned off (MITM vector).",
                    "org.springframework.kafka", "spring-kafka"));
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.SSL_ENDPOINT_IDENTIFICATION_DISABLED, sev.get(RuleId.SSL_ENDPOINT_IDENTIFICATION_DISABLED),
                    null, "ssl.endpoint.identification.algorithm", "",
                    "mp.messaging.{direction}.{channel}.ssl.endpoint.identification.algorithm=\"\" — hostname verification turned off (MITM vector)."));
        }
        if (sev.get(RuleId.SR_AUTO_REGISTER_SCHEMAS_TRUE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SR_AUTO_REGISTER_SCHEMAS_TRUE, sev.get(RuleId.SR_AUTO_REGISTER_SCHEMAS_TRUE),
                    "spring.kafka.properties.auto.register.schemas", "true",
                    "spring.kafka.properties.auto.register.schemas=true — producer can register new schemas at runtime. Set to false outside dev and use a CI step.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SR_USE_LATEST_VERSION_TRUE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SR_USE_LATEST_VERSION_TRUE, sev.get(RuleId.SR_USE_LATEST_VERSION_TRUE),
                    "spring.kafka.properties.use.latest.version", "true",
                    "spring.kafka.properties.use.latest.version=true — without latest.compatibility.strict, the serializer may write records the consumers can't read.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.CRED_SASL_JAAS_LITERAL) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.CRED_SASL_JAAS_LITERAL, sev.get(RuleId.CRED_SASL_JAAS_LITERAL),
                    "spring.kafka.properties.sasl.jaas.config",
                    v -> isLiteralCredential(v) && v.toLowerCase().contains("password=") && !v.contains("password=\"${"),
                    "spring.kafka.properties.sasl.jaas.config={value} — JAAS string contains an inline password literal. Inject via ${ENV_VAR}.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.CRED_BASIC_AUTH_USER_INFO_LITERAL) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.CRED_BASIC_AUTH_USER_INFO_LITERAL, sev.get(RuleId.CRED_BASIC_AUTH_USER_INFO_LITERAL),
                    "spring.kafka.properties.basic.auth.user.info",
                    v -> isLiteralCredential(v) && v.contains(":"),
                    "spring.kafka.properties.basic.auth.user.info={value} — Schema Registry credentials in a literal user:password. Inject via ${ENV_VAR}.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL, sev.get(RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL),
                    "spring.kafka.ssl.key-store-password",
                    KafkaLinterMojo::isLiteralCredential,
                    "spring.kafka.ssl.key-store-password={value} — keystore password in source/config. Inject via ${ENV_VAR}.",
                    "org.springframework.kafka", "spring-kafka"));
            rules.add(PropertyFileRule.predicate(
                    RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL, sev.get(RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL),
                    "spring.kafka.ssl.trust-store-password",
                    KafkaLinterMojo::isLiteralCredential,
                    "spring.kafka.ssl.trust-store-password={value} — truststore password in source/config. Inject via ${ENV_VAR}.",
                    "org.springframework.kafka", "spring-kafka"));
            rules.add(PropertyFileRule.predicate(
                    RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL, sev.get(RuleId.CRED_SSL_KEYSTORE_PASSWORD_LITERAL),
                    "spring.kafka.ssl.key-password",
                    KafkaLinterMojo::isLiteralCredential,
                    "spring.kafka.ssl.key-password={value} — private key password in source/config. Inject via ${ENV_VAR}.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SCHEMA_REGISTRY_URL_HTTP) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SCHEMA_REGISTRY_URL_HTTP, sev.get(RuleId.SCHEMA_REGISTRY_URL_HTTP),
                    "spring.kafka.properties.schema.registry.url",
                    v -> v != null && v.startsWith("http://"),
                    "spring.kafka.properties.schema.registry.url={value} — Schema Registry over plain HTTP. Schemas and basic-auth credentials leak. Use https://.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        return rules;
    }

    private static void addIfEnabled(List<Rule> rules, Map<RuleId, Severity> sev, RuleId id,
                                     java.util.function.Function<Severity, Rule> ctor) {
        Severity s = sev.get(id);
        if (s == null || s == Severity.OFF) return;
        rules.add(ctor.apply(s));
    }

    private static int parseIntOrZero(String s) {
        if (s == null) return 0;
        try { return Integer.parseInt(s.trim()); } catch (NumberFormatException e) { return 0; }
    }

    private static long parseLongOrZero(String s) {
        if (s == null) return 0L;
        try { return Long.parseLong(s.trim()); } catch (NumberFormatException e) { return 0L; }
    }

    /** True when v is a non-empty literal credential — not blank, not a ${...} placeholder. */
    private static boolean isLiteralCredential(String v) {
        if (v == null) return false;
        String t = v.trim();
        if (t.isEmpty()) return false;
        return !t.contains("${");
    }

    /** True when v carries an unresolved ${...} placeholder — a Java-side bug, since plain Java does not expand templates. */
    private static boolean looksLikeUnresolvedPlaceholder(String v) {
        if (v == null) return false;
        int open = v.indexOf("${");
        if (open < 0) return false;
        int close = v.indexOf('}', open + 2);
        return close > open + 2;
    }
}
