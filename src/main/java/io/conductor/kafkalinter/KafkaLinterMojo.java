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
}
