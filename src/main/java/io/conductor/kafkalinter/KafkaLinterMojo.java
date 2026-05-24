package io.conductor.kafkalinter;

import io.conductor.kafkalinter.report.Reporter;
import io.conductor.kafkalinter.rules.ConsumerAutoCommitTrueRule;
import io.conductor.kafkalinter.rules.ConsumerCommitPerRecordRule;
import io.conductor.kafkalinter.rules.AdminCloseZeroDurationRule;
import io.conductor.kafkalinter.rules.ConsumerCloseZeroDurationRule;
import io.conductor.kafkalinter.rules.ConsumerPollInfiniteDurationRule;
import io.conductor.kafkalinter.rules.ConsumerPollZeroRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPauseNoResumeRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPollResultIgnoredRule;
import io.conductor.kafkalinter.rules.ConsumerSubscribeInLoopRule;
import io.conductor.kafkalinter.rules.ProducerCloseZeroDurationRule;
import io.conductor.kafkalinter.rules.ProducerFlushInLoopRule;
import io.conductor.kafkalinter.rules.ProducerInLoopRule;
import io.conductor.kafkalinter.rules.ProducerNoCompressionRule;
import io.conductor.kafkalinter.rules.ProducerSendBlockingGetRule;
import io.conductor.kafkalinter.rules.ProducerSendNoCallbackRule;
import io.conductor.kafkalinter.rules.ProducerSendNullCallbackRule;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.rules.clients.AdminCloseNoTimeoutRule;
import io.conductor.kafkalinter.rules.clients.AdminUsedAfterCloseRule;
import io.conductor.kafkalinter.rules.clients.AdminNewTopicReplicationFactorOneRule;
import io.conductor.kafkalinter.rules.clients.AdminNotClosedRule;
import io.conductor.kafkalinter.rules.clients.AvroSpecificReaderMissingRule;
import io.conductor.kafkalinter.rules.clients.ConsumerIsolationReadUncommittedWithTxnRule;
import io.conductor.kafkalinter.rules.connect.ConnectAvroAutoRegisterSchemasTrueRule;
import io.conductor.kafkalinter.rules.connect.ConnectConfigProviderReferenceUndefinedRule;
import io.conductor.kafkalinter.rules.connect.ConnectConsumerOverrideGroupIdRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumIncludeAndExcludeListBothSetRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumIncrementalSnapshotWithoutSignalChannelRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumMysqlServerIdRandomRelianceRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumMysqlServerIdSharedRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumPostgresFilteredPublicationHeartbeatDisabledRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumPostgresPublicationNameSharedRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumPostgresSlotNameSharedRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumPostgresTasksMaxGreaterThanOneRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumMysqlTasksMaxGreaterThanOneRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumOracleTasksMaxGreaterThanOneRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumDb2TasksMaxGreaterThanOneRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumSnapshotModeNeverRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumMysqlSnapshotLockingModeNoneRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumPostgresPluginNameDeprecatedRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumMysqlIncludeQueryTrueRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumSqlServerDatabaseDbnameDeprecatedRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumLegacySchemaHistoryKeysRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumDatabaseServerNameDeprecatedRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumValueConverterStringRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumKeyConverterStringRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumSchemaHistoryTopicSharedRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumTopicPrefixSharedRule;
import io.conductor.kafkalinter.rules.connect.ConnectDlqContextHeadersDisabledRule;
import io.conductor.kafkalinter.rules.connect.ConnectDlqReplicationFactorLowRule;
import io.conductor.kafkalinter.rules.connect.ConnectDlqTopicEqualsInputTopicRule;
import io.conductor.kafkalinter.rules.connect.ConnectFileStreamDemoConnectorRule;
import io.conductor.kafkalinter.rules.connect.ConnectGcsSinkFlushSizeHugeWithoutTimeRotateRule;
import io.conductor.kafkalinter.rules.connect.ConnectHdfsSinkFlushSizeHugeWithoutTimeRotateRule;
import io.conductor.kafkalinter.rules.connect.ConnectJsonConverterSchemasEnableUnsetRule;
import io.conductor.kafkalinter.rules.connect.ConnectNameMissingRule;
import io.conductor.kafkalinter.rules.connect.ConnectSinkTopicsAndTopicsRegexBothSetRule;
import io.conductor.kafkalinter.rules.connect.ConnectSinkTopicsAndTopicsRegexNeitherSetRule;
import io.conductor.kafkalinter.rules.connect.ConnectTasksMaxAbsentRule;
import io.conductor.kafkalinter.rules.connect.ConnectTasksMaxLessThanOneRule;
import io.conductor.kafkalinter.rules.connect.ConnectTransformRegexRouterMissingRegexOrReplacementRule;
import io.conductor.kafkalinter.rules.connect.ConnectErrorsRetryTimeoutAbsentRule;
import io.conductor.kafkalinter.rules.connect.ConnectErrorsToleranceAbsentRule;
import io.conductor.kafkalinter.rules.connect.ConnectErrorsToleranceAllNoDlqRule;
import io.conductor.kafkalinter.rules.connect.ConnectErrorsToleranceAllNoLogRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumDecimalHandlingModeDoubleRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumEventProcessingFailureHandlingModeSkipOrWarnRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumSchemaHistoryInternalSkipUnparseableDdlTrueRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumSkippedOperationsDropsDataRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumKeyConverterByteArrayRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumMaxQueueSizeLessThanMaxBatchSizeRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumTimePrecisionModeConnectRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumTombstonesOnDeleteDisabledRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumValueConverterByteArrayRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumPostgresPublicationAutocreateModeAllTablesRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumPostgresSlotDropOnStopTrueRule;
import io.conductor.kafkalinter.rules.connect.ConnectDebeziumPublicationFilteredMissingFilterListRule;
import io.conductor.kafkalinter.rules.connect.ConnectJdbcSinkAutoEvolveTrueWithoutAutoCreateTrueRule;
import io.conductor.kafkalinter.rules.connect.ConnectJdbcSinkDeleteEnabledTrueWithoutPkModeRecordKeyRule;
import io.conductor.kafkalinter.rules.connect.ConnectJdbcSinkTableNameFormatWithoutTopicPlaceholderRule;
import io.conductor.kafkalinter.rules.connect.ConnectJdbcSinkUpsertOrUpdateWithoutPkRule;
import io.conductor.kafkalinter.rules.connect.ConnectJdbcSourceModeColumnMissingRule;
import io.conductor.kafkalinter.rules.connect.ConnectJdbcSourcePollIntervalMsTooLowRule;
import io.conductor.kafkalinter.rules.connect.ConnectJdbcSourceQueryAndTableBothSetRule;
import io.conductor.kafkalinter.rules.connect.ConnectJdbcSourceValidateNonNullFalseRule;
import io.conductor.kafkalinter.rules.connect.ConnectPredicateDefinedButNotListedRule;
import io.conductor.kafkalinter.rules.connect.ConnectPredicateReferenceUndefinedRule;
import io.conductor.kafkalinter.rules.connect.ConnectProducerEnableIdempotenceFalseRule;
import io.conductor.kafkalinter.rules.connect.ConnectS3SinkFlushSizeHugeWithoutTimeRotateRule;
import io.conductor.kafkalinter.rules.connect.ConnectS3SinkFlushSizeTooSmallRule;
import io.conductor.kafkalinter.rules.connect.ConnectS3SinkS3PartSizeTooSmallRule;
import io.conductor.kafkalinter.rules.connect.ConnectSchemaRegistryConverterMissingUrlRule;
import io.conductor.kafkalinter.rules.connect.ConnectSinkAutoCommitTrueRule;
import io.conductor.kafkalinter.rules.connect.ConnectSinkConsumerAutoOffsetResetLatestRule;
import io.conductor.kafkalinter.rules.connect.ConnectStorageSinkFieldPartitionerWithoutPartitionFieldNameRule;
import io.conductor.kafkalinter.rules.connect.ConnectStorageSinkPathFormatWithoutTimePartitionerRule;
import io.conductor.kafkalinter.rules.connect.ConnectStorageSinkPartitionDurationMsTooLowRule;
import io.conductor.kafkalinter.rules.connect.ConnectStorageSinkRotateIntervalMsTooLowRule;
import io.conductor.kafkalinter.rules.connect.ConnectStorageSinkTimestampExtractorWallclockRule;
import io.conductor.kafkalinter.rules.connect.ConnectTopicCreationGroupDefinedButNotListedRule;
import io.conductor.kafkalinter.rules.connect.ConnectTransformDefinedButNotListedRule;
import io.conductor.kafkalinter.rules.connect.ConnectTransformFilterWithoutPredicateRule;
import io.conductor.kafkalinter.rules.connect.ConnectRestAdvertisedHostNameLocalhostRule;
import io.conductor.kafkalinter.rules.connect.ConnectSourceProducerAcksNotAllRule;
import io.conductor.kafkalinter.rules.connect.ConnectTransformAliasUndefinedRule;
import io.conductor.kafkalinter.rules.connect.ConnectWorkerInternalTopicReplicationFactorLowRule;
import io.conductor.kafkalinter.rules.connect.Mm2BlacklistDeprecatedRule;
import io.conductor.kafkalinter.rules.connect.Mm2ClusterBootstrapServersLocalhostRule;
import io.conductor.kafkalinter.rules.connect.Mm2EmitCheckpointsDisabledRule;
import io.conductor.kafkalinter.rules.connect.Mm2EmitHeartbeatsDisabledRule;
import io.conductor.kafkalinter.rules.connect.Mm2InternalTopicReplicationFactorLowRule;
import io.conductor.kafkalinter.rules.connect.Mm2RefreshTopicsIntervalTooLowRule;
import io.conductor.kafkalinter.rules.connect.Mm2SourceConnectorTasksMaxOneRule;
import io.conductor.kafkalinter.rules.connect.Mm2SyncTopicAclsDisabledRule;
import io.conductor.kafkalinter.rules.connect.Mm2SyncTopicConfigsDisabledRule;
import io.conductor.kafkalinter.rules.clients.ProducerBufferMemoryMisconfigRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesAcksAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesBootstrapServersAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesBatchSizeAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesBufferMemoryAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesClientIdAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesCompressionTypeAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesDeliveryTimeoutMsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesEnableIdempotenceAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesLingerMsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesRequestTimeoutMsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerPropertiesMaxBlockMsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ProducerFlushInCallbackRule;
import io.conductor.kafkalinter.rules.clients.ProducerCloseInCallbackRule;
import io.conductor.kafkalinter.rules.clients.ProducerSendInCallbackRule;
import io.conductor.kafkalinter.rules.clients.ProducerInitTransactionsNotCalledRule;
import io.conductor.kafkalinter.rules.clients.ProducerBeginTransactionNoAbortRule;
import io.conductor.kafkalinter.rules.clients.ClientIdMissingRule;
import io.conductor.kafkalinter.rules.clients.CommitAsyncNoFinalSyncRule;
import io.conductor.kafkalinter.rules.clients.ConsumerAssignAndSubscribeRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesAutoOffsetResetAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesBootstrapServersAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesClientIdAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesClientRackAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesPartitionAssignmentStrategyAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesHeartbeatIntervalMsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesFetchMaxWaitMsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesRequestTimeoutMsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesEnableAutoCommitAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesFetchMinBytesAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesGroupIdAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesIsolationLevelAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesMaxPollIntervalMsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesMaxPollRecordsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerPropertiesSessionTimeoutMsAbsentRule;
import io.conductor.kafkalinter.rules.clients.ConsumerHeartbeatSessionRatioRule;
import io.conductor.kafkalinter.rules.clients.ConsumerNotThreadSafeRule;
import io.conductor.kafkalinter.rules.clients.ConsumerNoWakeupShutdownRule;
import io.conductor.kafkalinter.rules.clients.ConsumerCloseNoTimeoutRule;
import io.conductor.kafkalinter.rules.clients.ConsumerNotClosedRule;
import io.conductor.kafkalinter.rules.clients.ConsumerSeekBeforePollRule;
import io.conductor.kafkalinter.rules.clients.HeadersSensitiveKeysRule;
import io.conductor.kafkalinter.rules.clients.SecurityProtocolPlaintextRemoteRule;
import io.conductor.kafkalinter.rules.clients.KafkaClientMetadataRecoveryStrategyAbsentRule;
import io.conductor.kafkalinter.rules.clients.KafkaClientTypoGroupIdRule;
import io.conductor.kafkalinter.rules.clients.ConsumerCommitAsyncInRebalanceRule;
import io.conductor.kafkalinter.rules.clients.KafkaClientInStaticInitializerRule;
import io.conductor.kafkalinter.rules.clients.PollInRebalanceCallbackRule;
import io.conductor.kafkalinter.rules.clients.ProducerMaxInFlightTooHighRule;
import io.conductor.kafkalinter.rules.clients.PropertiesMutatedAfterCtorRule;
import io.conductor.kafkalinter.rules.clients.ProducerCloseNoTimeoutRule;
import io.conductor.kafkalinter.rules.clients.ProducerNotClosedRule;
import io.conductor.kafkalinter.rules.clients.ProducerPerRecordAllocationRule;
import io.conductor.kafkalinter.rules.clients.ProducerUsedAfterCloseRule;
import io.conductor.kafkalinter.rules.clients.ConsumerUsedAfterCloseRule;
import io.conductor.kafkalinter.rules.clients.ProducerRecordPartitionAndKeyRule;
import io.conductor.kafkalinter.rules.clients.StringSerializerNonStringRule;
import io.conductor.kafkalinter.rules.clients.ConsumerGroupIdRandomRule;
import io.conductor.kafkalinter.rules.clients.ProducerTransactionalIdRandomRule;
import io.conductor.kafkalinter.rules.clients.ProducerTxnIdWithoutIdempotenceRule;
import io.conductor.kafkalinter.rules.clients.StreamsApplicationIdRandomRule;
import io.conductor.kafkalinter.rules.config.ConfigKeyValueRule;
import io.conductor.kafkalinter.rules.config.MethodCallRule;
import io.conductor.kafkalinter.rules.config.PropertyFileRule;
import io.conductor.kafkalinter.rules.observability.JacksonDefaultTypingRule;
import io.conductor.kafkalinter.rules.observability.LambdaProducerCallbackEmptyRule;
import io.conductor.kafkalinter.rules.observability.SchemaRegistryUrlMissingRule;
import io.conductor.kafkalinter.rules.observability.DeserSrNoAuthCredentialsRule;
import io.conductor.kafkalinter.rules.observability.SrJsonValueTypeMissingRule;
import io.conductor.kafkalinter.rules.observability.SrProtobufValueTypeMissingRule;
import io.conductor.kafkalinter.rules.observability.SrLatestCacheTtlSecAbsentRule;
import io.conductor.kafkalinter.rules.observability.SrNormalizeSchemasAbsentRule;
import io.conductor.kafkalinter.rules.observability.SrUseLatestVersionMissingRule;
import io.conductor.kafkalinter.rules.quarkus.QkBlockingMissingOnIncomingRule;
import io.conductor.kafkalinter.rules.quarkus.QkDevservicesInProdRule;
import io.conductor.kafkalinter.rules.quarkus.SmallRyeChannelConfigRule;
import io.conductor.kafkalinter.rules.spring.SpringErrorHandlingDeserializerNoDelegatesRule;
import io.conductor.kafkalinter.rules.spring.SpringJsonDeserializerTrustedPackagesWildcardRule;
import io.conductor.kafkalinter.rules.spring.SpringRetryableTopicNoKafkaTemplateRule;
import io.conductor.kafkalinter.rules.streams.StreamsCleanupInProdRule;
import io.conductor.kafkalinter.rules.streams.StreamsCloseNoTimeoutRule;
import io.conductor.kafkalinter.rules.streams.StreamsCloseZeroDurationRule;
import io.conductor.kafkalinter.rules.streams.StreamsForeachPeekPrintsStdoutRule;
import io.conductor.kafkalinter.rules.streams.StreamsKStreamPrintRule;
import io.conductor.kafkalinter.rules.streams.StreamsNoGlobalStateRestoreListenerRule;
import io.conductor.kafkalinter.rules.streams.StreamsNoStateListenerRule;
import io.conductor.kafkalinter.rules.streams.StreamsNoShutdownHookRule;
import io.conductor.kafkalinter.rules.streams.StreamsNoUncaughtExceptionHandlerRule;
import io.conductor.kafkalinter.rules.streams.StreamsNotClosedRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesBootstrapServersAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesCommitIntervalMsAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesDefaultDeserializationExceptionHandlerAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesDefaultKeySerdeAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesDefaultProductionExceptionHandlerAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesDefaultValueSerdeAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesProcessingExceptionHandlerAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesClientRackAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesDefaultDslStoreAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesStatestoreCacheMaxBytesAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesNumStandbyReplicasAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesMaxTaskIdleMsAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesTaskTimeoutMsAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesDefaultTimestampExtractorAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesAcceptableRecoveryLagAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesProbingRebalanceIntervalMsAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesMaxWarmupReplicasAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesRackAwareAssignmentTagsAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesTopologyOptimizationAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesNumStreamThreadsAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesProcessingGuaranteeAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesReplicationFactorAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsPropertiesStateDirAbsentRule;
import io.conductor.kafkalinter.rules.streams.StreamsRemoveThreadNoTimeoutRule;
import io.conductor.kafkalinter.rules.streams.StreamsRemoveThreadZeroDurationRule;
import io.conductor.kafkalinter.rules.streams.StreamsStoreQueryParametersNoStaleStoresRule;
import io.conductor.kafkalinter.rules.security.CredAwsCredentialLiteralRule;
import io.conductor.kafkalinter.rules.security.SecurityProtocolPlaceholderRule;
import io.conductor.kafkalinter.rules.security.SecuritySaslMechanismPlainRule;
import io.conductor.kafkalinter.rules.security.SecuritySaslOauthbearerTokenEndpointHttpRule;
import io.conductor.kafkalinter.rules.security.SecuritySslKeystoreLocationTmpRule;
import io.conductor.kafkalinter.rules.security.SecuritySslKeystoreTypeJksRule;
import io.conductor.kafkalinter.rules.security.SecuritySslProtocolLegacyRule;
import io.conductor.kafkalinter.rules.security.SecuritySslTruststoreLocationTmpRule;
import io.conductor.kafkalinter.rules.spring.SpringListenerAsyncRule;
import io.conductor.kafkalinter.rules.spring.SpringListenerThreadSleepRule;
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
        addIfEnabled(rules, sev, RuleId.CLIENT_ID_MISSING, ClientIdMissingRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_RECORD_PARTITION_AND_KEY, ProducerRecordPartitionAndKeyRule::new);
        addIfEnabled(rules, sev, RuleId.COMMIT_ASYNC_NO_FINAL_SYNC, CommitAsyncNoFinalSyncRule::new);
        addIfEnabled(rules, sev, RuleId.POLL_IN_REBALANCE_CALLBACK, PollInRebalanceCallbackRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_COMMITASYNC_IN_REBALANCE, ConsumerCommitAsyncInRebalanceRule::new);
        addIfEnabled(rules, sev, RuleId.KAFKA_CLIENT_IN_STATIC_INITIALIZER, KafkaClientInStaticInitializerRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_SEEK_BEFORE_POLL, ConsumerSeekBeforePollRule::new);
        addIfEnabled(rules, sev, RuleId.PROPERTIES_MUTATED_AFTER_CTOR, PropertiesMutatedAfterCtorRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_USED_AFTER_CLOSE, ProducerUsedAfterCloseRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_USED_AFTER_CLOSE, ConsumerUsedAfterCloseRule::new);
        addIfEnabled(rules, sev, RuleId.ADMIN_USED_AFTER_CLOSE, AdminUsedAfterCloseRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_NOT_CLOSED, ProducerNotClosedRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_CLOSE_NO_TIMEOUT, ProducerCloseNoTimeoutRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_PER_RECORD_ALLOCATION, ProducerPerRecordAllocationRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_NOT_CLOSED, ConsumerNotClosedRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_CLOSE_NO_TIMEOUT, ConsumerCloseNoTimeoutRule::new);
        addIfEnabled(rules, sev, RuleId.ADMIN_NOT_CLOSED, AdminNotClosedRule::new);
        addIfEnabled(rules, sev, RuleId.ADMIN_CLOSE_NO_TIMEOUT, AdminCloseNoTimeoutRule::new);
        addIfEnabled(rules, sev, RuleId.ADMIN_NEW_TOPIC_REPLICATION_FACTOR_ONE, AdminNewTopicReplicationFactorOneRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_NOT_CLOSED, StreamsNotClosedRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_STORE_QUERY_PARAMETERS_NO_STALE_STORES,
                StreamsStoreQueryParametersNoStaleStoresRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_CLEANUP_IN_PROD, StreamsCleanupInProdRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_CLOSE_NO_TIMEOUT, StreamsCloseNoTimeoutRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_REMOVE_THREAD_NO_TIMEOUT,
                StreamsRemoveThreadNoTimeoutRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_KSTREAM_PRINT, StreamsKStreamPrintRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_FOREACH_PEEK_PRINTS_STDOUT,
                StreamsForeachPeekPrintsStdoutRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_NO_WAKEUP_SHUTDOWN, ConsumerNoWakeupShutdownRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_NOT_THREAD_SAFE, ConsumerNotThreadSafeRule::new);
        addIfEnabled(rules, sev, RuleId.STRING_SERIALIZER_NON_STRING, StringSerializerNonStringRule::new);
        addIfEnabled(rules, sev, RuleId.HEADERS_SENSITIVE_KEYS, HeadersSensitiveKeysRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_SEND_BLOCKING_GET, ProducerSendBlockingGetRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_SEND_NO_CALLBACK, ProducerSendNoCallbackRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_SEND_NULL_CALLBACK, ProducerSendNullCallbackRule::new);
        addIfEnabled(rules, sev, RuleId.LAMBDA_PRODUCER_CALLBACK_EMPTY,
                LambdaProducerCallbackEmptyRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_FLUSH_IN_LOOP, ProducerFlushInLoopRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_SUBSCRIBE_IN_LOOP, ConsumerSubscribeInLoopRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_POLL_INFINITE_DURATION, ConsumerPollInfiniteDurationRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_CLOSE_ZERO_DURATION, ProducerCloseZeroDurationRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_CLOSE_ZERO_DURATION, ConsumerCloseZeroDurationRule::new);
        addIfEnabled(rules, sev, RuleId.ADMIN_CLOSE_ZERO_DURATION, AdminCloseZeroDurationRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_CLOSE_ZERO_DURATION, StreamsCloseZeroDurationRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_REMOVE_THREAD_ZERO_DURATION,
                StreamsRemoveThreadZeroDurationRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_FLUSH_IN_CALLBACK, ProducerFlushInCallbackRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_CLOSE_IN_CALLBACK, ProducerCloseInCallbackRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_SEND_IN_CALLBACK, ProducerSendInCallbackRule::new);
        addIfEnabled(rules, sev, RuleId.PRODUCER_BEGIN_TRANSACTION_NO_ABORT, ProducerBeginTransactionNoAbortRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_AUTO_COMMIT_TRUE, ConsumerAutoCommitTrueRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_COMMIT_PER_RECORD, ConsumerCommitPerRecordRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_POLL_ZERO, ConsumerPollZeroRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_POLL_RESULT_IGNORED, ConsumerPollResultIgnoredRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_PAUSE_NO_RESUME, ConsumerPauseNoResumeRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_POLL_LONG_DEPRECATED, s -> new MethodCallRule(
                RuleId.CONSUMER_POLL_LONG_DEPRECATED, s, KafkaTypes.CONSUMER_OWNERS, Set.of("poll"),
                desc -> desc != null && desc.startsWith("(J)"),
                "Consumer.poll(long) is deprecated since Kafka 2.0 (KIP-266) — replaced by poll(Duration). The long variant blocks indefinitely waiting for an initial group-coordinator assignment regardless of the timeout argument; the Duration variant returns an empty record set when the duration elapses, making coordinator-unavailability visible to the caller. The deprecated method is slated for removal in Kafka 4.x."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_COMMITSYNC_NO_TIMEOUT, s -> new MethodCallRule(
                RuleId.CONSUMER_COMMITSYNC_NO_TIMEOUT, s, KafkaTypes.CONSUMER_OWNERS, Set.of("commitSync"),
                desc -> desc != null && (desc.equals("()V") || desc.equals("(Ljava/util/Map;)V")),
                "Consumer.commitSync() / commitSync(Map) (no Duration) blocks indefinitely on coordinator unavailability — equivalent to commitSync(Duration.ofMillis(Long.MAX_VALUE)). Use commitSync(Duration) / commitSync(Map, Duration) so coordinator outages surface as recoverable TimeoutException instead of silent stalls."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_END_OFFSETS_NO_TIMEOUT, s -> new MethodCallRule(
                RuleId.CONSUMER_END_OFFSETS_NO_TIMEOUT, s, KafkaTypes.CONSUMER_OWNERS, Set.of("endOffsets"),
                desc -> desc != null && desc.equals("(Ljava/util/Collection;)Ljava/util/Map;"),
                "Consumer.endOffsets(Collection) (no Duration) blocks for up to default.api.timeout.ms (60 s by default) on broker/leader unavailability. Lag-monitoring scripts and admin tooling that use this overload pin threads during the exact outages they exist to detect. Use endOffsets(Collection, Duration) so timeouts surface as recoverable TimeoutException."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_BEGINNING_OFFSETS_NO_TIMEOUT, s -> new MethodCallRule(
                RuleId.CONSUMER_BEGINNING_OFFSETS_NO_TIMEOUT, s, KafkaTypes.CONSUMER_OWNERS, Set.of("beginningOffsets"),
                desc -> desc != null && desc.equals("(Ljava/util/Collection;)Ljava/util/Map;"),
                "Consumer.beginningOffsets(Collection) (no Duration) blocks for up to default.api.timeout.ms (60 s by default) on broker/leader unavailability. Use beginningOffsets(Collection, Duration) so timeouts surface as recoverable TimeoutException."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_OFFSETS_FOR_TIMES_NO_TIMEOUT, s -> new MethodCallRule(
                RuleId.CONSUMER_OFFSETS_FOR_TIMES_NO_TIMEOUT, s, KafkaTypes.CONSUMER_OWNERS, Set.of("offsetsForTimes"),
                desc -> desc != null && desc.equals("(Ljava/util/Map;)Ljava/util/Map;"),
                "Consumer.offsetsForTimes(Map) (no Duration) blocks for up to default.api.timeout.ms (60 s by default) on broker/leader unavailability. Replay tooling that uses this overload appears 'stuck' during exactly the incidents that prompt replay. Use offsetsForTimes(Map, Duration)."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_POSITION_NO_TIMEOUT, s -> new MethodCallRule(
                RuleId.CONSUMER_POSITION_NO_TIMEOUT, s, KafkaTypes.CONSUMER_OWNERS, Set.of("position"),
                desc -> desc != null && desc.equals("(Lorg/apache/kafka/common/TopicPartition;)J"),
                "Consumer.position(TopicPartition) (no Duration) can block for up to default.api.timeout.ms on coordinator unavailability when the cached position is stale. Lag/health probes that use this overload hang for 60 s per partition during outages. Use position(TopicPartition, Duration)."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_PARTITIONS_FOR_NO_TIMEOUT, s -> new MethodCallRule(
                RuleId.CONSUMER_PARTITIONS_FOR_NO_TIMEOUT, s, KafkaTypes.CONSUMER_OWNERS, Set.of("partitionsFor"),
                desc -> desc != null && desc.equals("(Ljava/lang/String;)Ljava/util/List;"),
                "Consumer.partitionsFor(String) (no Duration) blocks for up to default.api.timeout.ms (60 s) on metadata unavailability. Startup-health checks that use this overload hang for a full minute on misconfigured bootstrap or missing topics. Use partitionsFor(String, Duration)."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_LIST_TOPICS_NO_TIMEOUT, s -> new MethodCallRule(
                RuleId.CONSUMER_LIST_TOPICS_NO_TIMEOUT, s, KafkaTypes.CONSUMER_OWNERS, Set.of("listTopics"),
                desc -> desc != null && desc.equals("()Ljava/util/Map;"),
                "Consumer.listTopics() (no Duration) blocks for up to default.api.timeout.ms (60 s) and returns the full-cluster metadata snapshot. Expensive even on a healthy cluster; thread-pinning hazard during outages. Use listTopics(Duration)."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_COMMITTED_NO_TIMEOUT, s -> new MethodCallRule(
                RuleId.CONSUMER_COMMITTED_NO_TIMEOUT, s, KafkaTypes.CONSUMER_OWNERS, Set.of("committed"),
                desc -> desc != null && desc.equals("(Ljava/util/Set;)Ljava/util/Map;"),
                "Consumer.committed(Set) (no Duration) blocks for up to default.api.timeout.ms (60 s) on group-coordinator outage. Lag monitors that use this overload pin threads during exactly the incidents that justify monitoring. Use committed(Set, Duration)."));

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
        addIfEnabled(rules, sev, RuleId.PRODUCER_DEPRECATED_PARTITIONER, s -> ConfigKeyValueRule.literalAny(
                RuleId.PRODUCER_DEPRECATED_PARTITIONER, s, KafkaTypes.PARTITIONER_CLASS_KEY,
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
        addIfEnabled(rules, sev, RuleId.PRODUCER_TRANSACTIONAL_ID_RANDOM, ProducerTransactionalIdRandomRule::new);
        addIfEnabled(rules, sev, RuleId.STREAMS_APPLICATION_ID_RANDOM, StreamsApplicationIdRandomRule::new);
        addIfEnabled(rules, sev, RuleId.CONSUMER_GROUP_ID_RANDOM, ConsumerGroupIdRandomRule::new);
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
        addIfEnabled(rules, sev, RuleId.STREAMS_REPLICATION_FACTOR_BROKER_DEFAULT, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_REPLICATION_FACTOR_BROKER_DEFAULT, s, KafkaTypes.STREAMS_REPLICATION_FACTOR_KEY, "-1",
                "Streams replication.factor=-1 — defers to broker default.replication.factor, which is 1 on every managed Kafka platform's default. Internal changelog/repartition topics silently end up at RF=1 on prod. Set an explicit positive value (3 is the canonical answer)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_WINDOWSTORE_CHANGELOG_ADDITIONAL_RETENTION_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_WINDOWSTORE_CHANGELOG_ADDITIONAL_RETENTION_MS_TOO_HIGH, s,
                KafkaTypes.STREAMS_WINDOWSTORE_CHANGELOG_ADDITIONAL_RETENTION_MS_KEY,
                v -> { if (v == null) return false; try { long n = Long.parseLong(v.trim()); return n > 604_800_000L; } catch (NumberFormatException e) { return false; } },
                "windowstore.changelog.additional.retention.ms={value} — above 7 days. This is the changelog-side safety buffer, not the active-store retention; pushing it past 7 days bloats internal-topic disk and multiplies restore time after rebalances. Default 24h (86400000) is right."));
        addIfEnabled(rules, sev, RuleId.STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_LOW, s,
                KafkaTypes.STREAMS_REPARTITION_PURGE_INTERVAL_MS_KEY,
                v -> { if (v == null) return false; try { long n = Long.parseLong(v.trim()); return n > 0 && n < 5_000L; } catch (NumberFormatException e) { return false; } },
                "repartition.purge.interval.ms={value} — below 5 s. DeleteRecords admin RPCs hammer the controller queue and the admin-client inflight slots, contending with create/delete topic and leader election. Default 30000 (30 s) is right; raise to ≥5000."));
        addIfEnabled(rules, sev, RuleId.STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_HIGH, s,
                KafkaTypes.STREAMS_REPARTITION_PURGE_INTERVAL_MS_KEY,
                v -> { if (v == null) return false; try { long n = Long.parseLong(v.trim()); return n > 300_000L; } catch (NumberFormatException e) { return false; } },
                "repartition.purge.interval.ms={value} — above 5 min. Repartition-topic records sit on broker disk for the full interval after they're consumed; on a high-throughput topology that's tens of GB of avoidable disk usage. Default 30000 (30 s) is right; keep below 300000."));
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
        addIfEnabled(rules, sev, RuleId.STREAMS_PROCESSING_GUARANTEE_AT_LEAST_ONCE_EXPLICIT, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_PROCESSING_GUARANTEE_AT_LEAST_ONCE_EXPLICIT, s, KafkaTypes.STREAMS_PROCESSING_GUARANTEE_KEY,
                "at_least_once",
                "processing.guarantee=at_least_once — explicit declaration of the default. On stateful topologies (joins, aggregations, windowed) this is almost always a regression: someone turned EOS off without leaving a paper trail. Set exactly_once_v2 explicitly or remove the override entirely."));
        addIfEnabled(rules, sev, RuleId.STREAMS_DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_LOG_AND_CONTINUE, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_LOG_AND_CONTINUE, s, KafkaTypes.STREAMS_DEFAULT_DESER_HANDLER_KEY,
                v -> v != null && v.contains("LogAndContinueExceptionHandler"),
                "default.deserialization.exception.handler={value} — LogAndContinueExceptionHandler silently skips records that fail to deserialize, advances the source-offset, and produces zero diagnostics beyond a WARN log. A schema-incompatible upstream rollout turns into data loss with no DLQ; postmortems take hours longer than they should. Use LogAndFailExceptionHandler (the default) and let k8s restart the pod on poisoned input."));
        addIfEnabled(rules, sev, RuleId.STREAMS_COMMIT_INTERVAL_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_COMMIT_INTERVAL_TOO_LOW, s, KafkaTypes.STREAMS_COMMIT_INTERVAL_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 100; },
                "commit.interval.ms={value} — well below the 100 ms documented floor. Broker write rate (offset commits + changelog flush) goes pathological."));
        addIfEnabled(rules, sev, RuleId.STREAMS_CACHE_DISABLED, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_CACHE_DISABLED, s, KafkaTypes.STREAMS_CACHE_MAX_BYTES_BUFFERING_KEY,
                "0"::equals,
                "cache.max.bytes.buffering=0 — every state-store update is forwarded; changelog write rate explodes."));
        addIfEnabled(rules, sev, RuleId.STREAMS_MATERIALIZED_WITH_LOGGING_DISABLED, s -> new MethodCallRule(
                RuleId.STREAMS_MATERIALIZED_WITH_LOGGING_DISABLED, s, Set.of(KafkaTypes.MATERIALIZED), Set.of("withLoggingDisabled"),
                "Materialized.withLoggingDisabled() — state-store changelog topic disabled. The store is no longer fault-tolerant: on task reassignment or pod restart it starts empty and downstream aggregates/joins silently return wrong answers."));
        addIfEnabled(rules, sev, RuleId.STREAMS_STOREBUILDER_WITH_LOGGING_DISABLED, s -> new MethodCallRule(
                RuleId.STREAMS_STOREBUILDER_WITH_LOGGING_DISABLED, s, Set.of(KafkaTypes.STORE_BUILDER), Set.of("withLoggingDisabled"),
                "StoreBuilder.withLoggingDisabled() — Processor-API state store has no changelog topic. Restoration after rebalance yields an empty store; Processor.process() then runs against missing state and corrupts downstream output."));
        addIfEnabled(rules, sev, RuleId.STREAMS_SET_UNCAUGHT_EXCEPTION_HANDLER_LEGACY_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_SET_UNCAUGHT_EXCEPTION_HANDLER_LEGACY_DEPRECATED, s,
                Set.of(KafkaTypes.KAFKA_STREAMS), Set.of("setUncaughtExceptionHandler"),
                desc -> desc != null && desc.equals("(Ljava/lang/Thread$UncaughtExceptionHandler;)V"),
                "KafkaStreams.setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler) is deprecated since 2.8 (KIP-671) — use the StreamsUncaughtExceptionHandler overload to return REPLACE_THREAD / SHUTDOWN_CLIENT / SHUTDOWN_APPLICATION instead of letting threads die silently."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_SUBSCRIBE_WITHOUT_REBALANCE_LISTENER, s -> new MethodCallRule(
                RuleId.CONSUMER_SUBSCRIBE_WITHOUT_REBALANCE_LISTENER, s, KafkaTypes.CONSUMER_OWNERS, Set.of("subscribe"),
                desc -> desc != null && (desc.equals("(Ljava/util/Collection;)V") || desc.equals("(Ljava/util/regex/Pattern;)V")),
                "Consumer.subscribe(Collection)/subscribe(Pattern) without a ConsumerRebalanceListener — the consumer cannot flush in-memory state, commit final offsets, or release per-partition resources before partition revoke. Pass a ConsumerRebalanceListener."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_ENFORCE_REBALANCE_NO_REASON, s -> new MethodCallRule(
                RuleId.CONSUMER_ENFORCE_REBALANCE_NO_REASON, s, KafkaTypes.CONSUMER_OWNERS, Set.of("enforceRebalance"),
                desc -> desc != null && !desc.contains("Ljava/lang/String;"),
                "Consumer.enforceRebalance() with no reason String — KIP-735 (Kafka 3.0+) added the enforceRebalance(String) overload so broker-side group-coordinator logs capture WHO triggered each manually-initiated rebalance. Without a reason, an SRE investigating 'why did this group rebalance N times in M minutes?' has no cross-actor attribution. Pass enforceRebalance(\"<actor> <event-context>\") — e.g. enforceRebalance(\"autoscaler scale-out svc-payments-prod 21→30\")."));
        addIfEnabled(rules, sev, RuleId.STREAMS_REPARTITION_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_REPARTITION_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("repartition"),
                desc -> desc != null && desc.equals("()Lorg/apache/kafka/streams/kstream/KStream;"),
                "KStream.repartition() with no Repartitioned argument — the auto-generated repartition topic name is derived from the topology graph index; any upstream edit renames it and downstream aggregations start from offset 0 of an empty topic. Use repartition(Repartitioned.as(\"name\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_GROUP_BY_NO_GROUPED, s -> new MethodCallRule(
                RuleId.STREAMS_GROUP_BY_NO_GROUPED, s, Set.of(KafkaTypes.KSTREAM), Set.of("groupBy"),
                desc -> desc != null && desc.equals("(Lorg/apache/kafka/streams/kstream/KeyValueMapper;)Lorg/apache/kafka/streams/kstream/KGroupedStream;"),
                "KStream.groupBy(KeyValueMapper) with no Grouped argument — the implicit repartition topic name is derived from the topology graph index; any upstream edit renames it and the aggregation restarts from zero. Use groupBy(mapper, Grouped.as(\"name\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_SUPPRESS_BUFFER_UNBOUNDED, s -> new MethodCallRule(
                RuleId.STREAMS_SUPPRESS_BUFFER_UNBOUNDED, s, Set.of(KafkaTypes.SUPPRESSED_BUFFER_CONFIG), Set.of("unbounded"),
                "Suppressed.BufferConfig.unbounded() — suppress() buffer grows until JVM heap exhaustion on a slow downstream commit. Use BufferConfig.maxBytes(n) or maxRecords(n) with shutDownWhenFull() so the bound is explicit."));
        addIfEnabled(rules, sev, RuleId.STREAMS_COUNT_NO_MATERIALIZED, s -> new MethodCallRule(
                RuleId.STREAMS_COUNT_NO_MATERIALIZED, s, KafkaTypes.GROUPED_KSTREAM_OWNERS, Set.of("count"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Materialized;"),
                "count() without a Materialized argument — the underlying state store and changelog topic are auto-named from the topology graph index, so any upstream edit renames the changelog and the count restarts from zero on the next deploy. Pass Materialized.as(\"name\")."));
        addIfEnabled(rules, sev, RuleId.STREAMS_AGGREGATE_NO_MATERIALIZED, s -> new MethodCallRule(
                RuleId.STREAMS_AGGREGATE_NO_MATERIALIZED, s, KafkaTypes.GROUPED_KSTREAM_OWNERS, Set.of("aggregate"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Materialized;"),
                "aggregate() without a Materialized argument — the underlying state store and changelog topic are auto-named from the topology graph index, so any upstream edit renames the changelog and the aggregation restarts from the initializer's seed on the next deploy. Pass Materialized.as(\"name\")."));
        addIfEnabled(rules, sev, RuleId.STREAMS_REDUCE_NO_MATERIALIZED, s -> new MethodCallRule(
                RuleId.STREAMS_REDUCE_NO_MATERIALIZED, s, KafkaTypes.GROUPED_KSTREAM_OWNERS, Set.of("reduce"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Materialized;"),
                "reduce() without a Materialized argument — the underlying state store and changelog topic are auto-named from the topology graph index, so any upstream edit renames the changelog and the reduction restarts from the first incoming record on the next deploy. Pass Materialized.as(\"name\")."));
        addIfEnabled(rules, sev, RuleId.STREAMS_STREAM_JOIN_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_STREAM_JOIN_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("join", "leftJoin", "outerJoin"),
                desc -> desc != null
                        && !desc.contains("Lorg/apache/kafka/streams/kstream/StreamJoined;")
                        && !desc.contains("Lorg/apache/kafka/streams/kstream/Joined;")
                        && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.join() / leftJoin() / outerJoin() without a naming argument (StreamJoined for KStream-KStream, Joined for KStream-KTable, Named for KStream-GlobalKTable) — auto-generated repartition topic and join state-store names are derived from the topology graph index, so any upstream edit renames them and the join produces nulls on the next deploy."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_COMMIT_ASYNC_NO_CALLBACK, s -> new MethodCallRule(
                RuleId.CONSUMER_COMMIT_ASYNC_NO_CALLBACK, s, KafkaTypes.CONSUMER_OWNERS, Set.of("commitAsync"),
                desc -> desc != null && desc.equals("()V"),
                "Consumer.commitAsync() with no callback — failed commits (rebalance, coordinator unreachable, network error) are silently swallowed and the application has no signal to detect or retry. Pass an OffsetCommitCallback."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_RECORD_NO_KEY, s -> new MethodCallRule(
                RuleId.PRODUCER_RECORD_NO_KEY, s, Set.of(KafkaTypes.PRODUCER_RECORD), Set.of("<init>"),
                desc -> desc != null && desc.equals("(Ljava/lang/String;Ljava/lang/Object;)V"),
                "new ProducerRecord<>(topic, value) — 2-arg constructor sets the key to null. Records have no per-key ordering, log-compacted topics cannot dedupe by key, and the default partitioner uses sticky-batching across partitions. Pass an explicit key as the second argument."));
        addIfEnabled(rules, sev, RuleId.STREAMS_KTABLE_GROUP_BY_NO_GROUPED, s -> new MethodCallRule(
                RuleId.STREAMS_KTABLE_GROUP_BY_NO_GROUPED, s, Set.of(KafkaTypes.KTABLE), Set.of("groupBy"),
                desc -> desc != null && desc.equals("(Lorg/apache/kafka/streams/kstream/KeyValueMapper;)Lorg/apache/kafka/streams/kstream/KGroupedTable;"),
                "KTable.groupBy(KeyValueMapper) with no Grouped argument — the implicit repartition topic name is derived from the topology graph index; any upstream edit renames it and the downstream aggregation restarts from offset 0 of an empty repartition. Use groupBy(mapper, Grouped.as(\"name\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TABLE_NO_MATERIALIZED, s -> new MethodCallRule(
                RuleId.STREAMS_TABLE_NO_MATERIALIZED, s, Set.of(KafkaTypes.STREAMS_BUILDER), Set.of("table"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Materialized;"),
                "StreamsBuilder.table(topic) / table(topic, Consumed) with no Materialized — the local KeyValueStore name and its changelog topic name are derived from the topology graph index. Any topology edit renames them; the new application restores from an empty changelog; every KTable lookup returns null. Use table(topic, Materialized.as(\"name\")) or the three-arg overload."));
        addIfEnabled(rules, sev, RuleId.STREAMS_GLOBAL_TABLE_NO_MATERIALIZED, s -> new MethodCallRule(
                RuleId.STREAMS_GLOBAL_TABLE_NO_MATERIALIZED, s, Set.of(KafkaTypes.STREAMS_BUILDER), Set.of("globalTable"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Materialized;"),
                "StreamsBuilder.globalTable(topic) / globalTable(topic, Consumed) with no Materialized — the global state store is auto-named from the topology graph index. After a topology edit, every Streams instance restores an empty new-named global store; stream-globalTable joins return null for every key during the (potentially multi-hour) restore. Use globalTable(topic, Materialized.as(\"name\"))."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED, s -> new MethodCallRule(
                RuleId.PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED, s, KafkaTypes.PRODUCER_OWNERS, Set.of("sendOffsetsToTransaction"),
                desc -> desc != null && desc.equals("(Ljava/util/Map;Ljava/lang/String;)V"),
                "Producer.sendOffsetsToTransaction(Map, String groupId) is deprecated since Kafka 3.0 (KIP-447) — the String-groupId form bypasses the broker's generation/member fencing and lets a zombie producer overwrite a rebalanced consumer's committed offsets. Use sendOffsetsToTransaction(Map, consumer.groupMetadata())."));
        addIfEnabled(rules, sev, RuleId.STREAMS_FOREIGN_KEY_JOIN_NO_MATERIALIZED, s -> new MethodCallRule(
                RuleId.STREAMS_FOREIGN_KEY_JOIN_NO_MATERIALIZED, s, Set.of(KafkaTypes.KTABLE), Set.of("join", "leftJoin"),
                desc -> desc != null
                        && desc.contains("Ljava/util/function/Function;")
                        && !desc.contains("Lorg/apache/kafka/streams/kstream/Materialized;"),
                "KTable.join(KTable, Function, ValueJoiner...) — foreign-key table join with no Materialized argument. The subscription store, response store, subscription topic, and response topic are all auto-named from the topology graph index; any topology edit renames every one. Use the overload with Materialized.as(\"name\")."));
        addIfEnabled(rules, sev, RuleId.STREAMS_SELECT_KEY_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_SELECT_KEY_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("selectKey"),
                desc -> desc != null
                        && desc.equals("(Lorg/apache/kafka/streams/kstream/KeyValueMapper;)Lorg/apache/kafka/streams/kstream/KStream;"),
                "KStream.selectKey(KeyValueMapper) with no Named — the SelectKey processor node name (and any downstream repartition topic name) is graph-index-derived. Any topology edit renames the downstream repartition topic; aggregations restart from offset 0. Use selectKey(mapper, Named.as(\"name\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_MERGE_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_MERGE_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("merge"),
                desc -> desc != null
                        && desc.equals("(Lorg/apache/kafka/streams/kstream/KStream;)Lorg/apache/kafka/streams/kstream/KStream;"),
                "KStream.merge(KStream) with no Named — merge processor node name is graph-index-derived. Per-node metrics tagged by node ID break dashboards on every topology edit. Use merge(other, Named.as(\"name\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TO_TABLE_NO_MATERIALIZED, s -> new MethodCallRule(
                RuleId.STREAMS_TO_TABLE_NO_MATERIALIZED, s, Set.of(KafkaTypes.KSTREAM), Set.of("toTable"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Materialized;"),
                "KStream.toTable() / toTable(Named) without Materialized — the resulting KTable's internal store and changelog topic are auto-named from the topology graph index. Any topology edit renames them; the new instance sees an empty store, the table is silently empty until upstream re-emits every key. Use toTable(Named.as(\"...\"), Materialized.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_PROCESS_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_PROCESS_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("process", "processValues"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.process(supplier, stateStores...) / processValues(supplier, stateStores...) with no Named — the processor node name is graph-index-derived, breaking per-node metric labels (process-rate, dropped-records-rate) on every topology edit. Use process(supplier, Named.as(\"...\"), stateStores...)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_GROUP_BY_KEY_NO_GROUPED, s -> new MethodCallRule(
                RuleId.STREAMS_GROUP_BY_KEY_NO_GROUPED, s, Set.of(KafkaTypes.KSTREAM), Set.of("groupByKey"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Grouped;"),
                "KStream.groupByKey() without Grouped — if the upstream stream is repartition-required (any prior selectKey/map/flatMap), the auto-named repartition topic is graph-index-derived. Topology edits rename it; downstream aggregations restart from offset 0. Use groupByKey(Grouped.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_BRANCHED_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_BRANCHED_NO_NAMED, s, Set.of(KafkaTypes.BRANCHED_KSTREAM), Set.of("branch"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Branched;"),
                "BranchedKStream.branch(Predicate) with no Branched — each branch's sub-graph is named from the topology graph index; the returned Map<String, KStream> uses those auto-names as keys, so branches.get(\"X-PREDICATE-N\") returns null after any topology edit. Use branch(predicate, Branched.as(\"branchName\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_STREAM_NO_CONSUMED, s -> new MethodCallRule(
                RuleId.STREAMS_STREAM_NO_CONSUMED, s, Set.of(KafkaTypes.STREAMS_BUILDER), Set.of("stream"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Consumed;"),
                "StreamsBuilder.stream(topic) with no Consumed — source node name is graph-index-derived (KSTREAM-SOURCE-<N>, breaks source-tagged metrics on topology edits) AND serdes default to global default.key.serde / default.value.serde, so config-level changes silently corrupt this source's deserialization. Use stream(topic, Consumed.with(keySerde, valueSerde).withName(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TO_NO_PRODUCED, s -> new MethodCallRule(
                RuleId.STREAMS_TO_NO_PRODUCED, s, Set.of(KafkaTypes.KSTREAM), Set.of("to"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Produced;"),
                "KStream.to(topic) with no Produced — sink node name is graph-index-derived (KSTREAM-SINK-<N>) AND serdes default to global default.key.serde / default.value.serde, so config-level changes silently corrupt this sink's serialization. Use to(topic, Produced.with(keySerde, valueSerde).withName(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_SPLIT_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_SPLIT_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("split"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.split() with no Named — the split parent node is graph-index-derived (KSTREAM-BRANCH-<N>) and prefixes every branch map key. Even branches passed Branched.as(\"x\") end up as keys \"<auto-name>-x\". Use split(Named.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.ADMIN_CREATE_TOPICS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_CREATE_TOPICS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("createTopics"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/CreateTopicsOptions;"),
                "Admin.createTopics(Collection<NewTopic>) with no CreateTopicsOptions — uses the default request.timeout.ms (~30s) with no caller-visible bound. Pass new CreateTopicsOptions().timeoutMs(60_000) so retry logic can distinguish 'in flight' from 'failed'."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DELETE_TOPICS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DELETE_TOPICS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("deleteTopics"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DeleteTopicsOptions;"),
                "Admin.deleteTopics() with no DeleteTopicsOptions — destructive operation using the default request.timeout.ms (~30s) with no caller-visible bound; TimeoutException does NOT mean the delete failed. Pass new DeleteTopicsOptions().timeoutMs(60_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_ALTER_CONFIGS_DEPRECATED, s -> new MethodCallRule(
                RuleId.ADMIN_ALTER_CONFIGS_DEPRECATED, s, KafkaTypes.ADMIN_OWNERS, Set.of("alterConfigs"),
                "Admin.alterConfigs(Map<ConfigResource, Config>) is deprecated since Kafka 2.3 (KIP-339) — it performs a FULL REPLACEMENT, so any key not present in the Config payload gets reset to broker default. Use incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>) which mutates only the keys you name."));
        addIfEnabled(rules, sev, RuleId.STREAMS_KTABLE_FILTER_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_KTABLE_FILTER_NO_NAMED, s, Set.of(KafkaTypes.KTABLE), Set.of("filter", "filterNot"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KTable.filter/filterNot with no Named — the filter node name is graph-index-derived (KTABLE-FILTER-<N>); editing the topology renumbers the index and silently rebrands every metric tag and (for materialized variants) the changelog/state-store name. Use filter(predicate, Named.as(\"...\")) or filter(predicate, Materialized.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_KTABLE_MAP_VALUES_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_KTABLE_MAP_VALUES_NO_NAMED, s, Set.of(KafkaTypes.KTABLE), Set.of("mapValues"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KTable.mapValues with no Named — the mapValues node name is graph-index-derived (KTABLE-MAPVALUES-<N>); editing the topology renumbers the index and silently rebrands every metric tag and (for materialized variants) the changelog/state-store name. Use mapValues(mapper, Named.as(\"...\")) or mapValues(mapper, Materialized.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_FOREACH_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_FOREACH_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("foreach"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.foreach() with no Named — TERMINAL sink, action runs synchronously on the Streams thread, so I/O inside the action caps topology throughput. Node name is graph-index-derived (KSTREAM-FOREACH-<N>); metric tags churn on topology edits. Use foreach(action, Named.as(\"...\")) — and audit whether terminal-side-effect operators belong here at all (consider an explicit sink topic + a separate downstream consumer)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_FLAT_MAP_VALUES_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_FLAT_MAP_VALUES_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("flatMapValues"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.flatMapValues() with no Named — fan-out (one input → N outputs) but NOT key-changing, so no auto-repartition. Pure observability hazard: node name is graph-index-derived (KSTREAM-FLATMAPVALUES-<N>); throughput dashboards rebrand on topology edits. Use flatMapValues(mapper, Named.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_KTABLE_TO_STREAM_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_KTABLE_TO_STREAM_NO_NAMED, s, Set.of(KafkaTypes.KTABLE), Set.of("toStream"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KTable.toStream() with no Named — transition node is graph-index-derived (KTABLE-TOSTREAM-<N>); per-node metric tags rebrand on topology edits. Use toStream(Named.as(\"...\")). For the KeyValueMapper overload, this is also key-changing — see STREAMS_MAP_NO_NAMED for the downstream auto-repartition hazard."));
        addIfEnabled(rules, sev, RuleId.STREAMS_MAP_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_MAP_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("map"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.map(KeyValueMapper) with no Named — map is KEY-CHANGING; any downstream stateful op silently inserts an auto-repartition whose topic name is graph-index-derived (KSTREAM-KEY-SELECT-<N>). Topology edits orphan the old repartition topic on broker disk and force a full replay on next deploy. Use map(mapper, Named.as(\"...\")) — or use mapValues if no key change is needed."));
        addIfEnabled(rules, sev, RuleId.STREAMS_FLAT_MAP_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_FLAT_MAP_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("flatMap"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.flatMap(KeyValueMapper) with no Named — flatMap is KEY-CHANGING AND fan-out; auto-repartition topic carries N× input throughput. Topology edits orphan N× the broker-disk volume vs map. Use flatMap(mapper, Named.as(\"...\")) — or use flatMapValues then selectKey to isolate fan-out from key-change."));
        addIfEnabled(rules, sev, RuleId.STREAMS_KSTREAM_FILTER_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_KSTREAM_FILTER_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("filter", "filterNot"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.filter/filterNot with no Named — KSTREAM-FILTER-<N> graph-index-derived; per-node dropped-records metric tags rebrand on every topology edit, silently breaking filter-drop-rate alerts. Use filter(predicate, Named.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_KSTREAM_MAP_VALUES_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_KSTREAM_MAP_VALUES_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("mapValues"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.mapValues with no Named — KSTREAM-MAPVALUES-<N> graph-index-derived; NOT key-changing so no auto-repartition (safe alternative to map() when only the value changes), but still rebrand metric tags on topology edits. Use mapValues(mapper, Named.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_IN_MEMORY_KV_STORE, s -> new MethodCallRule(
                RuleId.STREAMS_IN_MEMORY_KV_STORE, s, Set.of(KafkaTypes.STREAMS_STORES),
                Set.of("inMemoryKeyValueStore", "inMemoryWindowStore", "inMemorySessionStore"),
                "Stores.inMemory*Store() — state lives only in JVM heap; on restart/crash/rebalance the store is empty and must be fully restored from the changelog topic (minutes-to-hours for non-trivial state, blocking the partition's processing). Use Stores.persistentKeyValueStore / persistentWindowStore / persistentSessionStore for production."));
        addIfEnabled(rules, sev, RuleId.STREAMS_COUNT_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_COUNT_NO_NAMED, s, KafkaTypes.GROUPED_KSTREAM_OWNERS, Set.of("count"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "Aggregator.count() with no Named — KSTREAM-AGGREGATE-<N> processor node, state store, changelog topic, and (when key-changing upstream) repartition topic are ALL graph-index-derived. Any topology edit upstream shifts <N> and silently invalidates the count's persistent state. Use count(Named.as(\"...\")) AND count(Named.as(\"...\"), Materialized.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_REDUCE_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_REDUCE_NO_NAMED, s, KafkaTypes.GROUPED_KSTREAM_OWNERS, Set.of("reduce"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "Aggregator.reduce(Reducer) with no Named — KSTREAM-REDUCE-<N> processor, state store, changelog topic all graph-index-derived. Topology edits silently invalidate the reduce's persistent state (running sum/max/min/custom-combine restarts from empty). Use reduce(reducer, Named.as(\"...\")) AND reduce(reducer, Named.as(\"...\"), Materialized.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_AGGREGATE_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_AGGREGATE_NO_NAMED, s, KafkaTypes.GROUPED_KSTREAM_OWNERS, Set.of("aggregate"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "Aggregator.aggregate(Initializer, Aggregator) with no Named — KSTREAM-AGGREGATE-<N> processor, state store, changelog topic all graph-index-derived. Of count/reduce/aggregate, aggregate typically carries the LARGEST per-key state (custom VA type); losing it across a topology edit is the most expensive to rebuild. Use aggregate(initializer, aggregator, Named.as(\"...\"), Materialized.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_BUILDER_BUILD_NO_PROPERTIES, s -> new MethodCallRule(
                RuleId.STREAMS_BUILDER_BUILD_NO_PROPERTIES, s, Set.of(KafkaTypes.STREAMS_BUILDER), Set.of("build"),
                desc -> "()Lorg/apache/kafka/streams/Topology;".equals(desc),
                "StreamsBuilder.build() (no Properties argument) — topology.optimization is SILENTLY IGNORED, including REUSE_KTABLE_SOURCE_TOPICS and MERGE_REPARTITION_TOPICS. Production code that sets `topology.optimization=all` in props expects the rewrites; without props, the topology is emitted un-optimized and every internal topic is created with the un-optimized names. Use builder.build(streamsProperties)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_FOREIGN_KEY_JOIN_NO_TABLE_JOINED, s -> new MethodCallRule(
                RuleId.STREAMS_FOREIGN_KEY_JOIN_NO_TABLE_JOINED, s, Set.of(KafkaTypes.KTABLE), Set.of("join", "leftJoin"),
                desc -> desc != null
                        && desc.contains("Ljava/util/function/Function;")
                        && !desc.contains("Lorg/apache/kafka/streams/kstream/TableJoined;"),
                "KTable.join(KTable, Function, ValueJoiner...) foreign-key join with no TableJoined — the subscription registration topic AND the subscription response topic are both graph-index-derived (KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-<N>-topic and -RESPONSE-<N>-topic). Any topology edit renames both; the new deploy starts with empty subscription topics and the FK join produces NO output until every left-side key is re-emitted. Pass TableJoined.with(Named.as(\"my-fk-join\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_ALL_METADATA_FOR_STORE_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_ALL_METADATA_FOR_STORE_DEPRECATED, s, Set.of(KafkaTypes.KAFKA_STREAMS),
                Set.of("allMetadataForStore", "allMetadata"),
                "KafkaStreams.allMetadataForStore() / allMetadata() is deprecated since Kafka 3.0 (KIP-744) — returns the old org.apache.kafka.streams.state.StreamsMetadata, which silently elides standby-replica information. Interactive-query routers built on these methods cannot fall over to a standby host while the active is restoring; the IQ endpoint returns 503 for the full restore window. Use streamsMetadataForStore() / metadataForAllStreamsClients() which return the new org.apache.kafka.streams.StreamsMetadata with standbyStateStoreNames() populated."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_COMMITTED_SINGLE_PARTITION_DEPRECATED, s -> new MethodCallRule(
                RuleId.CONSUMER_COMMITTED_SINGLE_PARTITION_DEPRECATED, s, KafkaTypes.CONSUMER_OWNERS, Set.of("committed"),
                desc -> desc != null && desc.startsWith("(Lorg/apache/kafka/common/TopicPartition;"),
                "Consumer.committed(TopicPartition) / committed(TopicPartition, Duration) deprecated since Kafka 2.4 (KIP-520). Each call is one OFFSET_FETCH round trip for a single partition; a loop over an N-partition assignment becomes N × broker-RTT of sequential serial fetches. Use the batched overloads consumer.committed(Set.of(tp1, tp2, ...)) and consumer.committed(Set<TopicPartition>, Duration) which return a Map<TopicPartition, OffsetAndMetadata> in a single round trip."));
        addIfEnabled(rules, sev, RuleId.STREAMS_WINDOWS_GRACE_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_WINDOWS_GRACE_DEPRECATED, s,
                Set.of(KafkaTypes.TIME_WINDOWS, KafkaTypes.JOIN_WINDOWS, KafkaTypes.SESSION_WINDOWS),
                Set.of("grace"),
                "TimeWindows.grace() / JoinWindows.grace() / SessionWindows.grace() chained-instance methods deprecated since Kafka 3.0 (KIP-633). The legacy `Windows.of(size).grace(grace)` pattern hid a 24-hour default grace period when `.grace()` was omitted, silently buffering late events for a day. Migrate to the new static factories that make grace explicit at construction: TimeWindows.ofSizeAndGrace(size, grace) / TimeWindows.ofSizeWithNoGrace(size); JoinWindows.ofTimeDifferenceAndGrace(diff, grace) / ofTimeDifferenceWithNoGrace(diff); SessionWindows.ofInactivityGapAndGrace(gap, grace) / ofInactivityGapWithNoGrace(gap). Calling `.grace()` after a new factory throws IllegalStateException at runtime."));
        addIfEnabled(rules, sev, RuleId.STREAMS_LEGACY_PROCESSOR_API_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_LEGACY_PROCESSOR_API_DEPRECATED, s,
                Set.of(KafkaTypes.TOPOLOGY, KafkaTypes.STREAMS_BUILDER),
                Set.of("addProcessor", "addGlobalStore"),
                desc -> desc != null
                        && desc.contains("Lorg/apache/kafka/streams/processor/ProcessorSupplier;")
                        && !desc.contains("Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;"),
                "Topology.addProcessor / Topology.addGlobalStore / StreamsBuilder.addGlobalStore overloads taking the legacy org.apache.kafka.streams.processor.ProcessorSupplier are deprecated since Kafka 3.3 (KIP-820). The new org.apache.kafka.streams.processor.api.ProcessorSupplier returns Processor<KIn, VIn, KOut, VOut> with typed Record<KIn, VIn>, named-child fan-out via context.forward(record, childName), and a compile-time fixed-key variant (FixedKeyProcessor) that the legacy API lacks. Change the import from `org.apache.kafka.streams.processor.ProcessorSupplier` to `org.apache.kafka.streams.processor.api.ProcessorSupplier` and refactor the Processor's process(K, V) into process(Record<K, V> record)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_TOPICS_RESULT_LEGACY_DEPRECATED, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_TOPICS_RESULT_LEGACY_DEPRECATED, s,
                Set.of(KafkaTypes.ADMIN_DESCRIBE_TOPICS_RESULT),
                Set.of("values", "all"),
                "DescribeTopicsResult.values() / DescribeTopicsResult.all() deprecated since Kafka 3.1 (KIP-516) — both predate topic IDs and silently return an EMPTY map when the underlying describeTopics call was made with TopicCollection.ofTopicIds(...). Migrate to topicNameValues() / allTopicNames() (if you queried by topic NAME) or topicIdValues() / allTopicIds() (if you queried by topic ID). The new accessors are identical in shape — same Map type, same KafkaFuture wrapping — they just communicate which key-space the result lives in and refuse to silently return empty results from the wrong-keyed query."));
        addIfEnabled(rules, sev, RuleId.ADMIN_FEATURE_UPDATE_ALLOW_DOWNGRADE_DEPRECATED, s -> new MethodCallRule(
                RuleId.ADMIN_FEATURE_UPDATE_ALLOW_DOWNGRADE_DEPRECATED, s,
                Set.of(KafkaTypes.ADMIN_FEATURE_UPDATE),
                Set.of("<init>", "allowDowngrade"),
                desc -> desc != null && (desc.equals("(SZ)V") || desc.equals("()Z")),
                "FeatureUpdate(short, boolean) constructor and FeatureUpdate.allowDowngrade() getter deprecated since Kafka 3.3 (KIP-778). The boolean flag conflates SAFE_DOWNGRADE (data files still readable) with UNSAFE_DOWNGRADE (broker may refuse to start) — there is no boolean expression for the unsafe path, so old-API callers cannot force a data-format-breaking downgrade. Replace with new FeatureUpdate(version, FeatureUpdate.UpgradeType.UPGRADE | SAFE_DOWNGRADE | UNSAFE_DOWNGRADE) and update.upgradeType() != UpgradeType.UPGRADE for read-side checks."));
        addIfEnabled(rules, sev, RuleId.ADMIN_LIST_CONSUMER_GROUP_OFFSETS_TOPIC_PARTITIONS_DEPRECATED, s -> new MethodCallRule(
                RuleId.ADMIN_LIST_CONSUMER_GROUP_OFFSETS_TOPIC_PARTITIONS_DEPRECATED, s,
                Set.of(KafkaTypes.ADMIN_LIST_CONSUMER_GROUP_OFFSETS_OPTIONS),
                Set.of("topicPartitions"),
                "ListConsumerGroupOffsetsOptions.topicPartitions(List<TopicPartition>) / topicPartitions() deprecated since Kafka 3.3 (KIP-709) — the per-Options TP filter is silently IGNORED by the new batched Admin.listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>) overload, where each Spec carries its own per-group TP filter. Use new ListConsumerGroupOffsetsSpec().topicPartitions(tps) per-group inside the Map; the batched form fans out OFFSET_FETCH RPCs to all coordinators in parallel instead of N × broker-RTT sequential per-group queries."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_LOG_DIRS_RESULT_LEGACY_DEPRECATED, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_LOG_DIRS_RESULT_LEGACY_DEPRECATED, s,
                Set.of(KafkaTypes.ADMIN_DESCRIBE_LOG_DIRS_RESULT),
                Set.of("values", "all"),
                "DescribeLogDirsResult.values() / DescribeLogDirsResult.all() deprecated since Kafka 3.0 (KIP-743) — both return the INTERNAL `org.apache.kafka.common.requests.DescribeLogDirsResponse$LogDirInfo` type, leaking wire-protocol shape into AdminClient consumers and missing newer fields (totalBytes, usableBytes). Migrate to descriptions() / allDescriptions() returning the public `org.apache.kafka.clients.admin.LogDirDescription`. The internal LogDirInfo lives in the `common.requests` non-public package and its field layout changes between minor Kafka releases."));
        addIfEnabled(rules, sev, RuleId.ADMIN_UPDATE_FEATURES_OPTIONS_DRY_RUN_DEPRECATED, s -> new MethodCallRule(
                RuleId.ADMIN_UPDATE_FEATURES_OPTIONS_DRY_RUN_DEPRECATED, s,
                Set.of(KafkaTypes.ADMIN_UPDATE_FEATURES_OPTIONS),
                Set.of("dryRun"),
                "UpdateFeaturesOptions.dryRun() / dryRun(boolean) deprecated since Kafka 3.5 (KIP-919) — UpdateFeaturesOptions was the only AdminClient *Options class that called the validate-without-apply mode `dryRun` instead of the canonical `validateOnly`. Rename callsites to validateOnly() / validateOnly(boolean) — wire-equivalent (same internal field), purely a naming-consistency cleanup with the rest of the AdminClient Options surface."));
        addIfEnabled(rules, sev, RuleId.ADMIN_TOPIC_LISTING_NAME_INTERNAL_CTOR_DEPRECATED, s -> new MethodCallRule(
                RuleId.ADMIN_TOPIC_LISTING_NAME_INTERNAL_CTOR_DEPRECATED, s,
                Set.of(KafkaTypes.ADMIN_TOPIC_LISTING),
                Set.of("<init>"),
                desc -> desc != null && desc.equals("(Ljava/lang/String;Z)V"),
                "TopicListing(String name, boolean isInternal) constructor deprecated since Kafka 3.0 (KIP-516) — predates topic IDs and constructs a TopicListing whose topicId() returns Uuid.ZERO_UUID (the sentinel for pre-2.8 brokers). Replace with new TopicListing(name, topicId, isInternal). If you genuinely don't have a topic-ID, pass Uuid.ZERO_UUID explicitly to make the intent visible. Affects test/mock/scaffolding code that hand-rolls TopicListing instances — Admin.listTopics() callers are unaffected."));
        addIfEnabled(rules, sev, RuleId.STREAMS_DEFAULT_WINDOWED_KEY_SERDE_INNER_DEPRECATED, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_DEFAULT_WINDOWED_KEY_SERDE_INNER_DEPRECATED, s,
                KafkaTypes.STREAMS_DEFAULT_WINDOWED_KEY_SERDE_INNER_KEY,
                v -> v != null && !v.isEmpty(),
                "default.windowed.key.serde.inner Streams config key deprecated since Kafka 2.7 (KIP-684) — global implicit inner-serde for the default windowed key-serde. A topology with multiple windowed operators having different key-types cannot satisfy one global setting; misconfiguration surfaces only at runtime as ClassCastException deep inside the windowed processor. Replace with explicit per-operator Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(InnerKey.class), valueSerde) so the key-type is compile-time checked."));
        addIfEnabled(rules, sev, RuleId.STREAMS_DEFAULT_WINDOWED_VALUE_SERDE_INNER_DEPRECATED, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_DEFAULT_WINDOWED_VALUE_SERDE_INNER_DEPRECATED, s,
                KafkaTypes.STREAMS_DEFAULT_WINDOWED_VALUE_SERDE_INNER_KEY,
                v -> v != null && !v.isEmpty(),
                "default.windowed.value.serde.inner Streams config key deprecated since Kafka 2.7 (KIP-684) — global implicit inner-serde for the default windowed value-serde. A topology with multiple windowed aggregations producing different value-types cannot satisfy one global setting; wrong wiring writes corrupt bytes to the changelog topic that surface as deserialization errors on state-store recovery. Replace with explicit per-operator Materialized.with(keySerde, innerValueSerde) at every windowed-aggregation site."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_RECORD_LEGACY_CHECKSUM_CTOR_DEPRECATED, s -> new MethodCallRule(
                RuleId.CONSUMER_RECORD_LEGACY_CHECKSUM_CTOR_DEPRECATED, s,
                Set.of(KafkaTypes.CONSUMER_RECORD),
                Set.of("<init>"),
                desc -> desc != null && (desc.contains("Lorg/apache/kafka/common/record/TimestampType;J") || desc.contains("Lorg/apache/kafka/common/record/TimestampType;Ljava/lang/Long;")),
                "ConsumerRecord constructor with checksum parameter deprecated since Kafka 2.0 (KIP-101 / KIP-82) — the per-record CRC field carries no useful information after the v2 message format moved CRCs to the batch level (KIP-98 in 0.11). Replace with ConsumerRecord(topic, partition, offset, ts, TimestampType.CREATE_TIME, keySize, valueSize, key, value, new RecordHeaders(), Optional.empty()) or the 5-arg shortcut ConsumerRecord(topic, partition, offset, key, value). Affects test/mock scaffolding that hand-rolls ConsumerRecord instances."));
        addIfEnabled(rules, sev, RuleId.STREAMS_RETRIES_CONFIG_DEPRECATED, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_RETRIES_CONFIG_DEPRECATED, s,
                KafkaTypes.STREAMS_RETRIES_KEY,
                v -> v != null && !v.isEmpty(),
                "Streams-level `retries` config deprecated since Kafka 2.7 (KIP-572) — Streams ignores it at the framework level (uses task.timeout.ms instead) but forwards the value to the embedded producer, where it OVERRIDES the KIP-91 MAX_INT default. A `retries=3` on a Streams config silently caps the embedded producer at 3 retries; a single 30-second broker-roll then kills the task with TimeoutException. Remove the entry and use `StreamsConfig.TASK_TIMEOUT_MS_CONFIG` for task-level retry budget. This rule is Streams-specific — plain KafkaProducer.retries is a different legitimate config."));
        addIfEnabled(rules, sev, RuleId.AUTO_INCLUDE_JMX_REPORTER_KEY_DEPRECATED, s -> new ConfigKeyValueRule(
                RuleId.AUTO_INCLUDE_JMX_REPORTER_KEY_DEPRECATED, s,
                KafkaTypes.AUTO_INCLUDE_JMX_REPORTER_KEY,
                v -> v != null && !v.isEmpty(),
                "`auto.include.jmx.reporter` config key deprecated since Kafka 3.3 (KIP-830) — JMX reporter is now always installed by default across all kafka-clients (Producer/Consumer/AdminClient/Streams). The key is scheduled for removal in Kafka 4.0+. Setting `=true` is redundant; setting `=false` is an observability antipattern (hides metrics from Prometheus/Datadog/Cruise Control). Remove the line. For non-JMX-only deployments, set `metric.reporters=` (empty) explicitly — that's the post-3.3 supported mechanism."));
        addIfEnabled(rules, sev, RuleId.STREAMS_DEFAULT_DSL_STORE_KEY_DEPRECATED, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_DEFAULT_DSL_STORE_KEY_DEPRECATED, s,
                KafkaTypes.DEFAULT_DSL_STORE_KEY,
                v -> v != null && !v.isEmpty(),
                "`default.dsl.store` config key deprecated since Kafka 3.5 (KIP-954) — the string-valued key accepted only `rocksDB` / `in_memory` and locks users out of new built-in stores (KIP-986 versioned stores) and any third-party DslStoreSuppliers. Replace with `StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG` and pass `BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class.getName()` (default) or `InMemoryDslStoreSuppliers.class.getName()`. Remove entirely if you want the RocksDB default — that's the right move for almost all production apps."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DELETE_TOPICS_RESULT_VALUES_DEPRECATED, s -> new MethodCallRule(
                RuleId.ADMIN_DELETE_TOPICS_RESULT_VALUES_DEPRECATED, s,
                Set.of(KafkaTypes.ADMIN_DELETE_TOPICS_RESULT),
                Set.of("values"),
                "DeleteTopicsResult.values() deprecated since Kafka 3.0 (KIP-516) — returns Map<String, KafkaFuture<Void>> with no awareness of topic IDs. Throws UnsupportedOperationException at runtime if deleteTopics() was called with TopicCollection.ofTopicIds(...). Replace with topicNameValues() for delete-by-name or topicIdValues() (returns Map<Uuid, KafkaFuture<Void>>) for delete-by-id. The renamed accessors make the key-space explicit at the callsite."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_RECORD_METADATA_LEGACY_CHECKSUM_CTOR_DEPRECATED, s -> new MethodCallRule(
                RuleId.PRODUCER_RECORD_METADATA_LEGACY_CHECKSUM_CTOR_DEPRECATED, s,
                Set.of(KafkaTypes.RECORD_METADATA),
                Set.of("<init>"),
                desc -> desc != null && desc.contains("Ljava/lang/Long;II)V"),
                "RecordMetadata 7-arg constructor with `Long checksum` parameter deprecated since Kafka 2.0 (KIP-101 / KIP-82) — the v2 message format (KIP-98 in 0.11) moved CRCs to the batch level; per-record checksum carries no useful information. Replace with the 6-arg constructor `new RecordMetadata(tp, baseOffset, (int) batchIndex, timestamp, keySize, valueSize)`. Affects test/mock scaffolding and mock-producer libraries that hand-roll RecordMetadata."));
        addIfEnabled(rules, sev, RuleId.KAFKA_FUTURE_THENAPPLY_FUNCTION_DEPRECATED, s -> new MethodCallRule(
                RuleId.KAFKA_FUTURE_THENAPPLY_FUNCTION_DEPRECATED, s,
                Set.of(KafkaTypes.KAFKA_FUTURE),
                Set.of("thenApply"),
                desc -> desc != null && desc.contains("Lorg/apache/kafka/common/KafkaFuture$Function;"),
                "KafkaFuture.thenApply(KafkaFuture.Function) deprecated since Kafka 3.0 (KIP-707) — the legacy `Function` interface declared a checked-exception apply() that forces awkward try/catch wrapping at every callsite. Replace with `KafkaFuture.BaseFunction` (same `apply(T)` shape, no checked exception) or, for new code, switch to `kafkaFuture.toCompletionStage().thenApply(...)` with standard java.util.function.Function. The plugin distinguishes the deprecated overload from the modern thenApply(BaseFunction) via descriptor."));
        addIfEnabled(rules, sev, RuleId.KAFKA_FUTURE_GET_NO_TIMEOUT, s -> new MethodCallRule(
                RuleId.KAFKA_FUTURE_GET_NO_TIMEOUT, s,
                Set.of(KafkaTypes.KAFKA_FUTURE),
                Set.of("get"),
                desc -> "()Ljava/lang/Object;".equals(desc),
                "KafkaFuture.get() (no-argument, unbounded) — parks the calling thread until the kafka-clients machinery resolves the future, with no caller-side deadline. In AdminClient code paths (createTopics, deleteTopics, describeCluster, alterConfigs, listConsumerGroupOffsets, ...), a slow/unreachable controller can hang the caller indefinitely; the AdminClient's `default.api.timeout.ms` is the only escape and it is configurable to Long.MAX_VALUE. Replace with the bounded overload `.get(timeout, TimeUnit)` matched to the surrounding deadline (HTTP request budget, reconciliation interval, terminationGracePeriodSeconds minus a buffer). The plugin matches `INVOKEVIRTUAL`/`INVOKEINTERFACE org/apache/kafka/common/KafkaFuture.get()Ljava/lang/Object;` — the bounded `.get(long, TimeUnit)` overload has a different descriptor and is not flagged."));
        addIfEnabled(rules, sev, RuleId.STREAMS_KSTREAM_PROCESS_LEGACY_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_KSTREAM_PROCESS_LEGACY_DEPRECATED, s,
                Set.of(KafkaTypes.KSTREAM),
                Set.of("process"),
                desc -> desc != null
                        && desc.contains("Lorg/apache/kafka/streams/processor/ProcessorSupplier;")
                        && !desc.contains("Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;"),
                "KStream.process(legacy org.apache.kafka.streams.processor.ProcessorSupplier, String...) is deprecated since Kafka Streams 3.3 (KIP-820). The legacy Processor API uses untyped process(K, V) returning void with context.forward(K, V), preventing downstream DSL chaining. Switch the import to `org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn, VIn, KOut, VOut>` and refactor the Processor's process(K, V) to process(Record<KIn, VIn> record). The modern overload returns KStream<KOut, VOut> so subsequent .filter()/.map()/.to(...) DSL operators chain naturally."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TIME_WINDOWED_DESERIALIZER_NO_SIZE_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_TIME_WINDOWED_DESERIALIZER_NO_SIZE_DEPRECATED, s,
                Set.of(KafkaTypes.TIME_WINDOWED_DESERIALIZER),
                Set.of("<init>"),
                desc -> "(Lorg/apache/kafka/common/serialization/Deserializer;)V".equals(desc),
                "new TimeWindowedDeserializer(Deserializer) — the 1-arg ctor that omits windowSize — is deprecated since Kafka Streams 2.8 (KIP-659). It defaults windowSize to Long.MAX_VALUE, so every reconstructed Windowed<K>.window().end() is meaningless (start + Long.MAX_VALUE overflows to a wrap-around negative long). Pass an explicit windowSize matching the topology's TimeWindows: new TimeWindowedDeserializer<>(inner, Duration.ofMinutes(5).toMillis())."));
        addIfEnabled(rules, sev, RuleId.STREAMS_WINDOWED_SERDES_TIME_FROM_CLASS_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_WINDOWED_SERDES_TIME_FROM_CLASS_DEPRECATED, s,
                Set.of(KafkaTypes.WINDOWED_SERDES),
                Set.of("timeWindowedSerdeFrom"),
                desc -> "(Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;".equals(desc),
                "WindowedSerdes.timeWindowedSerdeFrom(Class) — the 1-arg static factory that omits windowSize — is deprecated since Kafka Streams 2.8 (KIP-659). Internally it wires a TimeWindowedDeserializer without windowSize, so the resulting Serde<Windowed<T>> produces Windowed<T> instances with bogus windowEnd. Use the 2-arg form: WindowedSerdes.timeWindowedSerdeFrom(InnerKey.class, Duration.ofMinutes(5).toMillis())."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_TRANSACTION_TIMEOUT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_TRANSACTION_TIMEOUT_MS_TOO_LOW, s, KafkaTypes.TRANSACTION_TIMEOUT_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 0 && n < 30_000L; },
                "transaction.timeout.ms={value} — below 30 s. The coordinator's per-transaction timer fires on routine flush() + commit-marker round-trips; in-flight transactions are aborted before commitTransaction() returns, throwing InvalidProducerEpochException. Default 60000; values below 30 s break EOS guarantees under any modest broker pressure or GC pause."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_DELIVERY_TIMEOUT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_DELIVERY_TIMEOUT_MS_TOO_LOW, s, KafkaTypes.DELIVERY_TIMEOUT_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 0 && n < 30_000L; },
                "delivery.timeout.ms={value} — below 30 s. With default request.timeout.ms=30000, the producer enforces delivery.timeout.ms >= linger.ms + request.timeout.ms at construction; values much below 30 s either fail at startup or, if request.timeout.ms is also low, surface false TimeoutException callbacks under broker hiccups and ship duplicate records on application retry."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_SESSION_TIMEOUT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_SESSION_TIMEOUT_MS_TOO_LOW, s, KafkaTypes.SESSION_TIMEOUT_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 0 && n < 10_000L; },
                "session.timeout.ms={value} — below 10 s. Broker enforces group.min.session.timeout.ms (default 6 s) as a hard floor; values above 6 s but below 10 s rebalance-storm under normal JVM GC pauses (2-9 s on G1/ZGC under K8s memory pressure). Default 45000 since KIP-389 (Kafka 2.5) for this reason."));
        addIfEnabled(rules, sev, RuleId.ADMIN_LIST_PARTITION_REASSIGNMENTS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_LIST_PARTITION_REASSIGNMENTS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("listPartitionReassignments"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/ListPartitionReassignmentsOptions;"),
                "Admin.listPartitionReassignments() / Admin.listPartitionReassignments(Set<TopicPartition>) with no ListPartitionReassignmentsOptions — KIP-455 progress-diagnostic polled by Cruise Control / KafkaRebalance to track multi-hour rebalances; inherits ~30 s default request.timeout.ms. On a contended controller (exactly the state during a rebalance), TimeoutException leaves the polling loop unable to distinguish 'still reassigning' from 'controller busy', and the 0-arg overload returns EVERY active reassignment cluster-wide inflating response size. Pass new ListPartitionReassignmentsOptions().timeoutMs(120_000) and prefer the (Set<TopicPartition>, Options) overload scoping the query to the caller's own partitions."));
        addIfEnabled(rules, sev, RuleId.ADMIN_ALTER_PARTITION_REASSIGNMENTS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_ALTER_PARTITION_REASSIGNMENTS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("alterPartitionReassignments"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/AlterPartitionReassignmentsOptions;"),
                "Admin.alterPartitionReassignments() with no AlterPartitionReassignmentsOptions — controller-driven inter-broker move; the AdminClient ack queues behind other admin writes on busy controllers. Default ~30 s fires before the controller acks; retry sees duplicate-rejection while the original is in flight. Pass new AlterPartitionReassignmentsOptions().timeoutMs(120_000) and poll listPartitionReassignments() for progress."));
        addIfEnabled(rules, sev, RuleId.ADMIN_CREATE_ACLS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_CREATE_ACLS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("createAcls"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/CreateAclsOptions;"),
                "Admin.createAcls() with no CreateAclsOptions — security mutation under default ~30 s timeout; mid-batch firing leaves some bindings persisted, others not. Pass new CreateAclsOptions().timeoutMs(120_000) AND inspect per-binding values() rather than relying on the masking all() future."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DELETE_ACLS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DELETE_ACLS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("deleteAcls"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DeleteAclsOptions;"),
                "Admin.deleteAcls() with no DeleteAclsOptions — IRREVERSIBLE security mutation with broad-filter semantics under default ~30 s timeout. A single AclBindingFilter.ANY call can wipe thousands of bindings; mid-batch timeout firing leaves partial deletion with no enumeration. Pass new DeleteAclsOptions().timeoutMs(120_000) AND narrow every filter to a specific principal/resource."));
        addIfEnabled(rules, sev, RuleId.ADMIN_INCREMENTAL_ALTER_CONFIGS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_INCREMENTAL_ALTER_CONFIGS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("incrementalAlterConfigs"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/AlterConfigsOptions;"),
                "Admin.incrementalAlterConfigs() with no AlterConfigsOptions — defaults validateOnly=false: the change is APPLIED to the broker/topic with no dry-run preview. Pass new AlterConfigsOptions().validateOnly(true) for a preview, then re-run with validateOnly(false) once the result is reviewed."));
        addIfEnabled(rules, sev, RuleId.ADMIN_ELECT_LEADERS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_ELECT_LEADERS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("electLeaders"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/ElectLeadersOptions;"),
                "Admin.electLeaders() with no ElectLeadersOptions — leader election is controller-driven and can take minutes for UNCLEAN or busy-cluster PREFERRED elections; default ~30 s timeout fires mid-election. Pass new ElectLeadersOptions().timeoutMs(300_000) for UNCLEAN, 120_000 for PREFERRED."));
        addIfEnabled(rules, sev, RuleId.ADMIN_ALTER_REPLICA_LOG_DIRS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_ALTER_REPLICA_LOG_DIRS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("alterReplicaLogDirs"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/AlterReplicaLogDirsOptions;"),
                "Admin.alterReplicaLogDirs() with no AlterReplicaLogDirsOptions — disk-to-disk replica migration is asynchronous on the broker but the AdminClient still waits for an initial ack; default ~30 s timeout fires on busy brokers, leaving the migration in-flight while the caller sees TimeoutException. Pass new AlterReplicaLogDirsOptions().timeoutMs(120_000) and poll describeLogDirs() for true completion."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DELETE_CONSUMER_GROUPS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DELETE_CONSUMER_GROUPS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("deleteConsumerGroups"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DeleteConsumerGroupsOptions;"),
                "Admin.deleteConsumerGroups() with no DeleteConsumerGroupsOptions — IRREVERSIBLE per-group deletion under default ~30 s timeout; timeout firing mid-batch leaves some groups deleted, others alive, with no atomic rollback. Pass new DeleteConsumerGroupsOptions().timeoutMs(120_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_LOG_DIRS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_LOG_DIRS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeLogDirs"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeLogDirsOptions;"),
                "Admin.describeLogDirs() with no DescribeLogDirsOptions — broker-side disk scan that traverses every log segment on every requested broker; on multi-TB brokers it routinely exceeds the default ~30 s timeout, breaking capacity-planning and rebalance-planning tooling. Pass new DescribeLogDirsOptions().timeoutMs(180_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_ACLS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_ACLS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeAcls"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeAclsOptions;"),
                "Admin.describeAcls() with no DescribeAclsOptions — full ACL-store scan on the controller; on multi-tenant clusters with tens of thousands of bindings it exceeds the default ~30 s timeout. Pass new DescribeAclsOptions().timeoutMs(120_000) and narrow the AclBindingFilter to the principal/resource you actually care about."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_CONSUMER_GROUPS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_CONSUMER_GROUPS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeConsumerGroups"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeConsumerGroupsOptions;"),
                "Admin.describeConsumerGroups() with no DescribeConsumerGroupsOptions — defaults includeAuthorizedOperations=false; the ConsumerGroupDescription's authorizedOperations() returns null, hiding group-level ACL state (READ, DELETE, DESCRIBE) from capability-check tooling. Pass new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true).timeoutMs(60_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_LIST_CONSUMER_GROUP_OFFSETS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_LIST_CONSUMER_GROUP_OFFSETS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("listConsumerGroupOffsets"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsOptions;"),
                "Admin.listConsumerGroupOffsets() with no ListConsumerGroupOffsetsOptions — returns committed offsets for EVERY partition the group has committed to within offsets.retention.minutes (default 7 days), including topics the group no longer subscribes to. For long-running groups this is hundreds of partitions. Pass new ListConsumerGroupOffsetsOptions().topicPartitions(currentPartitions).timeoutMs(60_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_ALTER_CONSUMER_GROUP_OFFSETS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_ALTER_CONSUMER_GROUP_OFFSETS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("alterConsumerGroupOffsets"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/AlterConsumerGroupOffsetsOptions;"),
                "Admin.alterConsumerGroupOffsets() with no AlterConsumerGroupOffsetsOptions — inherits default ~30s timeout on an IRREVERSIBLE per-partition offset rewrite. Timeout firing leaves the group in a half-reset state (some partitions advanced, others at original offset); restart of consumer will process partitions inconsistently. Pass new AlterConsumerGroupOffsetsOptions().timeoutMs(120_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DELETE_RECORDS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DELETE_RECORDS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("deleteRecords"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DeleteRecordsOptions;"),
                "Admin.deleteRecords() with no DeleteRecordsOptions — inherits default timeout (~30 s), often too short for multi-partition delete. deleteRecords is IRREVERSIBLE and per-partition: timeout firing leaves the topic in a half-truncated state. Pass new DeleteRecordsOptions().timeoutMs(120_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_LIST_CONSUMER_GROUPS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_LIST_CONSUMER_GROUPS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("listConsumerGroups"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/ListConsumerGroupsOptions;"),
                "Admin.listConsumerGroups() with no ListConsumerGroupsOptions — returns ALL groups (Stable, Empty, Dead, in-rebalance) regardless of state; on big clusters with accumulated Dead/Empty groups this is multi-MB payload and seconds-scale latency. Pass new ListConsumerGroupsOptions().inStates(Set.of(ConsumerGroupState.STABLE)).timeoutMs(60_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_LIST_TRANSACTIONS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_LIST_TRANSACTIONS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("listTransactions"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/ListTransactionsOptions;"),
                "Admin.listTransactions() with no ListTransactionsOptions — returns ALL transactional-IDs (Ongoing, CompleteCommit, CompleteAbort, Empty, Dead) regardless of state; on Streams/EOS-v2 clusters with accumulated transactional-IDs this is 10MB+ payload. Pass new ListTransactionsOptions().filterStates(Set.of(TransactionState.ONGOING)).timeoutMs(60_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_LIST_CLIENT_METRICS_RESOURCES_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_LIST_CLIENT_METRICS_RESOURCES_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("listClientMetricsResources"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/ListClientMetricsResourcesOptions;"),
                "Admin.listClientMetricsResources() with no ListClientMetricsResourcesOptions — KIP-714 (Kafka 3.7+) client-telemetry subscription-list diagnostic; inherits ~30 s default request.timeout.ms. On a broker mid-config-reload or under quota-event pressure (the state SREs typically check 'did my new client-metrics subscription stick?'), TimeoutException leaves the verify-step ambiguous — and rollback-on-failure logic then accidentally deletes the subscription that did get created. Pass new ListClientMetricsResourcesOptions().timeoutMs(120_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_CLUSTER_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_CLUSTER_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeCluster"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeClusterOptions;"),
                "Admin.describeCluster() with no DescribeClusterOptions — defaults includeAuthorizedOperations=false; the result's authorizedOperations() returns null, hiding cluster-level ACL state (CREATE, DELETE, ALTER, DESCRIBE, CLUSTER_ACTION, ALTER_CONFIGS, DESCRIBE_CONFIGS, IDEMPOTENT_WRITE) from ACL-audit / capability-check tooling. Pass new DescribeClusterOptions().timeoutMs(60_000).includeAuthorizedOperations(true)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_LIST_OFFSETS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_LIST_OFFSETS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("listOffsets"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/ListOffsetsOptions;"),
                "Admin.listOffsets() with no ListOffsetsOptions — defaults isolationLevel=READ_UNCOMMITTED. On topics with active transactional producers (including Streams apps with EOS-v2), end-offsets include records from in-flight (potentially-aborted) transactions; lag-monitoring tools report phantom lag. Pass new ListOffsetsOptions().isolationLevel(IsolationLevel.READ_COMMITTED).timeoutMs(60_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_CREATE_PARTITIONS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_CREATE_PARTITIONS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("createPartitions"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/CreatePartitionsOptions;"),
                "Admin.createPartitions() with no CreatePartitionsOptions — no validateOnly dry-run available (the change is applied immediately) and timeout inherits AdminClient default (~30 s). Partition-add is IRREVERSIBLE and breaks keyed-record ordering across the cutover. Pass new CreatePartitionsOptions().validateOnly(true) first to verify, then re-run with validateOnly(false).timeoutMs(120_000) to apply."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_TOPICS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_TOPICS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeTopics"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeTopicsOptions;"),
                "Admin.describeTopics() with no DescribeTopicsOptions — inherits default timeout AND defaults includeAuthorizedOperations=false; the TopicDescription's authorizedOperations() returns null, silently breaking ACL-audit / migration tooling. Pass new DescribeTopicsOptions().timeoutMs(60_000).includeAuthorizedOperations(true)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_ALTER_CLIENT_QUOTAS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_ALTER_CLIENT_QUOTAS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("alterClientQuotas"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/AlterClientQuotasOptions;"),
                "Admin.alterClientQuotas(Collection<ClientQuotaAlteration>) with no AlterClientQuotasOptions — defaults validateOnly=false (the destructive quota mutation EXECUTES with no preview) and inherits the default request.timeout.ms (~30 s). On partial failure mid-batch some alterations are persisted and others not, with no caller-visible record of which is which. Pass new AlterClientQuotasOptions().validateOnly(true) first to preview, then re-run with validateOnly(false).timeoutMs(60_000) to apply."));
        addIfEnabled(rules, sev, RuleId.ADMIN_ALTER_USER_SCRAM_CREDENTIALS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_ALTER_USER_SCRAM_CREDENTIALS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("alterUserScramCredentials"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/AlterUserScramCredentialsOptions;"),
                "Admin.alterUserScramCredentials(List<UserScramCredentialAlteration>) with no AlterUserScramCredentialsOptions — security-sensitive credential mutation that inherits the AdminClient default request.timeout.ms (~30 s); on partial-failure mid-batch some users have already had their SCRAM credentials rotated/deleted while others have not, with no built-in rollback and no validateOnly dry-run (the options class doesn't expose one). Pass new AlterUserScramCredentialsOptions().timeoutMs(120_000) and issue alterations one user at a time so each KafkaFuture's outcome can be inspected independently."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DELETE_CONSUMER_GROUP_OFFSETS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DELETE_CONSUMER_GROUP_OFFSETS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("deleteConsumerGroupOffsets"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DeleteConsumerGroupOffsetsOptions;"),
                "Admin.deleteConsumerGroupOffsets(groupId, Set<TopicPartition>) with no DeleteConsumerGroupOffsetsOptions — destructive SRE recovery operation that inherits the AdminClient default request.timeout.ms (~30 s); on partial failure mid-batch some partitions' offsets are deleted (those partitions will rewind to auto.offset.reset) and others are not, leaving the group in a half-reset state. Pass new DeleteConsumerGroupOffsetsOptions().timeoutMs(120_000) and inspect each per-partition KafkaFuture<Void> in the result map before declaring the reset complete."));
        addIfEnabled(rules, sev, RuleId.ADMIN_UNREGISTER_BROKER_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_UNREGISTER_BROKER_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("unregisterBroker"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/UnregisterBrokerOptions;"),
                "Admin.unregisterBroker(int brokerId) with no UnregisterBrokerOptions — KIP-500 / KRaft-only destructive cluster-metadata operation that inherits the AdminClient default request.timeout.ms (~30 s); the controller commits the unregister to the metadata log only after quorum replication, and on a busy or mid-election controller (the typical state when unregisterBroker is called during a scale-down) a TimeoutException leaves cluster state undefined — the unregister may or may not have committed. Pass new UnregisterBrokerOptions().timeoutMs(120_000) and verify via describeCluster() that the broker is actually gone before proceeding."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_METADATA_QUORUM_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_METADATA_QUORUM_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeMetadataQuorum"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeMetadataQuorumOptions;"),
                "Admin.describeMetadataQuorum() with no DescribeMetadataQuorumOptions — KIP-595/KIP-630 KRaft-only controller-quorum diagnostic that inherits the ~30 s default request.timeout.ms; on a controller mid-failover or mid-log-replay (THE state under which the diagnostic is typically called), a TimeoutException leaves the SRE unable to distinguish 'probe failed' from 'quorum degraded' — at exactly the moment they most need that signal. Pass new DescribeMetadataQuorumOptions().timeoutMs(120_000) so the deadline accommodates worst-case quorum-replication latency."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_FEATURES_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_FEATURES_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeFeatures"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeFeaturesOptions;"),
                "Admin.describeFeatures() with no DescribeFeaturesOptions — KIP-584 feature-flag diagnostic that is the precondition for updateFeatures. Inherits the ~30 s default request.timeout.ms; on Kafka 2.7-3.3 also defaults to sendRequestToController=false (broker may serve a stale-by-up-to-5-seconds cached snapshot), poisoning the downstream updateFeatures call with a wrong source version that gets rejected as FeatureUpdateFailedException. Pass new DescribeFeaturesOptions().timeoutMs(120_000) — and on Kafka 2.7-3.3 add .sendRequestToController(true) so the AdminClient reads the controller's authoritative state."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_REPLICA_LOG_DIRS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_REPLICA_LOG_DIRS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeReplicaLogDirs"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeReplicaLogDirsOptions;"),
                "Admin.describeReplicaLogDirs(Collection<TopicPartitionReplica>) with no DescribeReplicaLogDirsOptions — KIP-113 per-replica log-dir diagnostic that is the precondition for alterReplicaLogDirs. Inherits the ~30 s default request.timeout.ms; on a broker mid-disk-IO-saturation (THE state under which the rebalance is typically called), per-replica KafkaFutures time out independently and a caller using .all().get() loses sight of replicas' current log-dir, leading to a downstream alterReplicaLogDirs that's a no-op (current==target) or a wrong-source move. Pass new DescribeReplicaLogDirsOptions().timeoutMs(120_000) and inspect each per-replica future before composing the alter spec."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_CLIENT_QUOTAS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_CLIENT_QUOTAS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeClientQuotas"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeClientQuotasOptions;"),
                "Admin.describeClientQuotas(ClientQuotaFilter) with no DescribeClientQuotasOptions — KIP-546 client-quota diagnostic that is the precondition AND the verification step for alterClientQuotas. Inherits the ~30 s default request.timeout.ms; on a multi-tenant cluster the result-size grows with tenant count so a broad ClientQuotaFilter.all() can time out before the response is fully assembled. Pass new DescribeClientQuotasOptions().timeoutMs(120_000) and ALWAYS prefer a narrow filter scoped to the specific entity the caller is about to alter."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_USER_SCRAM_CREDENTIALS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_USER_SCRAM_CREDENTIALS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeUserScramCredentials"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeUserScramCredentialsOptions;"),
                "Admin.describeUserScramCredentials() / describeUserScramCredentials(List<String>) with no DescribeUserScramCredentialsOptions — KIP-554 credentials diagnostic and verification step for alterUserScramCredentials. Inherits the default ~30 s request.timeout.ms; per-user KafkaFutures time out independently on a controller mid-election (typical credential-rotation incident state), and a caller using .all().get() loses sight of which users had their credentials actually updated successfully. Pass new DescribeUserScramCredentialsOptions().timeoutMs(120_000) and inspect each per-user future before declaring the rotation verified — and ALWAYS prefer the 2-arg overload with an explicit users list over the 0-arg form that scans every user in the cluster."));
        addIfEnabled(rules, sev, RuleId.ADMIN_CREATE_DELEGATION_TOKEN_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_CREATE_DELEGATION_TOKEN_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("createDelegationToken"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/CreateDelegationTokenOptions;"),
                "Admin.createDelegationToken() with no CreateDelegationTokenOptions — KIP-48 security-sensitive token issuance that silently inherits cluster defaults for owner (=caller's SASL principal, NOT the intended consumer service), renewers (=empty list, blocking external renewer jobs), maxLifetimeMs (=cluster-default delegation.token.max.lifetime.ms, typically 7 days), and timeoutMs (~30 s). The OWNER default is the most dangerous: tokens issued by a privileged provisioning script silently grant the script's principal's full ACLs to every holder of the HMAC. Pass new CreateDelegationTokenOptions().owner(targetPrincipal).renewers(List.of(renewerBot)).maxLifetimeMs(jobDurationMs).timeoutMs(120_000)."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_DELEGATION_TOKEN_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_DELEGATION_TOKEN_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeDelegationToken"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeDelegationTokenOptions;"),
                "Admin.describeDelegationToken() with no DescribeDelegationTokenOptions — KIP-48 token-state diagnostic used to verify [[admin-expire-delegation-token-no-options]] and [[admin-renew-delegation-token-no-options]] actually committed. The no-options form returns EVERY token in the cluster (no owner filter) AND inherits the ~30 s default request.timeout.ms; on a multi-tenant cluster with hundreds of tokens the response is large enough to time out before the caller sees it, defeating the verification step. Pass new DescribeDelegationTokenOptions().owners(List.of(callerPrincipal)).timeoutMs(120_000) so the broker returns only the tokens the caller actually wants to verify."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_PRODUCERS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_PRODUCERS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeProducers"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeProducersOptions;"),
                "Admin.describeProducers(Collection<TopicPartition>) with no DescribeProducersOptions — KIP-664 per-partition producer-state diagnostic that is the precondition for fenceProducers (decide which transactional.id values to fence). Inherits the default ~30 s request.timeout.ms; on a partition leader mid-ISR-shrink (typical EOS-v2 incident state), per-TopicPartition KafkaFutures time out independently and a caller using .all().get() loses sight of which partitions still have open dead-producer state — the downstream fenceProducers misses those producers, leaving them able to re-emit records past their tx-id epoch. Pass new DescribeProducersOptions().timeoutMs(120_000) and inspect each per-partition future before composing the fence spec."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_TRANSACTIONS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_TRANSACTIONS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeTransactions"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeTransactionsOptions;"),
                "Admin.describeTransactions(Collection<String>) with no DescribeTransactionsOptions — KIP-664 transaction-state diagnostic that's the precondition for downstream destructive recovery (abortTransaction, fenceProducers); inherits the AdminClient default request.timeout.ms (~30 s). On a transaction coordinator mid-failover (typical EOS-v2 recovery state), per-transactionalId KafkaFuture<TransactionDescription> entries time out independently; a caller using .all().get() or silently skipping timeouts then builds an AbortTransactionSpec missing those dead transactions, leaving their records blocking read_committed consumers indefinitely. Pass new DescribeTransactionsOptions().timeoutMs(120_000) and inspect each per-key future before composing the abort/fence spec."));
        addIfEnabled(rules, sev, RuleId.ADMIN_RENEW_DELEGATION_TOKEN_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_RENEW_DELEGATION_TOKEN_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("renewDelegationToken"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/RenewDelegationTokenOptions;"),
                "Admin.renewDelegationToken(byte[] hmac) with no RenewDelegationTokenOptions — KIP-48 security-sensitive operation that extends a SASL/SCRAM token's expiry, inheriting BOTH the broker-default renewal-period (typically 24 h, NOT 'as long as possible') AND the AdminClient default request.timeout.ms (~30 s); 'success-with-too-short-extension' is silent at call time and causes mid-batch auth failures hours later, and on TimeoutException the caller cannot tell whether the renewal committed. Pass new RenewDelegationTokenOptions().renewTimePeriodMs(targetMs).timeoutMs(120_000) with an explicit caller-chosen extension and verify the new expiryTimestamp via describeDelegationToken()."));
        addIfEnabled(rules, sev, RuleId.ADMIN_EXPIRE_DELEGATION_TOKEN_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_EXPIRE_DELEGATION_TOKEN_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("expireDelegationToken"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/ExpireDelegationTokenOptions;"),
                "Admin.expireDelegationToken(byte[] hmac) with no ExpireDelegationTokenOptions — KIP-48 security-sensitive destructive operation that early-expires a SASL/SCRAM delegation token, inheriting the AdminClient default request.timeout.ms (~30 s); on TimeoutException the caller cannot distinguish 'revocation didn't commit' from 'revocation committed but the retry hit a stale-cache broker returning DelegationTokenNotFoundException', leaving a compliance audit gap for the window between the first call and confirmation. Pass new ExpireDelegationTokenOptions().expiryTimePeriodMs(0).timeoutMs(120_000) and verify via describeDelegationToken() that the token's expiryTimestamp is now in the past before declaring revocation complete."));
        addIfEnabled(rules, sev, RuleId.ADMIN_ABORT_TRANSACTION_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_ABORT_TRANSACTION_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("abortTransaction"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/AbortTransactionOptions;"),
                "Admin.abortTransaction(AbortTransactionSpec) with no AbortTransactionOptions — KIP-664 destructive per-transaction recovery API that inherits the AdminClient default request.timeout.ms (~30 s); on partial failure mid-call, some partitions involved in the dead transaction are marked ABORT and others are not, leaving read_committed consumers with split visibility (half the partitions move past the dead transaction, half hang waiting for COMMIT/ABORT). Pass new AbortTransactionOptions().timeoutMs(120_000) so the recovery call has a realistic budget under the coordinator-failover conditions that typically trigger the call."));
        addIfEnabled(rules, sev, RuleId.ADMIN_FENCE_PRODUCERS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_FENCE_PRODUCERS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("fenceProducers"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/FenceProducersOptions;"),
                "Admin.fenceProducers(Collection<String>) with no FenceProducersOptions — KIP-664 destructive operation that bumps the transactional.id epoch on the transaction coordinator, forcibly invalidating any live producer using that tx-id. The no-options form inherits the default request.timeout.ms (~30 s); on a busy transaction coordinator (typical during the EOS-v2 recovery scenarios where fenceProducers is actually called) a mid-batch TimeoutException leaves some tx-ids fenced and others NOT, producing a split-brain state. Pass new FenceProducersOptions().timeoutMs(120_000) and fence one transactional.id per call when possible."));
        addIfEnabled(rules, sev, RuleId.TOPOLOGY_ADD_PROCESSOR_LEGACY_SUPPLIER, s -> new MethodCallRule(
                RuleId.TOPOLOGY_ADD_PROCESSOR_LEGACY_SUPPLIER, s, Set.of(KafkaTypes.TOPOLOGY), Set.of("addProcessor"),
                desc -> desc != null && desc.contains("Lorg/apache/kafka/streams/processor/ProcessorSupplier;"),
                "Topology.addProcessor(name, ProcessorSupplier, ...) called with the LEGACY org.apache.kafka.streams.processor.ProcessorSupplier — KIP-820 (Kafka 3.0+) replaced it with the typed org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn, VIn, KOut, VOut>. The legacy interface is on a deprecation timer (removal scheduled for a future Kafka major) and uses untyped K/V Processor#process(K, V) instead of the new Record-based typed API. Migrate to api.ProcessorSupplier and Processor#process(Record)."));
        addIfEnabled(rules, sev, RuleId.TOPOLOGY_ADD_GLOBAL_STORE_LEGACY_SUPPLIER, s -> new MethodCallRule(
                RuleId.TOPOLOGY_ADD_GLOBAL_STORE_LEGACY_SUPPLIER, s, Set.of(KafkaTypes.TOPOLOGY), Set.of("addGlobalStore"),
                desc -> desc != null && desc.contains("Lorg/apache/kafka/streams/processor/ProcessorSupplier;"),
                "Topology.addGlobalStore(storeBuilder, ..., ProcessorSupplier) called with the LEGACY org.apache.kafka.streams.processor.ProcessorSupplier — KIP-820 (Kafka 3.0+) added new overloads taking the typed org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn, VIn, Void, Void>. Global stores are populated by replaying the source topic through this processor on every instance, so a legacy untyped Processor running here means every Streams app instance is using the deprecated API on the replay path. Migrate to api.ProcessorSupplier and Processor#process(Record)."));
        addIfEnabled(rules, sev, RuleId.STREAMSBUILDER_ADDGLOBALSTORE_LEGACY_SUPPLIER, s -> new MethodCallRule(
                RuleId.STREAMSBUILDER_ADDGLOBALSTORE_LEGACY_SUPPLIER, s, Set.of(KafkaTypes.STREAMS_BUILDER), Set.of("addGlobalStore"),
                desc -> desc != null && desc.contains("Lorg/apache/kafka/streams/processor/ProcessorSupplier;"),
                "StreamsBuilder.addGlobalStore(StoreBuilder, topic, Consumed, ProcessorSupplier) called with the LEGACY org.apache.kafka.streams.processor.ProcessorSupplier — KIP-820 (Kafka 3.0+) replaced it with the typed org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn, VIn, Void, Void>. The DSL-level entry point for global stores; same deprecation timer as the PAPI Topology.addGlobalStore form. Migrate to api.ProcessorSupplier and Processor#process(Record)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_TABLE_NO_CONSUMED, s -> new MethodCallRule(
                RuleId.STREAMS_TABLE_NO_CONSUMED, s, Set.of(KafkaTypes.STREAMS_BUILDER), Set.of("table"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Consumed;"),
                "StreamsBuilder.table(topic) with no Consumed — KTable key/value serdes silently default to default.key.serde / default.value.serde from Streams config; a change to those defaults rebinds every Consumed-less table at once, breaking decode on first poll. Use table(topic, Consumed.with(keySerde, valueSerde).withName(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.STREAMS_GLOBAL_TABLE_NO_CONSUMED, s -> new MethodCallRule(
                RuleId.STREAMS_GLOBAL_TABLE_NO_CONSUMED, s, Set.of(KafkaTypes.STREAMS_BUILDER), Set.of("globalTable"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Consumed;"),
                "StreamsBuilder.globalTable(topic) with no Consumed — GlobalKTable serdes silently default to default.key.serde / default.value.serde; bootstrap is eager and kills app startup on decode failure with no exception-handler escape. Use globalTable(topic, Consumed.with(keySerde, valueSerde).withName(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.ADMIN_DESCRIBE_CONFIGS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_DESCRIBE_CONFIGS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("describeConfigs"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/DescribeConfigsOptions;"),
                "Admin.describeConfigs() with no DescribeConfigsOptions — uses the default request.timeout.ms AND defaults includeSynonyms=false / includeDocumentation=false, hiding the override chain (broker-default vs topic-override vs static-broker) needed by every migration / audit tool. Pass new DescribeConfigsOptions().timeoutMs(60_000).includeSynonyms(true).includeDocumentation(true)."));
        addIfEnabled(rules, sev, RuleId.STREAMS_KTABLE_JOIN_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_KTABLE_JOIN_NO_NAMED, s, Set.of(KafkaTypes.KTABLE), Set.of("join", "leftJoin", "outerJoin"),
                desc -> desc != null
                        && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;")
                        && !desc.contains("Lorg/apache/kafka/streams/kstream/Materialized;"),
                "KTable.join/leftJoin/outerJoin with neither Named nor Materialized — both the processor node AND the backing state store + changelog topic are graph-index-derived (KTABLE-MERGE-STATE-STORE-<N>); topology edits orphan the old changelog topic on broker disk and force a full restore from upstream KTable sources (potentially gigabytes) on the next deploy. Use join(other, joiner, Named.as(\"...\"), Materialized.as(\"...\"))."));
        addIfEnabled(rules, sev, RuleId.ADMIN_LIST_TOPICS_NO_OPTIONS, s -> new MethodCallRule(
                RuleId.ADMIN_LIST_TOPICS_NO_OPTIONS, s, KafkaTypes.ADMIN_OWNERS, Set.of("listTopics"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/clients/admin/ListTopicsOptions;"),
                "Admin.listTopics() with no ListTopicsOptions — uses the default request.timeout.ms (~30s) with no caller-visible bound AND defaults listInternal=false, silently excluding __consumer_offsets, *-changelog, *-repartition. Backup/discovery scripts get a falsely-complete topic list. Pass new ListTopicsOptions().timeoutMs(60_000).listInternal(true) as appropriate."));
        addIfEnabled(rules, sev, RuleId.STREAMS_PEEK_NO_NAMED, s -> new MethodCallRule(
                RuleId.STREAMS_PEEK_NO_NAMED, s, Set.of(KafkaTypes.KSTREAM), Set.of("peek"),
                desc -> desc != null && !desc.contains("Lorg/apache/kafka/streams/kstream/Named;"),
                "KStream.peek() with no Named — the peek processor node name is graph-index-derived (KSTREAM-PEEK-<N>); per-node metric tags rebrand on every topology edit. Also: audit whether the peek belongs in production at all (debug-style peeks ship every record through a synchronous callback). Use peek(action, Named.as(\"...\"))."));
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
        addIfEnabled(rules, sev, RuleId.STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_LOG_AND_SKIP, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_LOG_AND_SKIP, s, KafkaTypes.STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_KEY,
                v -> v != null && v.endsWith("LogAndSkipOnInvalidTimestamp"),
                "default.timestamp.extractor={value} — records with invalid timestamps are silently dropped (single WARN log line, no DLQ, no metric). Use FailOnInvalidTimestamp (default) and handle the error explicitly."));
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
        addIfEnabled(rules, sev, RuleId.STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_LOW, s, KafkaTypes.STREAMS_BUFFERED_RECORDS_PER_PARTITION_KEY,
                v -> {
                    if (v == null) return false;
                    try { long n = Long.parseLong(v.trim()); return n > 0 && n < 100L; }
                    catch (NumberFormatException e) { return false; }
                },
                "buffered.records.per.partition={value} — below 100. Task pauses partitions almost immediately; pause/resume churn dominates the processing loop. Default 1000 is right."));
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
        addIfEnabled(rules, sev, RuleId.PRODUCER_RECONNECT_BACKOFF_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_RECONNECT_BACKOFF_MS_TOO_HIGH, s, KafkaTypes.RECONNECT_BACKOFF_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 10_000L; },
                "reconnect.backoff.ms={value} — above 10 s. This is the FIRST reconnect delay before KIP-580 exponential backoff even kicks in; every transient broker bounce holds connections idle for at least this long before the first retry. Default 50 ms is right — initial reconnects should be cheap; raise reconnect.backoff.max.ms (the cap) if you need to be gentle, not the floor."));
        addIfEnabled(rules, sev, RuleId.KAFKA_RECONNECT_BACKOFF_MAX_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_RECONNECT_BACKOFF_MAX_MS_TOO_HIGH, s, KafkaTypes.RECONNECT_BACKOFF_MAX_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 60_000L; },
                "reconnect.backoff.max.ms={value} — above 60 s. KIP-580 caps exponential reconnect backoff at this value; after a few failed attempts every subsequent reconnect waits the full cap. A rolling broker restart that should drain in seconds stretches into minutes of dead connections, producer batches expire, consumers stall and rebalance, K8s liveness probes flap. Default 1000 ms is right."));
        addIfEnabled(rules, sev, RuleId.KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MS_TOO_HIGH, s, KafkaTypes.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 60_000L; },
                "socket.connection.setup.timeout.ms={value} — above 60 s. KIP-601 initial TCP setup timeout; with a value this high, a single broker DNS / network-partition failure pins a client thread waiting that long before failing over, request.timeout.ms fires first and the failure surfaces as request timeout (not connection timeout), masking the real cause. Default 10000 ms is right."));
        addIfEnabled(rules, sev, RuleId.STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_HIGH, s, KafkaTypes.STREAMS_PROBING_REBALANCE_INTERVAL_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 7_200_000L; },
                "probing.rebalance.interval.ms={value} — above 2 h. Streams won't ask 'are warm-up standbys caught up?' for that long; the standby capacity you provisioned via num.standby.replicas/max.warmup.replicas sits idle, scale-out is delayed by the full interval, and failover to standbys takes hours not minutes. Default 600000 (10 min) is the right answer."));
        addIfEnabled(rules, sev, RuleId.KAFKA_RETRY_BACKOFF_MAX_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_RETRY_BACKOFF_MAX_MS_TOO_HIGH, s, KafkaTypes.RETRY_BACKOFF_MAX_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 60_000L; },
                "retry.backoff.max.ms={value} — above 60 s. KIP-580 exponential backoff caps at this value, so after a few retries every subsequent retry waits the full cap — transient broker hiccups stretch into minutes of stalled requests. Default 1000 is right; over-raising harms the client without helping the broker."));
        addIfEnabled(rules, sev, RuleId.STREAMS_COMMIT_INTERVAL_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.STREAMS_COMMIT_INTERVAL_TOO_HIGH, s, KafkaTypes.STREAMS_COMMIT_INTERVAL_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 60_000L; },
                "commit.interval.ms={value} — above 60 s. Cache flushes, state-store commits, and offset commits are all deferred for the full interval; downstream sees data stale by up to {value} ms, and on crash recovery the changelog replay window is that long. The default (30 s at-least-once, 100 ms EOS) is the right trade-off; if you need lower write-amplification, raise statestore.cache.max.bytes instead."));
        addIfEnabled(rules, sev, RuleId.PRODUCER_SEND_BUFFER_BYTES_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.PRODUCER_SEND_BUFFER_BYTES_TOO_HIGH, s, KafkaTypes.SEND_BUFFER_BYTES_KEY,
                v -> { long n = parseLongOrZero(v); return n > 16L * 1024L * 1024L; },
                "send.buffer.bytes={value} — over 16 MiB. Pins SO_SNDBUF per broker connection in kernel memory (invisible to JVM heap profilers); on a wide cluster this silently chews hundreds of MiB. Throughput gain over the autotuned default is nil — set to -1 (default) and let Linux TCP autotuning size it."));
        addIfEnabled(rules, sev, RuleId.CONSUMER_RECEIVE_BUFFER_BYTES_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.CONSUMER_RECEIVE_BUFFER_BYTES_TOO_HIGH, s, KafkaTypes.RECEIVE_BUFFER_BYTES_KEY,
                v -> { long n = parseLongOrZero(v); return n > 16L * 1024L * 1024L; },
                "receive.buffer.bytes={value} — over 16 MiB. Pins SO_RCVBUF per broker connection in kernel memory; throughput ceiling is set by fetch.max.bytes / max.partition.fetch.bytes anyway, so the extra buffer is pure overhead. Set to -1 and let Linux autotune."));
        addIfEnabled(rules, sev, RuleId.SECURITY_SSL_KEYSTORE_LOCATION_TMP, s -> new ConfigKeyValueRule(
                RuleId.SECURITY_SSL_KEYSTORE_LOCATION_TMP, s, KafkaTypes.SSL_KEYSTORE_LOCATION_KEY,
                v -> v != null && (v.startsWith("/tmp") || v.startsWith("/var/tmp") || v.startsWith("/dev/shm")),
                "ssl.keystore.location={value} — keystore in /tmp / /var/tmp / /dev/shm. World-readable/writable scratch space; any other process can read the private key or replace the keystore with an attacker-controlled file. Move to /etc/kafka/ssl, /opt/app/secrets, or a Kubernetes Secret mount with chmod 600."));
        addIfEnabled(rules, sev, RuleId.SECURITY_SSL_TRUSTSTORE_LOCATION_TMP, s -> new ConfigKeyValueRule(
                RuleId.SECURITY_SSL_TRUSTSTORE_LOCATION_TMP, s, KafkaTypes.SSL_TRUSTSTORE_LOCATION_KEY,
                v -> v != null && (v.startsWith("/tmp") || v.startsWith("/var/tmp") || v.startsWith("/dev/shm")),
                "ssl.truststore.location={value} — truststore in /tmp / /var/tmp / /dev/shm. Any process that can write there replaces the CA bundle with an attacker-controlled CA and MITMs every subsequent broker handshake without triggering any TLS error. Move to /etc/kafka/ssl or a Kubernetes Secret mount."));
        addIfEnabled(rules, sev, RuleId.KAFKA_CLIENT_RACK_PLACEHOLDER, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_CLIENT_RACK_PLACEHOLDER, s, KafkaTypes.CLIENT_RACK_KEY,
                v -> looksLikeUnresolvedPlaceholder(v),
                "client.rack={value} — unresolved placeholder (${...}). The broker sees the literal text, fails the rack match, and silently falls back to leader-fetching across AZs. Resolve via @Value/System.getenv/ConfigProvider before constructing the client; verify with the preferred-read-replica consumer metric."));
        addIfEnabled(rules, sev, RuleId.STREAMS_GLOBAL_CONSUMER_AUTO_OFFSET_RESET_LATEST, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_GLOBAL_CONSUMER_AUTO_OFFSET_RESET_LATEST, s, KafkaTypes.STREAMS_GLOBAL_CONSUMER_AUTO_OFFSET_RESET_KEY, "latest",
                "global.consumer.auto.offset.reset=latest — GlobalKTable bootstrap consumer skips to tail instead of reading from beginning. The global store is permanently incomplete; every join against missing keys silently returns null. There is no production reason for this override — remove it."));
        addIfEnabled(rules, sev, RuleId.STREAMS_RESTORE_CONSUMER_AUTO_OFFSET_RESET_LATEST, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_RESTORE_CONSUMER_AUTO_OFFSET_RESET_LATEST, s, KafkaTypes.STREAMS_RESTORE_CONSUMER_AUTO_OFFSET_RESET_KEY, "latest",
                "restore.consumer.auto.offset.reset=latest — changelog-replay consumer skips to tail instead of replaying the full changelog. State stores start empty; aggregations, joins, and KTables silently produce wrong results after every rebalance. Remove this override."));
        addIfEnabled(rules, sev, RuleId.STREAMS_DEFAULT_DSL_STORE_INMEMORY, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_DEFAULT_DSL_STORE_INMEMORY, s, KafkaTypes.STREAMS_DEFAULT_DSL_STORE_KEY, "in_memory",
                "default.dsl.store=in_memory — every materialized DSL store lives entirely in heap. State growth bounded by -Xmx; restore times 5-10x slower than RocksDB. Almost always copy-pasted from a benchmark/test config — remove it and use the default (rocksDB)."));
        addIfEnabled(rules, sev, RuleId.KAFKA_SASL_LOGIN_CONNECT_TIMEOUT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SASL_LOGIN_CONNECT_TIMEOUT_MS_TOO_LOW, s, KafkaTypes.SASL_LOGIN_CONNECT_TIMEOUT_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 5000; },
                "sasl.login.connect.timeout.ms={value} — below 5000 ms. The TCP-connect timeout to the IdP (OAuth/OIDC token endpoint, Kerberos KDC) is tighter than realistic cold-start latency; every login attempt fails before the very first handshake completes. Raise to >=5000 (10000+ for cloud IdPs)."));
        addIfEnabled(rules, sev, RuleId.KAFKA_SASL_LOGIN_CONNECT_TIMEOUT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SASL_LOGIN_CONNECT_TIMEOUT_MS_TOO_HIGH, s, KafkaTypes.SASL_LOGIN_CONNECT_TIMEOUT_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 60_000L; },
                "sasl.login.connect.timeout.ms={value} — above 60 s. The TCP-connect timeout to the IdP (OAuth/OIDC token endpoint) is so loose that an unreachable IdP pins the login thread for the full duration before fast-failing. Outage detection latency multiplies, K8s probes flap, and token-refresh thunder during partial IdP degradation pins every client for the full timeout. Default 10 s is right."));
        addIfEnabled(rules, sev, RuleId.KAFKA_SASL_LOGIN_READ_TIMEOUT_MS_TOO_LOW, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SASL_LOGIN_READ_TIMEOUT_MS_TOO_LOW, s, KafkaTypes.SASL_LOGIN_READ_TIMEOUT_MS_KEY,
                v -> { int n = parseIntOrZero(v); return n > 0 && n < 5000; },
                "sasl.login.read.timeout.ms={value} — below 5000 ms. The socket-read timeout to the IdP is tighter than realistic JWT-issuance latency; client closes the socket mid-response and treats every cold-start as auth failure. Raise to >=5000 (10000+ for cloud IdPs)."));
        addIfEnabled(rules, sev, RuleId.KAFKA_SASL_LOGIN_READ_TIMEOUT_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SASL_LOGIN_READ_TIMEOUT_MS_TOO_HIGH, s, KafkaTypes.SASL_LOGIN_READ_TIMEOUT_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 60_000L; },
                "sasl.login.read.timeout.ms={value} — above 60 s. The socket-read timeout to the IdP is so loose that a hung IdP backend (TCP accepted but no response) pins the login thread for the full duration. Token-refresh thunder amplifies on partial IdP slowness. Default 10 s is right."));
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
        addIfEnabled(rules, sev, RuleId.KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_TOO_HIGH, s -> new ConfigKeyValueRule(
                RuleId.KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_TOO_HIGH, s, KafkaTypes.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_KEY,
                v -> { long n = parseLongOrZero(v); return n > 300_000L; },
                "socket.connection.setup.timeout.max.ms={value} — above 5 min. Exponential-backoff cap for connection setup retries; after a few failed attempts every attempt costs the full cap. Detecting a dead broker takes minutes per attempt; client walks through bootstrap.servers an order of magnitude slower. Default 30 s is right."));
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
        addIfEnabled(rules, sev, RuleId.STREAMS_SESSION_WINDOWS_WITH_DEPRECATED, s -> new MethodCallRule(
                RuleId.STREAMS_SESSION_WINDOWS_WITH_DEPRECATED, s, Set.of(KafkaTypes.SESSION_WINDOWS), Set.of("with"),
                "SessionWindows.with(Duration) is deprecated since Kafka Streams 2.7 — replaced by SessionWindows.ofInactivityGapWithNoGrace(Duration) or ofInactivityGapAndGrace(Duration, Duration). The legacy 24-hour default grace period silently inflated session-store size by ~1440×."));
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
        addIfEnabled(rules, sev, RuleId.STREAMS_ACCEPTABLE_RECOVERY_LAG_ZERO, s -> ConfigKeyValueRule.literal(
                RuleId.STREAMS_ACCEPTABLE_RECOVERY_LAG_ZERO, s, KafkaTypes.STREAMS_ACCEPTABLE_RECOVERY_LAG_KEY, "0",
                "acceptable.recovery.lag=0 — warm-standbys are never promoted to active because exact catch-up to the live tip is unreachable while the active task is still writing. Tasks pin to their current owner, probing rebalances fire every probing.rebalance.interval.ms forever, scale-up never completes. Default 10000 is correct; change probing.rebalance.interval.ms if you need faster promotion."));
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
        addIfEnabled(rules, sev, RuleId.SPRING_LISTENER_THREAD_SLEEP, SpringListenerThreadSleepRule::new);
        addIfEnabled(rules, sev, RuleId.SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES,
                SpringErrorHandlingDeserializerNoDelegatesRule::new);
        addIfEnabled(rules, sev, RuleId.SPRING_JSON_DESERIALIZER_TRUSTED_PACKAGES_WILDCARD,
                SpringJsonDeserializerTrustedPackagesWildcardRule::new);

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
        addIfEnabled(rules, sev, RuleId.SECURITY_SSL_ENABLED_PROTOCOLS_LEGACY, s -> new ConfigKeyValueRule(
                RuleId.SECURITY_SSL_ENABLED_PROTOCOLS_LEGACY, s, KafkaTypes.SSL_ENABLED_PROTOCOLS_KEY,
                KafkaLinterMojo::sslEnabledProtocolsContainsLegacy,
                "ssl.enabled.protocols={value} — legacy TLS version in the accept list. Pin to TLSv1.2,TLSv1.3 or leave unset; PCI-DSS / FedRAMP / FIPS forbid TLS < 1.2."));
        addIfEnabled(rules, sev, RuleId.SECURITY_SSL_CIPHER_SUITES_LEGACY, s -> new ConfigKeyValueRule(
                RuleId.SECURITY_SSL_CIPHER_SUITES_LEGACY, s, KafkaTypes.SSL_CIPHER_SUITES_KEY,
                KafkaLinterMojo::sslCipherSuitesContainsLegacy,
                "ssl.cipher.suites={value} — weak cipher in the list (RC4/MD5/DES/3DES/NULL/EXPORT/anon). Leave unset; the JDK picks modern AEAD suites."));
        addIfEnabled(rules, sev, RuleId.CRED_SR_BEARER_AUTH_TOKEN_LITERAL, s -> new ConfigKeyValueRule(
                RuleId.CRED_SR_BEARER_AUTH_TOKEN_LITERAL, s, KafkaTypes.SR_BEARER_AUTH_TOKEN_KEY,
                KafkaLinterMojo::isLiteralCredential,
                "bearer.auth.token={value} — Schema Registry bearer token in source/config. Inject via ${ENV_VAR} or switch to credentials.source=OAUTHBEARER."));
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
        addIfEnabled(rules, sev, RuleId.SR_LATEST_COMPATIBILITY_STRICT_FALSE, s -> ConfigKeyValueRule.literal(
                RuleId.SR_LATEST_COMPATIBILITY_STRICT_FALSE, s, KafkaTypes.SR_LATEST_COMPATIBILITY_STRICT_KEY, "false",
                "latest.compatibility.strict=false — combined with use.latest.version=true, the serializer pins records to the latest registered schema without verifying that the runtime record is compatible with it. Silent data loss: fields can be dropped or coerced with no error at serialization time."));
        addIfEnabled(rules, sev, RuleId.SR_VALUE_SUBJECT_NAME_STRATEGY_NON_DEFAULT, s -> new ConfigKeyValueRule(
                RuleId.SR_VALUE_SUBJECT_NAME_STRATEGY_NON_DEFAULT, s, KafkaTypes.SR_VALUE_SUBJECT_NAME_STRATEGY_KEY,
                v -> v != null && KafkaTypes.SR_NON_DEFAULT_SUBJECT_NAME_STRATEGY_FQCNS.stream().anyMatch(v.trim()::equals),
                "value.subject.name.strategy={value} — non-default strategy. Schema Registry compatibility checks set on <topic>-value no longer apply, and CI/governance tooling that indexes by topic-name silently misses breaking changes. Set to io.confluent.kafka.serializers.subject.TopicNameStrategy (the default)."));
        addIfEnabled(rules, sev, RuleId.SR_KEY_SUBJECT_NAME_STRATEGY_NON_DEFAULT, s -> new ConfigKeyValueRule(
                RuleId.SR_KEY_SUBJECT_NAME_STRATEGY_NON_DEFAULT, s, KafkaTypes.SR_KEY_SUBJECT_NAME_STRATEGY_KEY,
                v -> v != null && KafkaTypes.SR_NON_DEFAULT_SUBJECT_NAME_STRATEGY_FQCNS.stream().anyMatch(v.trim()::equals),
                "key.subject.name.strategy={value} — non-default strategy. Shared Avro key types across topics collide under one registry subject; the topic's compatibility level is bypassed. Set to io.confluent.kafka.serializers.subject.TopicNameStrategy (the default)."));
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
        if (sev.get(RuleId.SECURITY_PROTOCOL_PLAINTEXT_REMOTE) != Severity.OFF) {
            rules.add(new SecurityProtocolPlaintextRemoteRule(sev.get(RuleId.SECURITY_PROTOCOL_PLAINTEXT_REMOTE)));
        }
        if (sev.get(RuleId.CONSUMER_HEARTBEAT_SESSION_RATIO) != Severity.OFF) {
            rules.add(new ConsumerHeartbeatSessionRatioRule(sev.get(RuleId.CONSUMER_HEARTBEAT_SESSION_RATIO)));
        }
        if (sev.get(RuleId.AVRO_SPECIFIC_READER_MISSING) != Severity.OFF) {
            rules.add(new AvroSpecificReaderMissingRule(sev.get(RuleId.AVRO_SPECIFIC_READER_MISSING)));
        }
        if (sev.get(RuleId.CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN) != Severity.OFF) {
            rules.add(new ConsumerIsolationReadUncommittedWithTxnRule(sev.get(RuleId.CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN)));
        }
        if (sev.get(RuleId.CONNECT_ERRORS_TOLERANCE_ALL_NO_DLQ) != Severity.OFF) {
            rules.add(new ConnectErrorsToleranceAllNoDlqRule(sev.get(RuleId.CONNECT_ERRORS_TOLERANCE_ALL_NO_DLQ)));
        }
        if (sev.get(RuleId.CONNECT_ERRORS_TOLERANCE_ABSENT) != Severity.OFF) {
            rules.add(new ConnectErrorsToleranceAbsentRule(sev.get(RuleId.CONNECT_ERRORS_TOLERANCE_ABSENT)));
        }
        if (sev.get(RuleId.CONNECT_ERRORS_TOLERANCE_ALL_NO_LOG) != Severity.OFF) {
            rules.add(new ConnectErrorsToleranceAllNoLogRule(sev.get(RuleId.CONNECT_ERRORS_TOLERANCE_ALL_NO_LOG)));
        }
        if (sev.get(RuleId.CONNECT_ERRORS_RETRY_TIMEOUT_ABSENT) != Severity.OFF) {
            rules.add(new ConnectErrorsRetryTimeoutAbsentRule(sev.get(RuleId.CONNECT_ERRORS_RETRY_TIMEOUT_ABSENT)));
        }
        if (sev.get(RuleId.CONNECT_SINK_AUTO_COMMIT_TRUE) != Severity.OFF) {
            rules.add(new ConnectSinkAutoCommitTrueRule(sev.get(RuleId.CONNECT_SINK_AUTO_COMMIT_TRUE)));
        }
        if (sev.get(RuleId.CONNECT_SOURCE_PRODUCER_ACKS_NOT_ALL) != Severity.OFF) {
            rules.add(new ConnectSourceProducerAcksNotAllRule(sev.get(RuleId.CONNECT_SOURCE_PRODUCER_ACKS_NOT_ALL)));
        }
        if (sev.get(RuleId.CONNECT_SCHEMA_REGISTRY_CONVERTER_MISSING_URL) != Severity.OFF) {
            rules.add(new ConnectSchemaRegistryConverterMissingUrlRule(sev.get(RuleId.CONNECT_SCHEMA_REGISTRY_CONVERTER_MISSING_URL)));
        }
        if (sev.get(RuleId.CONNECT_DLQ_REPLICATION_FACTOR_LOW) != Severity.OFF) {
            rules.add(new ConnectDlqReplicationFactorLowRule(sev.get(RuleId.CONNECT_DLQ_REPLICATION_FACTOR_LOW)));
        }
        if (sev.get(RuleId.CONNECT_CONSUMER_OVERRIDE_GROUP_ID) != Severity.OFF) {
            rules.add(new ConnectConsumerOverrideGroupIdRule(sev.get(RuleId.CONNECT_CONSUMER_OVERRIDE_GROUP_ID)));
        }
        if (sev.get(RuleId.CONNECT_TRANSFORM_ALIAS_UNDEFINED) != Severity.OFF) {
            rules.add(new ConnectTransformAliasUndefinedRule(sev.get(RuleId.CONNECT_TRANSFORM_ALIAS_UNDEFINED)));
        }
        if (sev.get(RuleId.CONNECT_PREDICATE_REFERENCE_UNDEFINED) != Severity.OFF) {
            rules.add(new ConnectPredicateReferenceUndefinedRule(sev.get(RuleId.CONNECT_PREDICATE_REFERENCE_UNDEFINED)));
        }
        if (sev.get(RuleId.CONNECT_CONFIG_PROVIDER_REFERENCE_UNDEFINED) != Severity.OFF) {
            rules.add(new ConnectConfigProviderReferenceUndefinedRule(sev.get(RuleId.CONNECT_CONFIG_PROVIDER_REFERENCE_UNDEFINED)));
        }
        if (sev.get(RuleId.CONNECT_DLQ_TOPIC_EQUALS_INPUT_TOPIC) != Severity.OFF) {
            rules.add(new ConnectDlqTopicEqualsInputTopicRule(sev.get(RuleId.CONNECT_DLQ_TOPIC_EQUALS_INPUT_TOPIC)));
        }
        if (sev.get(RuleId.CONNECT_JSON_CONVERTER_SCHEMAS_ENABLE_UNSET) != Severity.OFF) {
            rules.add(new ConnectJsonConverterSchemasEnableUnsetRule(sev.get(RuleId.CONNECT_JSON_CONVERTER_SCHEMAS_ENABLE_UNSET)));
        }
        if (sev.get(RuleId.CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_BOTH_SET) != Severity.OFF) {
            rules.add(new ConnectSinkTopicsAndTopicsRegexBothSetRule(sev.get(RuleId.CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_BOTH_SET)));
        }
        if (sev.get(RuleId.CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_NEITHER_SET) != Severity.OFF) {
            rules.add(new ConnectSinkTopicsAndTopicsRegexNeitherSetRule(sev.get(RuleId.CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_NEITHER_SET)));
        }
        if (sev.get(RuleId.CONNECT_DLQ_CONTEXT_HEADERS_DISABLED) != Severity.OFF) {
            rules.add(new ConnectDlqContextHeadersDisabledRule(sev.get(RuleId.CONNECT_DLQ_CONTEXT_HEADERS_DISABLED)));
        }
        if (sev.get(RuleId.CONNECT_NAME_MISSING) != Severity.OFF) {
            rules.add(new ConnectNameMissingRule(sev.get(RuleId.CONNECT_NAME_MISSING)));
        }
        if (sev.get(RuleId.CONNECT_TRANSFORM_REGEXROUTER_MISSING_REGEX_OR_REPLACEMENT) != Severity.OFF) {
            rules.add(new ConnectTransformRegexRouterMissingRegexOrReplacementRule(sev.get(RuleId.CONNECT_TRANSFORM_REGEXROUTER_MISSING_REGEX_OR_REPLACEMENT)));
        }
        if (sev.get(RuleId.CONNECT_TASKS_MAX_LESS_THAN_ONE) != Severity.OFF) {
            rules.add(new ConnectTasksMaxLessThanOneRule(sev.get(RuleId.CONNECT_TASKS_MAX_LESS_THAN_ONE)));
        }
        if (sev.get(RuleId.CONNECT_TASKS_MAX_ABSENT) != Severity.OFF) {
            rules.add(new ConnectTasksMaxAbsentRule(sev.get(RuleId.CONNECT_TASKS_MAX_ABSENT)));
        }
        if (sev.get(RuleId.CONNECT_FILE_STREAM_DEMO_CONNECTOR) != Severity.OFF) {
            rules.add(new ConnectFileStreamDemoConnectorRule(sev.get(RuleId.CONNECT_FILE_STREAM_DEMO_CONNECTOR)));
        }
        if (sev.get(RuleId.CONNECT_AVRO_AUTO_REGISTER_SCHEMAS_TRUE) != Severity.OFF) {
            rules.add(new ConnectAvroAutoRegisterSchemasTrueRule(sev.get(RuleId.CONNECT_AVRO_AUTO_REGISTER_SCHEMAS_TRUE)));
        }
        if (sev.get(RuleId.CONNECT_S3_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE) != Severity.OFF) {
            rules.add(new ConnectS3SinkFlushSizeHugeWithoutTimeRotateRule(sev.get(RuleId.CONNECT_S3_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE)));
        }
        if (sev.get(RuleId.CONNECT_S3_SINK_FLUSH_SIZE_TOO_SMALL) != Severity.OFF) {
            rules.add(new ConnectS3SinkFlushSizeTooSmallRule(sev.get(RuleId.CONNECT_S3_SINK_FLUSH_SIZE_TOO_SMALL)));
        }
        if (sev.get(RuleId.CONNECT_S3_SINK_S3_PART_SIZE_TOO_SMALL) != Severity.OFF) {
            rules.add(new ConnectS3SinkS3PartSizeTooSmallRule(sev.get(RuleId.CONNECT_S3_SINK_S3_PART_SIZE_TOO_SMALL)));
        }
        if (sev.get(RuleId.CONNECT_HDFS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE) != Severity.OFF) {
            rules.add(new ConnectHdfsSinkFlushSizeHugeWithoutTimeRotateRule(sev.get(RuleId.CONNECT_HDFS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE)));
        }
        if (sev.get(RuleId.CONNECT_GCS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE) != Severity.OFF) {
            rules.add(new ConnectGcsSinkFlushSizeHugeWithoutTimeRotateRule(sev.get(RuleId.CONNECT_GCS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE)));
        }
        if (sev.get(RuleId.CONNECT_SINK_CONSUMER_AUTO_OFFSET_RESET_LATEST) != Severity.OFF) {
            rules.add(new ConnectSinkConsumerAutoOffsetResetLatestRule(sev.get(RuleId.CONNECT_SINK_CONSUMER_AUTO_OFFSET_RESET_LATEST)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_INCLUDE_AND_EXCLUDE_LIST_BOTH_SET) != Severity.OFF) {
            rules.add(new ConnectDebeziumIncludeAndExcludeListBothSetRule(sev.get(RuleId.CONNECT_DEBEZIUM_INCLUDE_AND_EXCLUDE_LIST_BOTH_SET)));
        }
        if (sev.get(RuleId.CONNECT_STORAGE_SINK_PATH_FORMAT_WITHOUT_TIME_PARTITIONER) != Severity.OFF) {
            rules.add(new ConnectStorageSinkPathFormatWithoutTimePartitionerRule(sev.get(RuleId.CONNECT_STORAGE_SINK_PATH_FORMAT_WITHOUT_TIME_PARTITIONER)));
        }
        if (sev.get(RuleId.CONNECT_STORAGE_SINK_TIMESTAMP_EXTRACTOR_WALLCLOCK) != Severity.OFF) {
            rules.add(new ConnectStorageSinkTimestampExtractorWallclockRule(sev.get(RuleId.CONNECT_STORAGE_SINK_TIMESTAMP_EXTRACTOR_WALLCLOCK)));
        }
        if (sev.get(RuleId.CONNECT_STORAGE_SINK_PARTITION_DURATION_MS_TOO_LOW) != Severity.OFF) {
            rules.add(new ConnectStorageSinkPartitionDurationMsTooLowRule(sev.get(RuleId.CONNECT_STORAGE_SINK_PARTITION_DURATION_MS_TOO_LOW)));
        }
        if (sev.get(RuleId.CONNECT_STORAGE_SINK_ROTATE_INTERVAL_MS_TOO_LOW) != Severity.OFF) {
            rules.add(new ConnectStorageSinkRotateIntervalMsTooLowRule(sev.get(RuleId.CONNECT_STORAGE_SINK_ROTATE_INTERVAL_MS_TOO_LOW)));
        }
        if (sev.get(RuleId.CRED_AWS_CREDENTIAL_LITERAL) != Severity.OFF) {
            rules.add(new CredAwsCredentialLiteralRule(sev.get(RuleId.CRED_AWS_CREDENTIAL_LITERAL)));
        }
        if (sev.get(RuleId.SECURITY_SASL_OAUTHBEARER_TOKEN_ENDPOINT_HTTP) != Severity.OFF) {
            rules.add(new SecuritySaslOauthbearerTokenEndpointHttpRule(sev.get(RuleId.SECURITY_SASL_OAUTHBEARER_TOKEN_ENDPOINT_HTTP)));
        }
        if (sev.get(RuleId.SECURITY_SSL_PROTOCOL_LEGACY) != Severity.OFF) {
            rules.add(new SecuritySslProtocolLegacyRule(sev.get(RuleId.SECURITY_SSL_PROTOCOL_LEGACY)));
        }
        if (sev.get(RuleId.SECURITY_SASL_MECHANISM_PLAIN) != Severity.OFF) {
            rules.add(new SecuritySaslMechanismPlainRule(sev.get(RuleId.SECURITY_SASL_MECHANISM_PLAIN)));
        }
        if (sev.get(RuleId.SECURITY_SSL_KEYSTORE_TYPE_JKS) != Severity.OFF) {
            rules.add(new SecuritySslKeystoreTypeJksRule(sev.get(RuleId.SECURITY_SSL_KEYSTORE_TYPE_JKS)));
        }
        if (sev.get(RuleId.SECURITY_SSL_KEYSTORE_LOCATION_TMP) != Severity.OFF) {
            rules.add(new SecuritySslKeystoreLocationTmpRule(sev.get(RuleId.SECURITY_SSL_KEYSTORE_LOCATION_TMP)));
        }
        if (sev.get(RuleId.SECURITY_SSL_TRUSTSTORE_LOCATION_TMP) != Severity.OFF) {
            rules.add(new SecuritySslTruststoreLocationTmpRule(sev.get(RuleId.SECURITY_SSL_TRUSTSTORE_LOCATION_TMP)));
        }
        if (sev.get(RuleId.SECURITY_PROTOCOL_PLACEHOLDER) != Severity.OFF) {
            rules.add(new SecurityProtocolPlaceholderRule(sev.get(RuleId.SECURITY_PROTOCOL_PLACEHOLDER)));
        }
        if (sev.get(RuleId.CONNECT_TRANSFORM_DEFINED_BUT_NOT_LISTED) != Severity.OFF) {
            rules.add(new ConnectTransformDefinedButNotListedRule(sev.get(RuleId.CONNECT_TRANSFORM_DEFINED_BUT_NOT_LISTED)));
        }
        if (sev.get(RuleId.CONNECT_PREDICATE_DEFINED_BUT_NOT_LISTED) != Severity.OFF) {
            rules.add(new ConnectPredicateDefinedButNotListedRule(sev.get(RuleId.CONNECT_PREDICATE_DEFINED_BUT_NOT_LISTED)));
        }
        if (sev.get(RuleId.CONNECT_JDBC_SINK_DELETE_ENABLED_TRUE_WITHOUT_PK_MODE_RECORD_KEY) != Severity.OFF) {
            rules.add(new ConnectJdbcSinkDeleteEnabledTrueWithoutPkModeRecordKeyRule(sev.get(RuleId.CONNECT_JDBC_SINK_DELETE_ENABLED_TRUE_WITHOUT_PK_MODE_RECORD_KEY)));
        }
        if (sev.get(RuleId.CONNECT_JDBC_SINK_UPSERT_OR_UPDATE_WITHOUT_PK) != Severity.OFF) {
            rules.add(new ConnectJdbcSinkUpsertOrUpdateWithoutPkRule(sev.get(RuleId.CONNECT_JDBC_SINK_UPSERT_OR_UPDATE_WITHOUT_PK)));
        }
        if (sev.get(RuleId.CONNECT_JDBC_SINK_AUTO_EVOLVE_TRUE_WITHOUT_AUTO_CREATE_TRUE) != Severity.OFF) {
            rules.add(new ConnectJdbcSinkAutoEvolveTrueWithoutAutoCreateTrueRule(sev.get(RuleId.CONNECT_JDBC_SINK_AUTO_EVOLVE_TRUE_WITHOUT_AUTO_CREATE_TRUE)));
        }
        if (sev.get(RuleId.CONNECT_JDBC_SOURCE_QUERY_AND_TABLE_BOTH_SET) != Severity.OFF) {
            rules.add(new ConnectJdbcSourceQueryAndTableBothSetRule(sev.get(RuleId.CONNECT_JDBC_SOURCE_QUERY_AND_TABLE_BOTH_SET)));
        }
        if (sev.get(RuleId.CONNECT_JDBC_SOURCE_VALIDATE_NON_NULL_FALSE) != Severity.OFF) {
            rules.add(new ConnectJdbcSourceValidateNonNullFalseRule(sev.get(RuleId.CONNECT_JDBC_SOURCE_VALIDATE_NON_NULL_FALSE)));
        }
        if (sev.get(RuleId.CONNECT_JDBC_SOURCE_MODE_COLUMN_MISSING) != Severity.OFF) {
            rules.add(new ConnectJdbcSourceModeColumnMissingRule(sev.get(RuleId.CONNECT_JDBC_SOURCE_MODE_COLUMN_MISSING)));
        }
        if (sev.get(RuleId.CONNECT_JDBC_SOURCE_POLL_INTERVAL_MS_TOO_LOW) != Severity.OFF) {
            rules.add(new ConnectJdbcSourcePollIntervalMsTooLowRule(sev.get(RuleId.CONNECT_JDBC_SOURCE_POLL_INTERVAL_MS_TOO_LOW)));
        }
        if (sev.get(RuleId.CONNECT_WORKER_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW) != Severity.OFF) {
            rules.add(new ConnectWorkerInternalTopicReplicationFactorLowRule(sev.get(RuleId.CONNECT_WORKER_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW)));
        }
        if (sev.get(RuleId.CONNECT_REST_ADVERTISED_HOST_NAME_LOCALHOST) != Severity.OFF) {
            rules.add(new ConnectRestAdvertisedHostNameLocalhostRule(sev.get(RuleId.CONNECT_REST_ADVERTISED_HOST_NAME_LOCALHOST)));
        }
        if (sev.get(RuleId.CONNECT_TRANSFORM_FILTER_WITHOUT_PREDICATE) != Severity.OFF) {
            rules.add(new ConnectTransformFilterWithoutPredicateRule(sev.get(RuleId.CONNECT_TRANSFORM_FILTER_WITHOUT_PREDICATE)));
        }
        if (sev.get(RuleId.CONNECT_STORAGE_SINK_FIELD_PARTITIONER_WITHOUT_PARTITION_FIELD_NAME) != Severity.OFF) {
            rules.add(new ConnectStorageSinkFieldPartitionerWithoutPartitionFieldNameRule(sev.get(RuleId.CONNECT_STORAGE_SINK_FIELD_PARTITIONER_WITHOUT_PARTITION_FIELD_NAME)));
        }
        if (sev.get(RuleId.CONNECT_JDBC_SINK_TABLE_NAME_FORMAT_WITHOUT_TOPIC_PLACEHOLDER) != Severity.OFF) {
            rules.add(new ConnectJdbcSinkTableNameFormatWithoutTopicPlaceholderRule(sev.get(RuleId.CONNECT_JDBC_SINK_TABLE_NAME_FORMAT_WITHOUT_TOPIC_PLACEHOLDER)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_INCREMENTAL_SNAPSHOT_WITHOUT_SIGNAL_CHANNEL) != Severity.OFF) {
            rules.add(new ConnectDebeziumIncrementalSnapshotWithoutSignalChannelRule(sev.get(RuleId.CONNECT_DEBEZIUM_INCREMENTAL_SNAPSHOT_WITHOUT_SIGNAL_CHANNEL)));
        }
        if (sev.get(RuleId.MM2_BLACKLIST_DEPRECATED) != Severity.OFF) {
            rules.add(new Mm2BlacklistDeprecatedRule(sev.get(RuleId.MM2_BLACKLIST_DEPRECATED)));
        }
        if (sev.get(RuleId.MM2_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW) != Severity.OFF) {
            rules.add(new Mm2InternalTopicReplicationFactorLowRule(sev.get(RuleId.MM2_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW)));
        }
        if (sev.get(RuleId.MM2_SYNC_TOPIC_CONFIGS_DISABLED) != Severity.OFF) {
            rules.add(new Mm2SyncTopicConfigsDisabledRule(sev.get(RuleId.MM2_SYNC_TOPIC_CONFIGS_DISABLED)));
        }
        if (sev.get(RuleId.MM2_SYNC_TOPIC_ACLS_DISABLED) != Severity.OFF) {
            rules.add(new Mm2SyncTopicAclsDisabledRule(sev.get(RuleId.MM2_SYNC_TOPIC_ACLS_DISABLED)));
        }
        if (sev.get(RuleId.MM2_EMIT_HEARTBEATS_DISABLED) != Severity.OFF) {
            rules.add(new Mm2EmitHeartbeatsDisabledRule(sev.get(RuleId.MM2_EMIT_HEARTBEATS_DISABLED)));
        }
        if (sev.get(RuleId.MM2_EMIT_CHECKPOINTS_DISABLED) != Severity.OFF) {
            rules.add(new Mm2EmitCheckpointsDisabledRule(sev.get(RuleId.MM2_EMIT_CHECKPOINTS_DISABLED)));
        }
        if (sev.get(RuleId.MM2_REFRESH_TOPICS_INTERVAL_TOO_LOW) != Severity.OFF) {
            rules.add(new Mm2RefreshTopicsIntervalTooLowRule(sev.get(RuleId.MM2_REFRESH_TOPICS_INTERVAL_TOO_LOW)));
        }
        if (sev.get(RuleId.MM2_SOURCE_CONNECTOR_TASKS_MAX_ONE) != Severity.OFF) {
            rules.add(new Mm2SourceConnectorTasksMaxOneRule(sev.get(RuleId.MM2_SOURCE_CONNECTOR_TASKS_MAX_ONE)));
        }
        if (sev.get(RuleId.MM2_CLUSTER_BOOTSTRAP_SERVERS_LOCALHOST) != Severity.OFF) {
            rules.add(new Mm2ClusterBootstrapServersLocalhostRule(sev.get(RuleId.MM2_CLUSTER_BOOTSTRAP_SERVERS_LOCALHOST)));
        }
        if (sev.get(RuleId.SR_USE_LATEST_VERSION_MISSING) != Severity.OFF) {
            rules.add(new SrUseLatestVersionMissingRule(sev.get(RuleId.SR_USE_LATEST_VERSION_MISSING)));
        }
        if (sev.get(RuleId.SR_NORMALIZE_SCHEMAS_ABSENT) != Severity.OFF) {
            rules.add(new SrNormalizeSchemasAbsentRule(sev.get(RuleId.SR_NORMALIZE_SCHEMAS_ABSENT)));
        }
        if (sev.get(RuleId.SR_LATEST_CACHE_TTL_SEC_ABSENT) != Severity.OFF) {
            rules.add(new SrLatestCacheTtlSecAbsentRule(sev.get(RuleId.SR_LATEST_CACHE_TTL_SEC_ABSENT)));
        }
        if (sev.get(RuleId.KAFKA_CLIENT_METADATA_RECOVERY_STRATEGY_ABSENT) != Severity.OFF) {
            rules.add(new KafkaClientMetadataRecoveryStrategyAbsentRule(sev.get(RuleId.KAFKA_CLIENT_METADATA_RECOVERY_STRATEGY_ABSENT)));
        }
        if (sev.get(RuleId.SR_JSON_VALUE_TYPE_MISSING) != Severity.OFF) {
            rules.add(new SrJsonValueTypeMissingRule(sev.get(RuleId.SR_JSON_VALUE_TYPE_MISSING)));
        }
        if (sev.get(RuleId.SR_PROTOBUF_VALUE_TYPE_MISSING) != Severity.OFF) {
            rules.add(new SrProtobufValueTypeMissingRule(sev.get(RuleId.SR_PROTOBUF_VALUE_TYPE_MISSING)));
        }
        if (sev.get(RuleId.DESER_SR_NO_AUTH_CREDENTIALS) != Severity.OFF) {
            rules.add(new DeserSrNoAuthCredentialsRule(sev.get(RuleId.DESER_SR_NO_AUTH_CREDENTIALS)));
        }
        if (sev.get(RuleId.CONNECT_TOPIC_CREATION_GROUP_DEFINED_BUT_NOT_LISTED) != Severity.OFF) {
            rules.add(new ConnectTopicCreationGroupDefinedButNotListedRule(sev.get(RuleId.CONNECT_TOPIC_CREATION_GROUP_DEFINED_BUT_NOT_LISTED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_SLOT_DROP_ON_STOP_TRUE) != Severity.OFF) {
            rules.add(new ConnectDebeziumPostgresSlotDropOnStopTrueRule(sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_SLOT_DROP_ON_STOP_TRUE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_PUBLICATION_AUTOCREATE_MODE_ALL_TABLES) != Severity.OFF) {
            rules.add(new ConnectDebeziumPostgresPublicationAutocreateModeAllTablesRule(sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_PUBLICATION_AUTOCREATE_MODE_ALL_TABLES)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_PUBLICATION_FILTERED_MISSING_FILTER_LIST) != Severity.OFF) {
            rules.add(new ConnectDebeziumPublicationFilteredMissingFilterListRule(sev.get(RuleId.CONNECT_DEBEZIUM_PUBLICATION_FILTERED_MISSING_FILTER_LIST)));
        }
        if (sev.get(RuleId.CONNECT_PRODUCER_ENABLE_IDEMPOTENCE_FALSE) != Severity.OFF) {
            rules.add(new ConnectProducerEnableIdempotenceFalseRule(sev.get(RuleId.CONNECT_PRODUCER_ENABLE_IDEMPOTENCE_FALSE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_KEY_CONVERTER_BYTE_ARRAY) != Severity.OFF) {
            rules.add(new ConnectDebeziumKeyConverterByteArrayRule(sev.get(RuleId.CONNECT_DEBEZIUM_KEY_CONVERTER_BYTE_ARRAY)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_VALUE_CONVERTER_BYTE_ARRAY) != Severity.OFF) {
            rules.add(new ConnectDebeziumValueConverterByteArrayRule(sev.get(RuleId.CONNECT_DEBEZIUM_VALUE_CONVERTER_BYTE_ARRAY)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_MAX_QUEUE_SIZE_LESS_THAN_MAX_BATCH_SIZE) != Severity.OFF) {
            rules.add(new ConnectDebeziumMaxQueueSizeLessThanMaxBatchSizeRule(sev.get(RuleId.CONNECT_DEBEZIUM_MAX_QUEUE_SIZE_LESS_THAN_MAX_BATCH_SIZE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_EVENT_PROCESSING_FAILURE_HANDLING_MODE_SKIP_OR_WARN) != Severity.OFF) {
            rules.add(new ConnectDebeziumEventProcessingFailureHandlingModeSkipOrWarnRule(sev.get(RuleId.CONNECT_DEBEZIUM_EVENT_PROCESSING_FAILURE_HANDLING_MODE_SKIP_OR_WARN)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_SKIPPED_OPERATIONS_DROPS_DATA) != Severity.OFF) {
            rules.add(new ConnectDebeziumSkippedOperationsDropsDataRule(sev.get(RuleId.CONNECT_DEBEZIUM_SKIPPED_OPERATIONS_DROPS_DATA)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_SCHEMA_HISTORY_INTERNAL_SKIP_UNPARSEABLE_DDL_TRUE) != Severity.OFF) {
            rules.add(new ConnectDebeziumSchemaHistoryInternalSkipUnparseableDdlTrueRule(sev.get(RuleId.CONNECT_DEBEZIUM_SCHEMA_HISTORY_INTERNAL_SKIP_UNPARSEABLE_DDL_TRUE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_DECIMAL_HANDLING_MODE_DOUBLE) != Severity.OFF) {
            rules.add(new ConnectDebeziumDecimalHandlingModeDoubleRule(sev.get(RuleId.CONNECT_DEBEZIUM_DECIMAL_HANDLING_MODE_DOUBLE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_TIME_PRECISION_MODE_CONNECT) != Severity.OFF) {
            rules.add(new ConnectDebeziumTimePrecisionModeConnectRule(sev.get(RuleId.CONNECT_DEBEZIUM_TIME_PRECISION_MODE_CONNECT)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_TOMBSTONES_ON_DELETE_DISABLED) != Severity.OFF) {
            rules.add(new ConnectDebeziumTombstonesOnDeleteDisabledRule(sev.get(RuleId.CONNECT_DEBEZIUM_TOMBSTONES_ON_DELETE_DISABLED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_SCHEMA_HISTORY_TOPIC_SHARED) != Severity.OFF) {
            rules.add(new ConnectDebeziumSchemaHistoryTopicSharedRule(sev.get(RuleId.CONNECT_DEBEZIUM_SCHEMA_HISTORY_TOPIC_SHARED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_SLOT_NAME_SHARED) != Severity.OFF) {
            rules.add(new ConnectDebeziumPostgresSlotNameSharedRule(sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_SLOT_NAME_SHARED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_PUBLICATION_NAME_SHARED) != Severity.OFF) {
            rules.add(new ConnectDebeziumPostgresPublicationNameSharedRule(sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_PUBLICATION_NAME_SHARED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_FILTERED_PUBLICATION_HEARTBEAT_DISABLED) != Severity.OFF) {
            rules.add(new ConnectDebeziumPostgresFilteredPublicationHeartbeatDisabledRule(sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_FILTERED_PUBLICATION_HEARTBEAT_DISABLED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_TASKS_MAX_GREATER_THAN_ONE) != Severity.OFF) {
            rules.add(new ConnectDebeziumPostgresTasksMaxGreaterThanOneRule(sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_TASKS_MAX_GREATER_THAN_ONE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_TASKS_MAX_GREATER_THAN_ONE) != Severity.OFF) {
            rules.add(new ConnectDebeziumMysqlTasksMaxGreaterThanOneRule(sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_TASKS_MAX_GREATER_THAN_ONE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_ORACLE_TASKS_MAX_GREATER_THAN_ONE) != Severity.OFF) {
            rules.add(new ConnectDebeziumOracleTasksMaxGreaterThanOneRule(sev.get(RuleId.CONNECT_DEBEZIUM_ORACLE_TASKS_MAX_GREATER_THAN_ONE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_DB2_TASKS_MAX_GREATER_THAN_ONE) != Severity.OFF) {
            rules.add(new ConnectDebeziumDb2TasksMaxGreaterThanOneRule(sev.get(RuleId.CONNECT_DEBEZIUM_DB2_TASKS_MAX_GREATER_THAN_ONE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_SNAPSHOT_MODE_NEVER) != Severity.OFF) {
            rules.add(new ConnectDebeziumSnapshotModeNeverRule(sev.get(RuleId.CONNECT_DEBEZIUM_SNAPSHOT_MODE_NEVER)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_SNAPSHOT_LOCKING_MODE_NONE) != Severity.OFF) {
            rules.add(new ConnectDebeziumMysqlSnapshotLockingModeNoneRule(sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_SNAPSHOT_LOCKING_MODE_NONE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_PLUGIN_NAME_DEPRECATED) != Severity.OFF) {
            rules.add(new ConnectDebeziumPostgresPluginNameDeprecatedRule(sev.get(RuleId.CONNECT_DEBEZIUM_POSTGRES_PLUGIN_NAME_DEPRECATED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_INCLUDE_QUERY_TRUE) != Severity.OFF) {
            rules.add(new ConnectDebeziumMysqlIncludeQueryTrueRule(sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_INCLUDE_QUERY_TRUE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_SQL_SERVER_DATABASE_DBNAME_DEPRECATED) != Severity.OFF) {
            rules.add(new ConnectDebeziumSqlServerDatabaseDbnameDeprecatedRule(sev.get(RuleId.CONNECT_DEBEZIUM_SQL_SERVER_DATABASE_DBNAME_DEPRECATED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_LEGACY_SCHEMA_HISTORY_KEYS) != Severity.OFF) {
            rules.add(new ConnectDebeziumLegacySchemaHistoryKeysRule(sev.get(RuleId.CONNECT_DEBEZIUM_LEGACY_SCHEMA_HISTORY_KEYS)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_DATABASE_SERVER_NAME_DEPRECATED) != Severity.OFF) {
            rules.add(new ConnectDebeziumDatabaseServerNameDeprecatedRule(sev.get(RuleId.CONNECT_DEBEZIUM_DATABASE_SERVER_NAME_DEPRECATED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_VALUE_CONVERTER_STRING) != Severity.OFF) {
            rules.add(new ConnectDebeziumValueConverterStringRule(sev.get(RuleId.CONNECT_DEBEZIUM_VALUE_CONVERTER_STRING)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_KEY_CONVERTER_STRING) != Severity.OFF) {
            rules.add(new ConnectDebeziumKeyConverterStringRule(sev.get(RuleId.CONNECT_DEBEZIUM_KEY_CONVERTER_STRING)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_SERVER_ID_SHARED) != Severity.OFF) {
            rules.add(new ConnectDebeziumMysqlServerIdSharedRule(sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_SERVER_ID_SHARED)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_SERVER_ID_RANDOM_RELIANCE) != Severity.OFF) {
            rules.add(new ConnectDebeziumMysqlServerIdRandomRelianceRule(sev.get(RuleId.CONNECT_DEBEZIUM_MYSQL_SERVER_ID_RANDOM_RELIANCE)));
        }
        if (sev.get(RuleId.CONNECT_DEBEZIUM_TOPIC_PREFIX_SHARED) != Severity.OFF) {
            rules.add(new ConnectDebeziumTopicPrefixSharedRule(sev.get(RuleId.CONNECT_DEBEZIUM_TOPIC_PREFIX_SHARED)));
        }
        if (sev.get(RuleId.PRODUCER_BUFFER_MEMORY_MISCONFIG) != Severity.OFF) {
            rules.add(new ProducerBufferMemoryMisconfigRule(sev.get(RuleId.PRODUCER_BUFFER_MEMORY_MISCONFIG)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_COMPRESSION_TYPE_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesCompressionTypeAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_COMPRESSION_TYPE_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_ACKS_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesAcksAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_ACKS_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesBootstrapServersAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_ENABLE_IDEMPOTENCE_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesEnableIdempotenceAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_ENABLE_IDEMPOTENCE_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_LINGER_MS_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesLingerMsAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_LINGER_MS_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_BATCH_SIZE_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesBatchSizeAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_BATCH_SIZE_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_BUFFER_MEMORY_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesBufferMemoryAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_BUFFER_MEMORY_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_DELIVERY_TIMEOUT_MS_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesDeliveryTimeoutMsAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_DELIVERY_TIMEOUT_MS_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_REQUEST_TIMEOUT_MS_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesRequestTimeoutMsAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_REQUEST_TIMEOUT_MS_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_MAX_BLOCK_MS_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesMaxBlockMsAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_MAX_BLOCK_MS_ABSENT)));
        }
        if (sev.get(RuleId.PRODUCER_PROPERTIES_CLIENT_ID_ABSENT) != Severity.OFF) {
            rules.add(new ProducerPropertiesClientIdAbsentRule(sev.get(RuleId.PRODUCER_PROPERTIES_CLIENT_ID_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_AUTO_OFFSET_RESET_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesAutoOffsetResetAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_AUTO_OFFSET_RESET_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesBootstrapServersAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_GROUP_ID_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesGroupIdAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_GROUP_ID_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_ENABLE_AUTO_COMMIT_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesEnableAutoCommitAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_ENABLE_AUTO_COMMIT_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_MAX_POLL_RECORDS_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesMaxPollRecordsAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_MAX_POLL_RECORDS_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_MAX_POLL_INTERVAL_MS_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesMaxPollIntervalMsAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_MAX_POLL_INTERVAL_MS_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_ISOLATION_LEVEL_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesIsolationLevelAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_ISOLATION_LEVEL_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_CLIENT_ID_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesClientIdAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_CLIENT_ID_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_PARTITION_ASSIGNMENT_STRATEGY_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesPartitionAssignmentStrategyAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_PARTITION_ASSIGNMENT_STRATEGY_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_HEARTBEAT_INTERVAL_MS_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesHeartbeatIntervalMsAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_HEARTBEAT_INTERVAL_MS_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_FETCH_MAX_WAIT_MS_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesFetchMaxWaitMsAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_FETCH_MAX_WAIT_MS_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_REQUEST_TIMEOUT_MS_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesRequestTimeoutMsAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_REQUEST_TIMEOUT_MS_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_CLIENT_RACK_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesClientRackAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_CLIENT_RACK_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_SESSION_TIMEOUT_MS_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesSessionTimeoutMsAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_SESSION_TIMEOUT_MS_ABSENT)));
        }
        if (sev.get(RuleId.CONSUMER_PROPERTIES_FETCH_MIN_BYTES_ABSENT) != Severity.OFF) {
            rules.add(new ConsumerPropertiesFetchMinBytesAbsentRule(sev.get(RuleId.CONSUMER_PROPERTIES_FETCH_MIN_BYTES_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_PROCESSING_GUARANTEE_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesProcessingGuaranteeAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_PROCESSING_GUARANTEE_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_NUM_STANDBY_REPLICAS_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesNumStandbyReplicasAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_NUM_STANDBY_REPLICAS_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_TOPOLOGY_OPTIMIZATION_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesTopologyOptimizationAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_TOPOLOGY_OPTIMIZATION_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_MAX_TASK_IDLE_MS_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesMaxTaskIdleMsAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_MAX_TASK_IDLE_MS_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_TASK_TIMEOUT_MS_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesTaskTimeoutMsAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_TASK_TIMEOUT_MS_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_TIMESTAMP_EXTRACTOR_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesDefaultTimestampExtractorAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_TIMESTAMP_EXTRACTOR_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_ACCEPTABLE_RECOVERY_LAG_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesAcceptableRecoveryLagAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_ACCEPTABLE_RECOVERY_LAG_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_PROBING_REBALANCE_INTERVAL_MS_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesProbingRebalanceIntervalMsAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_PROBING_REBALANCE_INTERVAL_MS_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_MAX_WARMUP_REPLICAS_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesMaxWarmupReplicasAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_MAX_WARMUP_REPLICAS_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_RACK_AWARE_ASSIGNMENT_TAGS_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesRackAwareAssignmentTagsAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_RACK_AWARE_ASSIGNMENT_TAGS_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_NUM_STREAM_THREADS_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesNumStreamThreadsAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_NUM_STREAM_THREADS_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_REPLICATION_FACTOR_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesReplicationFactorAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_REPLICATION_FACTOR_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_STATE_DIR_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesStateDirAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_STATE_DIR_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesDefaultDeserializationExceptionHandlerAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_PRODUCTION_EXCEPTION_HANDLER_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesDefaultProductionExceptionHandlerAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_PRODUCTION_EXCEPTION_HANDLER_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_PROCESSING_EXCEPTION_HANDLER_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesProcessingExceptionHandlerAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_PROCESSING_EXCEPTION_HANDLER_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_STATESTORE_CACHE_MAX_BYTES_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesStatestoreCacheMaxBytesAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_STATESTORE_CACHE_MAX_BYTES_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_DSL_STORE_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesDefaultDslStoreAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_DSL_STORE_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_CLIENT_RACK_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesClientRackAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_CLIENT_RACK_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesBootstrapServersAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_COMMIT_INTERVAL_MS_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesCommitIntervalMsAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_COMMIT_INTERVAL_MS_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_KEY_SERDE_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesDefaultKeySerdeAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_KEY_SERDE_ABSENT)));
        }
        if (sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_VALUE_SERDE_ABSENT) != Severity.OFF) {
            rules.add(new StreamsPropertiesDefaultValueSerdeAbsentRule(sev.get(RuleId.STREAMS_PROPERTIES_DEFAULT_VALUE_SERDE_ABSENT)));
        }
        if (sev.get(RuleId.QUARKUS_KAFKA_EXTENSION_RENAMED) != Severity.OFF) {
            rules.add(new QuarkusKafkaExtensionRenamedRule(sev.get(RuleId.QUARKUS_KAFKA_EXTENSION_RENAMED)));
        }
        if (sev.get(RuleId.SPRING_KAFKA_BOOT_MISMATCH) != Severity.OFF) {
            rules.add(new SpringKafkaBootMismatchRule(sev.get(RuleId.SPRING_KAFKA_BOOT_MISMATCH)));
        }
        if (sev.get(RuleId.SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE) != Severity.OFF) {
            rules.add(new SpringRetryableTopicNoKafkaTemplateRule(sev.get(RuleId.SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE)));
        }
        if (sev.get(RuleId.PRODUCER_INIT_TRANSACTIONS_NOT_CALLED) != Severity.OFF) {
            rules.add(new ProducerInitTransactionsNotCalledRule(sev.get(RuleId.PRODUCER_INIT_TRANSACTIONS_NOT_CALLED)));
        }
        if (sev.get(RuleId.STREAMS_NO_UNCAUGHT_EXCEPTION_HANDLER) != Severity.OFF) {
            rules.add(new StreamsNoUncaughtExceptionHandlerRule(sev.get(RuleId.STREAMS_NO_UNCAUGHT_EXCEPTION_HANDLER)));
        }
        if (sev.get(RuleId.STREAMS_NO_SHUTDOWN_HOOK) != Severity.OFF) {
            rules.add(new StreamsNoShutdownHookRule(sev.get(RuleId.STREAMS_NO_SHUTDOWN_HOOK)));
        }
        if (sev.get(RuleId.STREAMS_NO_STATE_LISTENER) != Severity.OFF) {
            rules.add(new StreamsNoStateListenerRule(sev.get(RuleId.STREAMS_NO_STATE_LISTENER)));
        }
        if (sev.get(RuleId.STREAMS_NO_GLOBAL_STATE_RESTORE_LISTENER) != Severity.OFF) {
            rules.add(new StreamsNoGlobalStateRestoreListenerRule(sev.get(RuleId.STREAMS_NO_GLOBAL_STATE_RESTORE_LISTENER)));
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
        if (sev.get(RuleId.QK_INCOMING_PATTERN_TRUE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_INCOMING_PATTERN_TRUE, sev.get(RuleId.QK_INCOMING_PATTERN_TRUE),
                    "incoming", "pattern", "true",
                    "mp.messaging.incoming.{channel}.pattern=true — topic is interpreted as a regex matched against every topic in the cluster on every metadata refresh; new tenant topics or accidental matches trigger rebalances cluster-wide. Use an explicit topic list instead."));
        }
        if (sev.get(RuleId.QK_OUTGOING_PARTITION_PINNED) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.predicate(
                    RuleId.QK_OUTGOING_PARTITION_PINNED, sev.get(RuleId.QK_OUTGOING_PARTITION_PINNED),
                    "outgoing", "partition",
                    v -> { if (v == null) return false; try { return Integer.parseInt(v.trim()) >= 0; } catch (NumberFormatException e) { return false; } },
                    "mp.messaging.outgoing.{channel}.partition={value} — pins every record to one partition. Throughput is capped at single-partition rate, key-based ordering is broken, and partition-N leader outage stalls the whole channel. Remove the override and let the partitioner pick."));
        }
        if (sev.get(RuleId.QK_OUTGOING_MAX_INFLIGHT_MESSAGES_TOO_HIGH) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.predicate(
                    RuleId.QK_OUTGOING_MAX_INFLIGHT_MESSAGES_TOO_HIGH, sev.get(RuleId.QK_OUTGOING_MAX_INFLIGHT_MESSAGES_TOO_HIGH),
                    "outgoing", "max-inflight-messages",
                    v -> { if (v == null) return false; try { return Long.parseLong(v.trim()) > 10_000L; } catch (NumberFormatException e) { return false; } },
                    "mp.messaging.outgoing.{channel}.max-inflight-messages={value} — above 10000. The connector relaxes backpressure to the upstream Mutiny stream; during broker slowness the in-memory queue grows unbounded, leading to producer-pod OOM. Default 1024 is right; raise kafka-clients buffer.memory / max.in.flight.requests.per.connection if you need more in-flight."));
        }
        if (sev.get(RuleId.QK_INCOMING_BROADCAST_TRUE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_INCOMING_BROADCAST_TRUE, sev.get(RuleId.QK_INCOMING_BROADCAST_TRUE),
                    "incoming", "broadcast", "true",
                    "mp.messaging.incoming.{channel}.broadcast=true — every record fans out in-process to every subscriber of the channel; the slowest subscriber pins the channel's throughput, at-least-once semantics break per-subscriber, and a stuck subscriber turns into producer-side OOM. Use a second consumer group instead."));
        }
        if (sev.get(RuleId.QK_OUTGOING_CLOSE_TIMEOUT_TOO_LOW) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.predicate(
                    RuleId.QK_OUTGOING_CLOSE_TIMEOUT_TOO_LOW, sev.get(RuleId.QK_OUTGOING_CLOSE_TIMEOUT_TOO_LOW),
                    "outgoing", "close-timeout",
                    v -> { if (v == null) return false; try { long n = Long.parseLong(v.trim()); return n > 0 && n < 5_000L; } catch (NumberFormatException e) { return false; } },
                    "mp.messaging.outgoing.{channel}.close-timeout={value} — below 5000 ms. On graceful shutdown the producer must flush its accumulator before this deadline; below 5 s, every rolling deploy silently drops the records in-flight. Default 10000 ms is right; raise the pod's terminationGracePeriodSeconds if you need a bigger shutdown budget, don't shrink close-timeout."));
        }
        if (sev.get(RuleId.QK_INCOMING_THROTTLED_UNPROCESSED_RECORD_MAX_AGE_MS_TOO_LOW) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.predicate(
                    RuleId.QK_INCOMING_THROTTLED_UNPROCESSED_RECORD_MAX_AGE_MS_TOO_LOW, sev.get(RuleId.QK_INCOMING_THROTTLED_UNPROCESSED_RECORD_MAX_AGE_MS_TOO_LOW),
                    "incoming", "throttled.unprocessed-record-max-age.ms",
                    v -> { if (v == null) return false; try { long n = Long.parseLong(v.trim()); return n > 0 && n < 5_000L; } catch (NumberFormatException e) { return false; } },
                    "mp.messaging.incoming.{channel}.throttled.unprocessed-record-max-age.ms={value} — below 5000 ms. The throttled commit-strategy flips the health probe to UNHEALTHY for any unacked record older than this, so ordinary downstream slowness triggers pod restart. Default 60000 ms is calibrated to catch stuck (not slow) processing."));
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
        if (sev.get(RuleId.QK_OUTGOING_PARTITIONER_CLASS_DEPRECATED) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.predicate(
                    RuleId.QK_OUTGOING_PARTITIONER_CLASS_DEPRECATED, sev.get(RuleId.QK_OUTGOING_PARTITIONER_CLASS_DEPRECATED),
                    "outgoing", "partitioner.class",
                    v -> v != null && KafkaTypes.PARTITIONER_DEPRECATED_FQCNS.stream().anyMatch(v.trim()::equals),
                    "mp.messaging.outgoing.{channel}.partitioner.class={value} — deprecated by KIP-794 (Kafka 3.3). Both DefaultPartitioner and UniformStickyPartitioner are superseded by the built-in queue-and-RTT-aware strategy. Delete the line; the new partitioner is strictly better under uneven broker load."));
        }
        if (sev.get(RuleId.QK_OUTGOING_COMPRESSION_TYPE_GZIP) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_OUTGOING_COMPRESSION_TYPE_GZIP, sev.get(RuleId.QK_OUTGOING_COMPRESSION_TYPE_GZIP),
                    "outgoing", "compression.type", "gzip",
                    "mp.messaging.outgoing.{channel}.compression.type=gzip — gzip has 2-5× the CPU cost of lz4/zstd at the same or worse ratio on Kafka batch sizes. Use zstd (best ratio, fast) or lz4 (lowest CPU)."));
        }
        if (sev.get(RuleId.QK_FAIL_ON_DESERIALIZATION_FAILURE_FALSE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_FAIL_ON_DESERIALIZATION_FAILURE_FALSE, sev.get(RuleId.QK_FAIL_ON_DESERIALIZATION_FAILURE_FALSE),
                    "incoming", "fail-on-deserialization-failure", "false",
                    "mp.messaging.incoming.{channel}.fail-on-deserialization-failure=false — undeserializable records are logged once at WARN, replaced by null, and the offset advances. Silent data loss; configure a dead-letter-queue strategy instead."));
        }
        if (sev.get(RuleId.QK_OUTGOING_MERGE_TRUE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_OUTGOING_MERGE_TRUE, sev.get(RuleId.QK_OUTGOING_MERGE_TRUE),
                    "outgoing", "merge", "true",
                    "mp.messaging.outgoing.{channel}.merge=true — multiple @Outgoing producers feed one channel without back-pressure coordination or per-key ordering across upstreams. Fast producer can starve slow one in buffer.memory."));
        }
        if (sev.get(RuleId.QK_INCOMING_PAUSE_IF_NO_REQUESTS_FALSE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_INCOMING_PAUSE_IF_NO_REQUESTS_FALSE, sev.get(RuleId.QK_INCOMING_PAUSE_IF_NO_REQUESTS_FALSE),
                    "incoming", "pause-if-no-requests", "false",
                    "mp.messaging.incoming.{channel}.pause-if-no-requests=false — smallrye no longer pauses kafka-clients when downstream Mutiny has zero requests. Decoded records accumulate in an unbounded in-memory queue; sustained downstream slowness OOMs the pod."));
        }
        if (sev.get(RuleId.QK_LAZY_CLIENT_TRUE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_LAZY_CLIENT_TRUE, sev.get(RuleId.QK_LAZY_CLIENT_TRUE),
                    null, "lazy-client", "true",
                    "mp.messaging.{direction}.{channel}.lazy-client=true — kafka-clients producer/consumer deferred to first subscribe. Broker connectivity, SASL, and topic-existence failures surface after the pod is already Ready instead of at boot."));
        }
        if (sev.get(RuleId.QK_OUTGOING_KEY_LITERAL) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.predicate(
                    RuleId.QK_OUTGOING_KEY_LITERAL, sev.get(RuleId.QK_OUTGOING_KEY_LITERAL),
                    "outgoing", "key",
                    v -> v != null && !v.trim().isEmpty(),
                    "mp.messaging.outgoing.{channel}.key={value} — static literal key. Every record hashes to the same partition; topic effectively single-partition. Set the key per-record via OutgoingKafkaRecordMetadata instead."));
        }
        if (sev.get(RuleId.QK_INCOMING_BATCH_TRUE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_INCOMING_BATCH_TRUE, sev.get(RuleId.QK_INCOMING_BATCH_TRUE),
                    "incoming", "batch", "true",
                    "mp.messaging.incoming.{channel}.batch=true — listener receives Message<List<T>>; any single bad record fails the whole batch under default failure-strategy=fail and re-delivers the entire batch including successful records. Pair with explicit failure-strategy=dead-letter-queue and per-record error handling."));
        }
        if (sev.get(RuleId.QK_INCOMING_RETRY_TRUE) != Severity.OFF) {
            rules.add(SmallRyeChannelConfigRule.literal(
                    RuleId.QK_INCOMING_RETRY_TRUE, sev.get(RuleId.QK_INCOMING_RETRY_TRUE),
                    "incoming", "retry", "true",
                    "mp.messaging.incoming.{channel}.retry=true — smallrye retries the @Incoming method indefinitely (default retry-attempts=-1) before nacking. On a deterministic poison record the offset never advances and lag grows unbounded with no application error signal. Cap retry-attempts AND set failure-strategy=dead-letter-queue."));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_TIMEOUT_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_TIMEOUT_TOO_LOW, sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_TIMEOUT_TOO_LOW),
                    "spring.kafka.producer.properties.transaction.timeout.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 10_000L; },
                    "spring.kafka.producer.properties.transaction.timeout.ms={value} — below 10 s. Transactions abort spuriously inside normal commit windows (database write + downstream HTTP + producer.send routinely takes seconds); every abort rolls back the offset and the wrapped JDBC transaction. Default 60 s.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_BLOCK_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_MAX_BLOCK_MS_TOO_LOW, sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_BLOCK_MS_TOO_LOW),
                    "spring.kafka.producer.properties.max.block.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 5_000L; },
                    "spring.kafka.producer.properties.max.block.ms={value} — below 5 s. Routine metadata refreshes, transactional handshakes, and brief accumulator-full stalls surface as TimeoutException to the caller instead of being absorbed by the producer. Default 60 s.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_REQUEST_TIMEOUT_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_REQUEST_TIMEOUT_MS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_REQUEST_TIMEOUT_MS_TOO_HIGH),
                    "spring.kafka.producer.properties.request.timeout.ms",
                    v -> parseLongOrZero(v) > 120_000L,
                    "spring.kafka.producer.properties.request.timeout.ms={value} — above 2 min. Each silent leader failure pins the Sender thread on the dead broker for the full window before retry; produce p99.9 jumps to request.timeout.ms, and delivery.timeout.ms math breaks. Default 30 s.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_ISOLATION_LEVEL_READ_UNCOMMITTED_EXPLICIT) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_ISOLATION_LEVEL_READ_UNCOMMITTED_EXPLICIT, sev.get(RuleId.SPRING_BOOT_CONSUMER_ISOLATION_LEVEL_READ_UNCOMMITTED_EXPLICIT),
                    "spring.kafka.consumer.isolation-level",
                    v -> v != null && "read_uncommitted".equalsIgnoreCase(v.trim()),
                    "spring.kafka.consumer.isolation-level={value} — explicit read_uncommitted on a transactional topic returns aborted and in-flight transaction records; the consumer's side effects run on writes the producer subsequently rolls back. On non-transactional topics the setting is a no-op; explicit declaration is a copy-paste smell.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_COMPRESSION_TYPE_GZIP) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_PRODUCER_COMPRESSION_TYPE_GZIP, sev.get(RuleId.SPRING_BOOT_PRODUCER_COMPRESSION_TYPE_GZIP),
                    "spring.kafka.producer.compression-type", "gzip",
                    "spring.kafka.producer.compression-type=gzip — gzip has 2-5× the CPU cost of lz4/zstd for an identical or worse compression ratio on Kafka batch sizes. zstd or lz4 are strictly better in 2026; the transition is online.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_LOW, sev.get(RuleId.SPRING_BOOT_CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_LOW),
                    "spring.kafka.consumer.properties.default.api.timeout.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 30_000L; },
                    "spring.kafka.consumer.properties.default.api.timeout.ms={value} — below 30 s. Routine partition-leader moves and broker rolling restarts exhaust the retry budget; commitSync, position, endOffsets, partitionsFor all start throwing TimeoutException during events the consumer should absorb. Default 60 s.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_HIGH),
                    "spring.kafka.consumer.properties.default.api.timeout.ms",
                    v -> parseLongOrZero(v) > 300_000L,
                    "spring.kafka.consumer.properties.default.api.timeout.ms={value} — above 5 min. Blocking consumer calls (commitSync, position, listTopics, partitionsFor) hang the calling thread for the full window; graceful shutdown stalls past the pod's terminationGracePeriod. Default 60 s.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_AUTO_OFFSET_RESET_INVALID) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_AUTO_OFFSET_RESET_INVALID, sev.get(RuleId.SPRING_BOOT_CONSUMER_AUTO_OFFSET_RESET_INVALID),
                    "spring.kafka.consumer.auto-offset-reset",
                    v -> { if (v == null) return false; String t = v.trim().toLowerCase(); return !t.isEmpty() && !KafkaTypes.CONSUMER_AUTO_OFFSET_RESET_VALID_VALUES.contains(t); },
                    "spring.kafka.consumer.auto-offset-reset={value} — not one of earliest/latest/none. The consumer throws ConfigException at construction; the pod crash-loops on first deploy.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_BYTES_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_BYTES_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_BYTES_TOO_HIGH),
                    "spring.kafka.consumer.fetch-max-size",
                    v -> parseSpringDataSizeBytes(v) > 100L * 1024L * 1024L,
                    "spring.kafka.consumer.fetch-max-size={value} — above 100 MiB. A single FetchResponse can stall the consumer for seconds and pin that many bytes in heap per request; defaults around 50 MiB are safer.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_ACKS_INVALID) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_ACKS_INVALID, sev.get(RuleId.SPRING_BOOT_PRODUCER_ACKS_INVALID),
                    "spring.kafka.producer.acks",
                    v -> { if (v == null) return false; String t = v.trim().toLowerCase(); return !t.isEmpty() && !KafkaTypes.PRODUCER_ACKS_VALID_VALUES.contains(t); },
                    "spring.kafka.producer.acks={value} — not one of 0/1/-1/all. The producer throws ConfigException at construction; the pod crash-loops on first deploy.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_BLOCK_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_MAX_BLOCK_MS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_BLOCK_MS_TOO_HIGH),
                    "spring.kafka.producer.properties.max.block.ms",
                    v -> parseLongOrZero(v) > 60_000L,
                    "spring.kafka.producer.properties.max.block.ms={value} — above 60 s. producer.send() and partitionsFor() block the calling thread for the full window when the buffer is full or metadata is stale; request-handler thread pools drain within seconds during transient broker hiccups. Default 60 s.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_REQUEST_SIZE_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_MAX_REQUEST_SIZE_TOO_LOW, sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_REQUEST_SIZE_TOO_LOW),
                    "spring.kafka.producer.properties.max.request.size",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 65536L; },
                    "spring.kafka.producer.properties.max.request.size={value} — below 64 KiB. Defeats batching, and any single record above the cap throws RecordTooLargeException at send time with no retry. Default 1 MiB.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_LOW, sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_LOW),
                    "spring.kafka.consumer.properties.max.partition.fetch.bytes",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 1_048_576L; },
                    "spring.kafka.consumer.properties.max.partition.fetch.bytes={value} — below 1 MiB. Any single record above the cap stalls the partition with RecordTooLargeException and the consumer hard-stops without progress. Default 1 MiB.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_RECONNECT_BACKOFF_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_RECONNECT_BACKOFF_MS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_RECONNECT_BACKOFF_MS_TOO_HIGH),
                    "spring.kafka.producer.properties.reconnect.backoff.ms",
                    v -> parseLongOrZero(v) > 10_000L,
                    "spring.kafka.producer.properties.reconnect.backoff.ms={value} — above 10 s. After a broker disconnect, the producer waits the full backoff before any reconnect attempt; partition leader changes take an order of magnitude longer to recover than the underlying TCP reset. Default 50 ms.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_RETRY_BACKOFF_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_RETRY_BACKOFF_MS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_RETRY_BACKOFF_MS_TOO_HIGH),
                    "spring.kafka.producer.properties.retry.backoff.ms",
                    v -> parseLongOrZero(v) > 30_000L,
                    "spring.kafka.producer.properties.retry.backoff.ms={value} — above 30 s. After a retriable error the producer waits the full backoff between retry attempts; delivery.timeout.ms math breaks and routine partition-leader moves cause delivery failures. Default 100 ms.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_SEND_BUFFER_BYTES_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_SEND_BUFFER_BYTES_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_SEND_BUFFER_BYTES_TOO_HIGH),
                    "spring.kafka.producer.properties.send.buffer.bytes",
                    v -> parseLongOrZero(v) > 16L * 1024L * 1024L,
                    "spring.kafka.producer.properties.send.buffer.bytes={value} — over 16 MiB. Pins SO_SNDBUF per broker connection in kernel memory (invisible to JVM heap profilers); on a wide cluster this silently chews hundreds of MiB. Throughput gain over the autotuned default is nil — set to -1 (default) and let Linux TCP autotuning size it.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_SEND_BUFFER_BYTES_TOO_SMALL) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_SEND_BUFFER_BYTES_TOO_SMALL, sev.get(RuleId.SPRING_BOOT_PRODUCER_SEND_BUFFER_BYTES_TOO_SMALL),
                    "spring.kafka.producer.properties.send.buffer.bytes",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n <= 16384L; },
                    "spring.kafka.producer.properties.send.buffer.bytes={value} — at or below 16 KiB. Strangles SO_SNDBUF to a value smaller than a single TCP send window; producer throughput collapses to buffer/RTT, an order of magnitude below the network's actual capacity. Set to -1 and let Linux autotune.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_RECEIVE_BUFFER_BYTES_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_RECEIVE_BUFFER_BYTES_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_RECEIVE_BUFFER_BYTES_TOO_HIGH),
                    "spring.kafka.consumer.properties.receive.buffer.bytes",
                    v -> parseLongOrZero(v) > 16L * 1024L * 1024L,
                    "spring.kafka.consumer.properties.receive.buffer.bytes={value} — over 16 MiB. Pins SO_RCVBUF per broker connection in kernel memory; throughput ceiling is set by fetch.max.bytes / max.partition.fetch.bytes anyway, so the extra buffer is pure overhead. Set to -1 and let Linux autotune.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_RECEIVE_BUFFER_BYTES_TOO_SMALL) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_RECEIVE_BUFFER_BYTES_TOO_SMALL, sev.get(RuleId.SPRING_BOOT_CONSUMER_RECEIVE_BUFFER_BYTES_TOO_SMALL),
                    "spring.kafka.consumer.properties.receive.buffer.bytes",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n <= 16384L; },
                    "spring.kafka.consumer.properties.receive.buffer.bytes={value} — at or below 16 KiB. Strangles SO_RCVBUF to a value smaller than a single TCP receive window; throughput collapses and the consumer pays an order-of-magnitude latency hit per fetch. Set to -1 and let Linux autotune.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_RECONNECT_BACKOFF_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_RECONNECT_BACKOFF_MS_TOO_LOW, sev.get(RuleId.SPRING_BOOT_PRODUCER_RECONNECT_BACKOFF_MS_TOO_LOW),
                    "spring.kafka.producer.properties.reconnect.backoff.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 100L; },
                    "spring.kafka.producer.properties.reconnect.backoff.ms={value} — below 100 ms. Broker outage turns into a tight reconnect loop from this client; the producer hammers the broker with TCP connect attempts faster than the kernel can clean up failed sockets. Default 50 ms; recommended floor 100 ms.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_RETRY_BACKOFF_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_RETRY_BACKOFF_MS_TOO_LOW, sev.get(RuleId.SPRING_BOOT_PRODUCER_RETRY_BACKOFF_MS_TOO_LOW),
                    "spring.kafka.producer.properties.retry.backoff.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 50L; },
                    "spring.kafka.producer.properties.retry.backoff.ms={value} — below 50 ms. Retry loop pounds the broker before it has time to recover from the retriable error that just happened. Default 100 ms.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_WAIT_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_WAIT_MS_TOO_LOW, sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_WAIT_MS_TOO_LOW),
                    "spring.kafka.consumer.fetch-max-wait",
                    v -> { long n = parseSpringDurationMs(v); return n > 0 && n < 50L; },
                    "spring.kafka.consumer.fetch-max-wait={value} — below 50 ms. Broker returns immediately even when fetch.min.bytes is not satisfied; consumer spins in tight empty-fetch loop on quiet topics. Default 500 ms.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PARTITIONER_IGNORE_KEYS_TRUE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_PRODUCER_PARTITIONER_IGNORE_KEYS_TRUE, sev.get(RuleId.SPRING_BOOT_PRODUCER_PARTITIONER_IGNORE_KEYS_TRUE),
                    "spring.kafka.producer.properties.partitioner.ignore.keys", "true",
                    "spring.kafka.producer.properties.partitioner.ignore.keys=true — record key is ignored for partition routing; key-based ordering and compacted-topic semantics break silently.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_DISABLED) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_DISABLED, sev.get(RuleId.SPRING_BOOT_PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_DISABLED),
                    "spring.kafka.producer.properties.partitioner.adaptive.partitioning.enable", "false",
                    "spring.kafka.producer.properties.partitioner.adaptive.partitioning.enable=false — opts the producer out of KIP-794 adaptive partitioning; the built-in partitioner reverts to round-robin across all partitions and stops avoiding slow brokers.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_EXCLUDE_INTERNAL_TOPICS_FALSE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_CONSUMER_EXCLUDE_INTERNAL_TOPICS_FALSE, sev.get(RuleId.SPRING_BOOT_CONSUMER_EXCLUDE_INTERNAL_TOPICS_FALSE),
                    "spring.kafka.consumer.properties.exclude.internal.topics", "false",
                    "spring.kafka.consumer.properties.exclude.internal.topics=false — consumer can subscribe to __consumer_offsets / __transaction_state via regex; subscribing to internal topics either grants read access to sensitive cluster metadata or causes deserialization crashes.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_CLIENT_DNS_LOOKUP_DEFAULT) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_PRODUCER_CLIENT_DNS_LOOKUP_DEFAULT, sev.get(RuleId.SPRING_BOOT_PRODUCER_CLIENT_DNS_LOOKUP_DEFAULT),
                    "spring.kafka.producer.properties.client.dns.lookup", "default",
                    "spring.kafka.producer.properties.client.dns.lookup=default — removed in kafka-clients 3.0; the producer throws ConfigException at startup and the pod crash-loops on first deploy. Switch to use_all_dns_ips.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_PLACEHOLDER),
                    "spring.kafka.producer.transaction-id-prefix",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.producer.transaction-id-prefix={value} — unresolved ${...} placeholder reaches Spring; the literal string is taken as the transactional.id prefix, producer instances collide on the broker, and InvalidProducerEpochException fences live producers.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_GROUP_ID_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_GROUP_ID_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_CONSUMER_GROUP_ID_PLACEHOLDER),
                    "spring.kafka.consumer.group-id",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.consumer.group-id={value} — unresolved ${...} placeholder; the literal string becomes group.id, consumers across pods land in the wrong (or empty) group, partitions get reassigned the moment the real value lands.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_BOOTSTRAP_SERVERS_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_BOOTSTRAP_SERVERS_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_BOOTSTRAP_SERVERS_PLACEHOLDER),
                    "spring.kafka.bootstrap-servers",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.bootstrap-servers={value} — unresolved ${...} placeholder; the literal string is parsed as a hostname, DNS resolution fails, every Kafka client in the application throws ConfigException at startup and the pod crash-loops.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_TEMPLATE_TRANSACTION_ID_PREFIX_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_TEMPLATE_TRANSACTION_ID_PREFIX_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_TEMPLATE_TRANSACTION_ID_PREFIX_PLACEHOLDER),
                    "spring.kafka.template.transaction-id-prefix",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.template.transaction-id-prefix={value} — unresolved ${...} placeholder reaches KafkaTemplate as the literal transactional.id prefix; producer instances collide on the broker and InvalidProducerEpochException fences live producers on every transaction.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_APPLICATION_ID_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_APPLICATION_ID_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_STREAMS_APPLICATION_ID_PLACEHOLDER),
                    "spring.kafka.streams.application-id",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.streams.application-id={value} — unresolved ${...} placeholder; the literal string becomes application.id, creating broker-side groups and internal topics with the placeholder text and forking every replica into the same broken streams identity.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_SHUTDOWN_TIMEOUT_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_LISTENER_SHUTDOWN_TIMEOUT_TOO_LOW, sev.get(RuleId.SPRING_BOOT_LISTENER_SHUTDOWN_TIMEOUT_TOO_LOW),
                    "spring.kafka.listener.shutdown-timeout",
                    v -> { long n = parseSpringDurationMs(v); return n > 0 && n < 10000L; },
                    "spring.kafka.listener.shutdown-timeout={value} — below 10s; on graceful shutdown the listener container is force-killed mid-batch, in-flight records aren't committed, duplicates are reprocessed on the next start, and the consumer's missing LeaveGroup stalls the rebalance.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_CLEANUP_ON_SHUTDOWN_TRUE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_CLEANUP_ON_SHUTDOWN_TRUE, sev.get(RuleId.SPRING_BOOT_STREAMS_CLEANUP_ON_SHUTDOWN_TRUE),
                    "spring.kafka.streams.cleanup.on-shutdown", "true",
                    "spring.kafka.streams.cleanup.on-shutdown=true — the local state directory is wiped on every shutdown; the next startup must rebuild every state store by replaying the changelog topic from offset 0, taking minutes-to-hours.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_TEMPLATE_DEFAULT_TOPIC_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_TEMPLATE_DEFAULT_TOPIC_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_TEMPLATE_DEFAULT_TOPIC_PLACEHOLDER),
                    "spring.kafka.template.default-topic",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.template.default-topic={value} — unresolved ${...} placeholder; the literal string becomes the default topic and every KafkaTemplate.send(payload) shorthand call throws InvalidTopicException at the broker.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_APPLICATION_SERVER_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_APPLICATION_SERVER_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_STREAMS_APPLICATION_SERVER_PLACEHOLDER),
                    "spring.kafka.streams.properties.application.server",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.streams.properties.application.server={value} — unresolved ${...} placeholder; the local Streams instance advertises a literal host:port to peers, and every interactive-query request routed to this instance fails with UnknownHostException or NumberFormatException.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_ADMIN_FAIL_FAST_FALSE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_ADMIN_FAIL_FAST_FALSE, sev.get(RuleId.SPRING_BOOT_ADMIN_FAIL_FAST_FALSE),
                    "spring.kafka.admin.fail-fast", "false",
                    "spring.kafka.admin.fail-fast=false — KafkaAdmin silently retries broker connection on startup; misconfigured bootstrap-servers, expired credentials, or a down cluster don't crash the application but leave NewTopic beans unapplied and the cluster admin client in a permanent reconnection loop.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_CLIENT_ID_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_CLIENT_ID_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_CONSUMER_CLIENT_ID_PLACEHOLDER),
                    "spring.kafka.consumer.client-id",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.consumer.client-id={value} — unresolved ${...} placeholder; the literal string becomes client.id, broker-side quotas and metrics group every misconfigured pod into one quota bucket, and operational tooling can't attribute behavior to a specific instance.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_CLIENT_ID_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_CLIENT_ID_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_PRODUCER_CLIENT_ID_PLACEHOLDER),
                    "spring.kafka.producer.client-id",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.producer.client-id={value} — unresolved ${...} placeholder; the literal string becomes client.id, broker-side quotas and metrics group every misconfigured pod into one quota bucket, and operational tooling can't attribute traffic to a specific instance.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_CLIENT_ID_PLACEHOLDER) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_CLIENT_ID_PLACEHOLDER, sev.get(RuleId.SPRING_BOOT_STREAMS_CLIENT_ID_PLACEHOLDER),
                    "spring.kafka.streams.client-id",
                    v -> looksLikeUnresolvedPlaceholder(v),
                    "spring.kafka.streams.client-id={value} — unresolved ${...} placeholder; the Streams runtime derives every internal consumer/producer/restore/admin client.id from the literal placeholder, collapsing broker observability and quotas across the entire Streams app.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_CLIENT_ID_GENERIC) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_CLIENT_ID_GENERIC, sev.get(RuleId.SPRING_BOOT_PRODUCER_CLIENT_ID_GENERIC),
                    "spring.kafka.producer.client-id",
                    v -> v != null && io.conductor.kafkalinter.scanner.KafkaTypes.KAFKA_GENERIC_CLIENT_IDS.contains(v.trim().toLowerCase(java.util.Locale.ROOT)),
                    "spring.kafka.producer.client-id={value} — generic value; broker-side quotas and metrics can't distinguish this application from every other app that copy-pasted the same identifier. Set a value that includes the application name and environment.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_CLIENT_ID_GENERIC) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_CLIENT_ID_GENERIC, sev.get(RuleId.SPRING_BOOT_CONSUMER_CLIENT_ID_GENERIC),
                    "spring.kafka.consumer.client-id",
                    v -> v != null && io.conductor.kafkalinter.scanner.KafkaTypes.KAFKA_GENERIC_CLIENT_IDS.contains(v.trim().toLowerCase(java.util.Locale.ROOT)),
                    "spring.kafka.consumer.client-id={value} — generic value; broker-side quotas and metrics can't distinguish this application from every other app that copy-pasted the same identifier. Set a value that includes the application name and environment.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_CLIENT_ID_GENERIC) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_CLIENT_ID_GENERIC, sev.get(RuleId.SPRING_BOOT_STREAMS_CLIENT_ID_GENERIC),
                    "spring.kafka.streams.client-id",
                    v -> v != null && io.conductor.kafkalinter.scanner.KafkaTypes.KAFKA_GENERIC_CLIENT_IDS.contains(v.trim().toLowerCase(java.util.Locale.ROOT)),
                    "spring.kafka.streams.client-id={value} — generic value; every internal Streams client.id carries the generic prefix and broker observability/quotas collapse across unrelated Streams apps. Set a value that includes the application name.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_LOG_CONTAINER_CONFIG_TRUE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_LISTENER_LOG_CONTAINER_CONFIG_TRUE, sev.get(RuleId.SPRING_BOOT_LISTENER_LOG_CONTAINER_CONFIG_TRUE),
                    "spring.kafka.listener.log-container-config", "true",
                    "spring.kafka.listener.log-container-config=true — Spring dumps the full effective consumer config at INFO on every container start, including credentials (sasl.jaas.config, ssl.*-password) and other sensitive properties; production logs leak secrets and bury real signal in restart noise.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_MISSING_TRAILING_HYPHEN) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_MISSING_TRAILING_HYPHEN, sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_MISSING_TRAILING_HYPHEN),
                    "spring.kafka.producer.transaction-id-prefix",
                    v -> v != null && !v.trim().isEmpty() && !looksLikeUnresolvedPlaceholder(v) && !v.trim().endsWith("-"),
                    "spring.kafka.producer.transaction-id-prefix={value} — does not end with '-'. Spring appends a per-instance numeric suffix directly to the prefix, producing tx-ids like '{value}0', '{value}1' that are hard to grep on the broker and visually confusable with other applications sharing the prefix. End the prefix with '-'.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_TRANSACTIONAL_ID_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_TRANSACTIONAL_ID_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_TRANSACTIONAL_ID_SET),
                    "spring.kafka.producer.properties.transactional.id",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.transactional.id={value} — bypasses Spring's per-instance tx-id-prefix machinery. Every replica boots with the same transactional.id and the broker fences them in a permanent ProducerFencedException loop. Remove this key and use spring.kafka.producer.transaction-id-prefix instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_GROUP_ID_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_GROUP_ID_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_GROUP_ID_SET),
                    "spring.kafka.consumer.properties.group.id",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.group.id={value} — bypasses Spring's group-id resolution and silently overrides @KafkaListener(groupId=...) annotations. Remove this key and use spring.kafka.consumer.group-id (or the per-listener annotation) instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_ACKS_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_ACKS_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_ACKS_SET),
                    "spring.kafka.producer.properties.acks",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.acks={value} — silently overrides spring.kafka.producer.acks. The passthrough wins at runtime, so the Spring DSL value becomes dead config. Remove this key and use spring.kafka.producer.acks instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_BOOTSTRAP_SERVERS_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_BOOTSTRAP_SERVERS_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_BOOTSTRAP_SERVERS_SET),
                    "spring.kafka.producer.properties.bootstrap.servers",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.bootstrap.servers={value} — overrides spring.kafka.bootstrap-servers for the producer only, while consumer and admin still use the top-level value. Split-brain bootstrap; producer writes go to a different cluster than the consumer reads from. Remove this key.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_AUTO_OFFSET_RESET_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_AUTO_OFFSET_RESET_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_AUTO_OFFSET_RESET_SET),
                    "spring.kafka.consumer.properties.auto.offset.reset",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.auto.offset.reset={value} — silently overrides spring.kafka.consumer.auto-offset-reset. The passthrough wins at runtime, so the Spring DSL value becomes dead config. Remove this key and use spring.kafka.consumer.auto-offset-reset instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_ENABLE_AUTO_COMMIT_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_ENABLE_AUTO_COMMIT_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_ENABLE_AUTO_COMMIT_SET),
                    "spring.kafka.consumer.properties.enable.auto.commit",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.enable.auto.commit={value} — overrides spring.kafka.consumer.enable-auto-commit. Spring's listener container makes ack-mode decisions from the DSL value while kafka-clients runs with the passthrough value; ack-mode commits and kafka-clients auto-commits race for the same partitions, breaking at-least-once semantics. Remove this key and use spring.kafka.consumer.enable-auto-commit instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_MAX_POLL_RECORDS_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_MAX_POLL_RECORDS_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_MAX_POLL_RECORDS_SET),
                    "spring.kafka.consumer.properties.max.poll.records",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.max.poll.records={value} — silently overrides spring.kafka.consumer.max-poll-records. The passthrough wins for kafka-clients but Spring's batch-listener sizing still reads the DSL value; the two values drift. Remove this key and use spring.kafka.consumer.max-poll-records instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_COMPRESSION_TYPE_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_COMPRESSION_TYPE_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_COMPRESSION_TYPE_SET),
                    "spring.kafka.producer.properties.compression.type",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.compression.type={value} — silently overrides spring.kafka.producer.compression-type. The passthrough wins at runtime, so the Spring DSL value becomes dead config and compression-bug fixes that touch the DSL line have no effect. Remove this key and use spring.kafka.producer.compression-type instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_RETRIES_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_RETRIES_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_RETRIES_SET),
                    "spring.kafka.producer.properties.retries",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.retries={value} — silently overrides spring.kafka.producer.retries. The passthrough wins at runtime, so the Spring DSL value becomes dead config and retry-policy fixes that touch the DSL line have no effect. Remove this key and use spring.kafka.producer.retries instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_BATCH_SIZE_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_BATCH_SIZE_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_BATCH_SIZE_SET),
                    "spring.kafka.producer.properties.batch.size",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.batch.size={value} — silently overrides spring.kafka.producer.batch-size. Tuning that edits the DSL value has no runtime effect because the passthrough wins; throughput debugging is misled. Remove this key and use spring.kafka.producer.batch-size instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_PROPERTIES_APPLICATION_ID_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_PROPERTIES_APPLICATION_ID_SET, sev.get(RuleId.SPRING_BOOT_STREAMS_PROPERTIES_APPLICATION_ID_SET),
                    "spring.kafka.streams.properties.application.id",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.streams.properties.application.id={value} — overrides spring.kafka.streams.application-id silently. Every changelog and repartition topic name is derived from the application-id, so the override moves the entire topology to a different set of internal topics and orphans the previous state. Remove this key and use spring.kafka.streams.application-id instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_BUFFER_MEMORY_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_BUFFER_MEMORY_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_BUFFER_MEMORY_SET),
                    "spring.kafka.producer.properties.buffer.memory",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.buffer.memory={value} — silently overrides spring.kafka.producer.buffer-memory. Capacity-planning decisions made from the DSL value are wrong; produce-latency debugging is misled. Remove this key and use spring.kafka.producer.buffer-memory instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_PROPERTIES_REPLICATION_FACTOR_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_PROPERTIES_REPLICATION_FACTOR_SET, sev.get(RuleId.SPRING_BOOT_STREAMS_PROPERTIES_REPLICATION_FACTOR_SET),
                    "spring.kafka.streams.properties.replication.factor",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.streams.properties.replication.factor={value} — silently overrides spring.kafka.streams.replication-factor. Every internal changelog and repartition topic is created with the passthrough value; if it's lower than the DSL value, state-store durability is silently downgraded and the next broker failure loses state. Remove this key and use spring.kafka.streams.replication-factor instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_CLIENT_ID_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_CLIENT_ID_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_CLIENT_ID_SET),
                    "spring.kafka.producer.properties.client.id",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.client.id={value} — silently overrides spring.kafka.producer.client-id. The property file documents one identity, broker JMX and quota tooling sees another. Remove this key and use spring.kafka.producer.client-id instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_CLIENT_ID_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_CLIENT_ID_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_CLIENT_ID_SET),
                    "spring.kafka.consumer.properties.client.id",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.client.id={value} — silently overrides spring.kafka.consumer.client-id. Per-listener client-id derivation is bypassed and broker observability collapses across listeners. Remove this key and use spring.kafka.consumer.client-id instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_PROPERTIES_CLIENT_ID_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_PROPERTIES_CLIENT_ID_SET, sev.get(RuleId.SPRING_BOOT_STREAMS_PROPERTIES_CLIENT_ID_SET),
                    "spring.kafka.streams.properties.client.id",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.streams.properties.client.id={value} — silently overrides spring.kafka.streams.client-id. Every internal Streams client.id is derived from this base; the passthrough corrupts every embedded consumer, producer, restore-consumer, and admin identity in one stroke. Remove this key and use spring.kafka.streams.client-id instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_ISOLATION_LEVEL_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_ISOLATION_LEVEL_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_ISOLATION_LEVEL_SET),
                    "spring.kafka.consumer.properties.isolation.level",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.isolation.level={value} — silently overrides spring.kafka.consumer.isolation-level. End-to-end exactly-once consumption semantics can be silently broken (read_committed -> read_uncommitted demotion); the property file documents one isolation level while the runtime uses another. Remove this key and use spring.kafka.consumer.isolation-level instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_KEY_SERIALIZER_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_KEY_SERIALIZER_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_KEY_SERIALIZER_SET),
                    "spring.kafka.producer.properties.key.serializer",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.key.serializer={value} — silently overrides spring.kafka.producer.key-serializer. The typed KafkaTemplate<K,V> no longer matches the runtime serializer; ClassCastException at send time or silent byte corruption. Remove this key and use spring.kafka.producer.key-serializer instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_VALUE_SERIALIZER_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_VALUE_SERIALIZER_SET, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_VALUE_SERIALIZER_SET),
                    "spring.kafka.producer.properties.value.serializer",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.producer.properties.value.serializer={value} — silently overrides spring.kafka.producer.value-serializer. The typed KafkaTemplate<K,V> no longer matches the runtime serializer; downstream consumers fail to deserialize because the on-wire format diverges from the documented contract. Remove this key and use spring.kafka.producer.value-serializer instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_PROPERTIES_BOOTSTRAP_SERVERS_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_PROPERTIES_BOOTSTRAP_SERVERS_SET, sev.get(RuleId.SPRING_BOOT_STREAMS_PROPERTIES_BOOTSTRAP_SERVERS_SET),
                    "spring.kafka.streams.properties.bootstrap.servers",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.streams.properties.bootstrap.servers={value} — overrides spring.kafka.bootstrap-servers for the Streams runtime only. Streams reads from and writes to a different cluster than the producer and consumer of the same application; records the producer writes never reach the Streams app. Remove this key.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_KEY_DESERIALIZER_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_KEY_DESERIALIZER_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_KEY_DESERIALIZER_SET),
                    "spring.kafka.consumer.properties.key.deserializer",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.key.deserializer={value} — silently overrides spring.kafka.consumer.key-deserializer. The @KafkaListener method signature no longer matches the runtime deserializer; first poll throws ClassCastException or — worse — silently decodes garbage. Remove this key and use spring.kafka.consumer.key-deserializer.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_VALUE_DESERIALIZER_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_VALUE_DESERIALIZER_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_VALUE_DESERIALIZER_SET),
                    "spring.kafka.consumer.properties.value.deserializer",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.value.deserializer={value} — silently overrides spring.kafka.consumer.value-deserializer. Spring's ErrorHandlingDeserializer wrapper (which routes poison records to the DLT) is bypassed, so a single malformed record kills the listener container. Remove this key and use spring.kafka.consumer.value-deserializer.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_BOOTSTRAP_SERVERS_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_BOOTSTRAP_SERVERS_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_BOOTSTRAP_SERVERS_SET),
                    "spring.kafka.consumer.properties.bootstrap.servers",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.bootstrap.servers={value} — overrides spring.kafka.bootstrap-servers for the consumer only. The consumer reads from a different cluster than the producer of the same application; messages the producer writes never reach this consumer. Remove this key.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_SESSION_TIMEOUT_MS_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_SESSION_TIMEOUT_MS_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_SESSION_TIMEOUT_MS_SET),
                    "spring.kafka.consumer.properties.session.timeout.ms",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.session.timeout.ms={value} — silently overrides spring.kafka.consumer.session-timeout. The two settings target the same broker-side key at different scales; the passthrough wins, leaving the DSL line dead. Remove this key and use spring.kafka.consumer.session-timeout instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_HEARTBEAT_INTERVAL_MS_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_HEARTBEAT_INTERVAL_MS_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_HEARTBEAT_INTERVAL_MS_SET),
                    "spring.kafka.consumer.properties.heartbeat.interval.ms",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.heartbeat.interval.ms={value} — silently overrides spring.kafka.consumer.heartbeat-interval. The session-timeout / heartbeat-interval invariant (heartbeat ≤ session-timeout/3) can be silently violated; consumer fails to join the group. Remove this key and use spring.kafka.consumer.heartbeat-interval instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_FETCH_MIN_BYTES_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_FETCH_MIN_BYTES_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_FETCH_MIN_BYTES_SET),
                    "spring.kafka.consumer.properties.fetch.min.bytes",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.fetch.min.bytes={value} — silently overrides spring.kafka.consumer.fetch-min-size. Bypasses the typed DataSize parser; the operator's tuning knob in the DSL has no effect. Remove this key and use spring.kafka.consumer.fetch-min-size instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_FETCH_MAX_BYTES_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_FETCH_MAX_BYTES_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_FETCH_MAX_BYTES_SET),
                    "spring.kafka.consumer.properties.fetch.max.bytes",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.fetch.max.bytes={value} — silently overrides spring.kafka.consumer.fetch-max-size. Bypasses the typed DataSize parser. Remove this key and use spring.kafka.consumer.fetch-max-size instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_FETCH_MAX_WAIT_MS_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_FETCH_MAX_WAIT_MS_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_FETCH_MAX_WAIT_MS_SET),
                    "spring.kafka.consumer.properties.fetch.max.wait.ms",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.fetch.max.wait.ms={value} — silently overrides spring.kafka.consumer.fetch-max-wait. Bypasses the typed Duration parser. Remove this key and use spring.kafka.consumer.fetch-max-wait instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_AUTO_COMMIT_INTERVAL_MS_SET) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_AUTO_COMMIT_INTERVAL_MS_SET, sev.get(RuleId.SPRING_BOOT_CONSUMER_PROPERTIES_AUTO_COMMIT_INTERVAL_MS_SET),
                    "spring.kafka.consumer.properties.auto.commit.interval.ms",
                    v -> v != null && !v.trim().isEmpty(),
                    "spring.kafka.consumer.properties.auto.commit.interval.ms={value} — silently overrides spring.kafka.consumer.auto-commit-interval. Hides the duplicate-or-loss window from review. Remove this key and use spring.kafka.consumer.auto-commit-interval instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_SIZE_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_SIZE_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_SIZE_TOO_HIGH),
                    "spring.kafka.consumer.fetch-max-size",
                    v -> parseSpringDataSizeBytes(v) > 100L * 1024L * 1024L,
                    "spring.kafka.consumer.fetch-max-size={value} — above 100 MiB. Each fetch response delivers up to that much data in one shot; heap allocation per broker connection × consumer fleet adds up fast, and decompression on the listener thread takes seconds. Default 50 MiB is the right ceiling.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_SIZE_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_SIZE_TOO_LOW, sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_SIZE_TOO_LOW),
                    "spring.kafka.consumer.fetch-max-size",
                    v -> { long n = parseSpringDataSizeBytes(v); return n > 0 && n < 1024L * 1024L; },
                    "spring.kafka.consumer.fetch-max-size={value} — below 1 MiB. Any single record larger than the cap risks a RecordTooLargeException + silent skip, or a stuck-fetch loop on the partition. Default 50 MiB; the floor is the largest expected record × 2.",
                    "org.springframework.kafka", "spring-kafka"));
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
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_AUTO_OFFSET_RESET_NONE_EXPLICIT) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_CONSUMER_AUTO_OFFSET_RESET_NONE_EXPLICIT, sev.get(RuleId.SPRING_BOOT_CONSUMER_AUTO_OFFSET_RESET_NONE_EXPLICIT),
                    "spring.kafka.consumer.auto-offset-reset", "none",
                    "spring.kafka.consumer.auto-offset-reset=none — on a fresh consumer group (first deploy, renamed group, new environment) the consumer raises NoOffsetForPartitionException and the listener container retries forever without ever reading a record. Use 'earliest' for replayable pipelines or 'latest' for subscribe-from-now.",
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
        if (sev.get(RuleId.SPRING_BOOT_AUTO_COMMIT_INTERVAL_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_AUTO_COMMIT_INTERVAL_TOO_LOW, sev.get(RuleId.SPRING_BOOT_AUTO_COMMIT_INTERVAL_TOO_LOW),
                    "spring.kafka.consumer.auto-commit-interval",
                    v -> { try { long n = Long.parseLong(v.trim()); return n > 0 && n < 100L; } catch (NumberFormatException e) { return false; } },
                    "spring.kafka.consumer.auto-commit-interval={value} — below 100 ms. Combined with auto-commit=true, the consumer commits on essentially every poll, hammering __consumer_offsets and saturating the group coordinator with offset-commit traffic. Default 5000 (5 s) is right; for stronger guarantees switch to manual ack-mode instead.",
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
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_RECORDS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_RECORDS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_RECORDS_TOO_HIGH),
                    "spring.kafka.consumer.max-poll-records",
                    v -> { try { long n = v == null ? 0 : Long.parseLong(v.trim()); return n > 5_000L; } catch (NumberFormatException e) { return false; } },
                    "spring.kafka.consumer.max-poll-records={value} — above 5000. Each poll hands the @KafkaListener a huge batch that must finish within max.poll.interval.ms (default 5 min) or the consumer is ejected from the group, triggering rolling rebalances and lag accumulation. Lower to <=2000 or raise max.poll.interval.ms in lock step.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_SESSION_TIMEOUT_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_SESSION_TIMEOUT_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_SESSION_TIMEOUT_TOO_HIGH),
                    "spring.kafka.consumer.session-timeout",
                    v -> parseSpringDurationMs(v) > 60_000L,
                    "spring.kafka.consumer.session-timeout={value} — above 60 s. Most managed brokers cap this at 60 s (group.max.session.timeout.ms); past that the consumer's JoinGroup is rejected at startup with InvalidSessionTimeout and the application crashloops. Even when accepted, a crashed pod's partitions stay frozen for the full window before rebalancing. The right lever for slow-batch evictions is max.poll.interval.ms, not session-timeout.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_HEARTBEAT_INTERVAL_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_HEARTBEAT_INTERVAL_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_HEARTBEAT_INTERVAL_TOO_HIGH),
                    "spring.kafka.consumer.heartbeat-interval",
                    v -> parseSpringDurationMs(v) >= 15_000L,
                    "spring.kafka.consumer.heartbeat-interval={value} — at/above session.timeout.ms / 3 (default session 45 s). One missed heartbeat (GC pause, network blip) now triggers eviction and a group-wide rebalance. Keep at the default 3 s.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_SESSION_TIMEOUT_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_SESSION_TIMEOUT_TOO_LOW, sev.get(RuleId.SPRING_BOOT_CONSUMER_SESSION_TIMEOUT_TOO_LOW),
                    "spring.kafka.consumer.session-timeout",
                    v -> { long n = parseSpringDurationMs(v); return n > 0 && n < 10_000L; },
                    "spring.kafka.consumer.session-timeout={value} — below 10 s. Routine GC pauses and network blips exceed the session window; the consumer is evicted and the group rebalances. Pure noise rebalances; default 45 s is almost always right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_HEARTBEAT_INTERVAL_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_HEARTBEAT_INTERVAL_TOO_LOW, sev.get(RuleId.SPRING_BOOT_CONSUMER_HEARTBEAT_INTERVAL_TOO_LOW),
                    "spring.kafka.consumer.heartbeat-interval",
                    v -> { long n = parseSpringDurationMs(v); return n > 0 && n < 1_000L; },
                    "spring.kafka.consumer.heartbeat-interval={value} — below 1 s. Floods the group coordinator with heartbeats; broker-side CPU goes up for zero detection benefit (session.timeout.ms is the actual eviction knob). Default 3 s is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_TIMEOUT_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_TIMEOUT_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_TRANSACTION_TIMEOUT_TOO_HIGH),
                    "spring.kafka.producer.properties.transaction.timeout.ms",
                    v -> parseLongOrZero(v) > 900_000L,
                    "spring.kafka.producer.properties.transaction.timeout.ms={value} — above 15 min. A crashed transactional producer blocks the LSO on every partition it wrote to for the full window; downstream read_committed consumers stall with zero error signal. Default 60 s lets the broker self-heal in a minute.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_REQUEST_TIMEOUT_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_REQUEST_TIMEOUT_MS_TOO_LOW, sev.get(RuleId.SPRING_BOOT_PRODUCER_REQUEST_TIMEOUT_MS_TOO_LOW),
                    "spring.kafka.producer.properties.request.timeout.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 10_000L; },
                    "spring.kafka.producer.properties.request.timeout.ms={value} — below 10 s. Routine cross-AZ produce latency burns through retries; bound the application call instead. Default 30 s is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_IN_FLIGHT_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_MAX_IN_FLIGHT_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_IN_FLIGHT_TOO_HIGH),
                    "spring.kafka.producer.properties.max.in.flight.requests.per.connection",
                    v -> { long n = parseLongOrZero(v); return n > 5L; },
                    "spring.kafka.producer.properties.max.in.flight.requests.per.connection={value} — above 5. Idempotent producer (default since 3.0) rejects this at startup with ConfigException; KafkaTemplate fails to wire.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_INTERVAL_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_INTERVAL_MS_TOO_LOW, sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_INTERVAL_MS_TOO_LOW),
                    "spring.kafka.consumer.properties.max.poll.interval.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 60_000L; },
                    "spring.kafka.consumer.properties.max.poll.interval.ms={value} — below 60 s. Any batch overrun evicts the consumer; group enters a poll/evict loop with unbounded re-delivery. Tighten max.poll.records instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_INTERVAL_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_INTERVAL_MS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_POLL_INTERVAL_MS_TOO_HIGH),
                    "spring.kafka.consumer.properties.max.poll.interval.ms",
                    v -> parseLongOrZero(v) > 1_800_000L,
                    "spring.kafka.consumer.properties.max.poll.interval.ms={value} — above 30 min. A stuck consumer holds its partitions for the full window; other replicas cannot take the load; lag grows unbounded with no detection signal.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MIN_BYTES_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_FETCH_MIN_BYTES_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MIN_BYTES_TOO_HIGH),
                    "spring.kafka.consumer.fetch-min-size",
                    v -> parseSpringDataSizeBytes(v) > 10L * 1024L * 1024L,
                    "spring.kafka.consumer.fetch-min-size={value} — above 10 MiB. Every fetch blocks until fetch-max-wait elapses; end-to-end latency floors at fetch-max-wait on quiet topics.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_WAIT_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_WAIT_MS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_FETCH_MAX_WAIT_MS_TOO_HIGH),
                    "spring.kafka.consumer.fetch-max-wait",
                    v -> parseSpringDurationMs(v) > 5_000L,
                    "spring.kafka.consumer.fetch-max-wait={value} — above 5 s. Empty fetches block the broker request handler for the full window; end-to-end latency floors at the wait time and max.poll.interval.ms margin shrinks.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_DELIVERY_TIMEOUT_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_DELIVERY_TIMEOUT_MS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_DELIVERY_TIMEOUT_MS_TOO_HIGH),
                    "spring.kafka.producer.properties.delivery.timeout.ms",
                    v -> parseLongOrZero(v) > 600_000L,
                    "spring.kafka.producer.properties.delivery.timeout.ms={value} — above 10 min. Stuck records sit in the accumulator for the full window; buffer.memory fills and send() blocks on the hot path before the failure surfaces.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_REQUEST_SIZE_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_MAX_REQUEST_SIZE_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_MAX_REQUEST_SIZE_TOO_HIGH),
                    "spring.kafka.producer.properties.max.request.size",
                    v -> parseLongOrZero(v) > 10L * 1024L * 1024L,
                    "spring.kafka.producer.properties.max.request.size={value} — above 10 MiB. Broker message.max.bytes and consumer fetch settings must move together; otherwise the broker rejects with RecordTooLargeException and stored records become unfetchable.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_BATCH_SIZE_TOO_LARGE) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_BATCH_SIZE_TOO_LARGE, sev.get(RuleId.SPRING_BOOT_PRODUCER_BATCH_SIZE_TOO_LARGE),
                    "spring.kafka.producer.batch-size",
                    v -> parseSpringDataSizeBytes(v) > 1_048_576L,
                    "spring.kafka.producer.batch-size={value} — above 1 MiB. buffer.memory holds only a handful of in-flight batches; one slow partition pins its slot for the full delivery.timeout window and stalls writes to healthy partitions.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_HIGH),
                    "spring.kafka.consumer.properties.max.partition.fetch.bytes",
                    v -> parseLongOrZero(v) > 16L * 1024L * 1024L,
                    "spring.kafka.consumer.properties.max.partition.fetch.bytes={value} — above 16 MiB. Worst-case heap per poll scales linearly with partitions; OOM mid-batch is self-sustaining (re-delivery hits the same OOM on restart).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE, sev.get(RuleId.SPRING_BOOT_CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE),
                    "spring.kafka.consumer.properties.allow.auto.create.topics", "true",
                    "spring.kafka.consumer.properties.allow.auto.create.topics=true — a typo in @KafkaListener silently creates a one-partition, default-RF topic. Listener attaches, sees zero records, no error logged. Disable client-side to surface typos as boot failures.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_CONSUMER_CHECK_CRCS_FALSE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_CONSUMER_CHECK_CRCS_FALSE, sev.get(RuleId.SPRING_BOOT_CONSUMER_CHECK_CRCS_FALSE),
                    "spring.kafka.consumer.properties.check.crcs", "false",
                    "spring.kafka.consumer.properties.check.crcs=false — consumer accepts records without verifying the on-the-wire CRC. Disk/network/memory corruption reaches the @KafkaListener as valid records. Default true; intrinsic CRC32C cost is <1% per GiB.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_LINGER_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_LINGER_MS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_LINGER_MS_TOO_HIGH),
                    "spring.kafka.producer.properties.linger.ms",
                    v -> { try { long n = v == null ? 0 : Long.parseLong(v.trim()); return n > 1_000L; } catch (NumberFormatException e) { return false; } },
                    "spring.kafka.producer.properties.linger.ms={value} — above 1 s. Every send waits up to the full linger before the batch is shipped; under low-rate workloads every record eats the full linger as latency, KafkaTemplate.send().get() blocks for the full duration. Acceptable range 5-100 ms.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_DELIVERY_TIMEOUT_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_DELIVERY_TIMEOUT_MS_TOO_LOW, sev.get(RuleId.SPRING_BOOT_PRODUCER_DELIVERY_TIMEOUT_MS_TOO_LOW),
                    "spring.kafka.producer.properties.delivery.timeout.ms",
                    v -> { try { long n = v == null ? 0 : Long.parseLong(v.trim()); return n > 0 && n < 30_000L; } catch (NumberFormatException e) { return false; } },
                    "spring.kafka.producer.properties.delivery.timeout.ms={value} — below 30 s. Internal retry budget cannot satisfy retries × retry.backoff.ms + request.timeout.ms; every transient broker hiccup surfaces as final TimeoutException instead of being recoverable. Default 120000 (2 min) is right.",
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
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_ACK_MODE_TIME) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_LISTENER_ACK_MODE_TIME, sev.get(RuleId.SPRING_BOOT_LISTENER_ACK_MODE_TIME),
                    "spring.kafka.listener.ack-mode",
                    v -> v != null && "time".equalsIgnoreCase(v.trim()),
                    "spring.kafka.listener.ack-mode={value} — Spring commits offsets every spring.kafka.listener.ack-time milliseconds regardless of listener progress. A tick can land mid-poll-batch and commit past records the listener has not finished; a crash then loses those records (silent at-most-once window). Use the default BATCH instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_ACK_MODE_COUNT) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_LISTENER_ACK_MODE_COUNT, sev.get(RuleId.SPRING_BOOT_LISTENER_ACK_MODE_COUNT),
                    "spring.kafka.listener.ack-mode",
                    v -> v != null && ("count".equalsIgnoreCase(v.trim()) || "count_time".equalsIgnoreCase(v.trim())),
                    "spring.kafka.listener.ack-mode={value} — commit cadence is independent of poll boundaries; crash-recovery duplicates aren't bounded by max.poll.records anymore, and rebalance-commit races count-commit. Lower max.poll.records and stay in BATCH instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_ACK_MODE_MANUAL) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_LISTENER_ACK_MODE_MANUAL, sev.get(RuleId.SPRING_BOOT_LISTENER_ACK_MODE_MANUAL),
                    "spring.kafka.listener.ack-mode",
                    v -> v != null && ("manual".equalsIgnoreCase(v.trim()) || "manual_immediate".equalsIgnoreCase(v.trim())),
                    "spring.kafka.listener.ack-mode={value} — the container will not commit offsets unless the listener calls Acknowledgment.acknowledge(). A single forgotten ack() in any code path turns the listener into an infinite-replay loop on every restart. Verify every reachable branch calls acknowledge() exactly once.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_MISSING_TOPICS_FATAL_FALSE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_LISTENER_MISSING_TOPICS_FATAL_FALSE, sev.get(RuleId.SPRING_BOOT_LISTENER_MISSING_TOPICS_FATAL_FALSE),
                    "spring.kafka.listener.missing-topics-fatal", "false",
                    "spring.kafka.listener.missing-topics-fatal=false — the app boots even when a configured @KafkaListener topic doesn't exist; the listener attaches to nothing and silently processes zero records. Set to true so topic typos / missing-topic situations become loud boot failures instead of silent consumption gaps.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_IDLE_BETWEEN_POLLS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_LISTENER_IDLE_BETWEEN_POLLS_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_LISTENER_IDLE_BETWEEN_POLLS_TOO_HIGH),
                    "spring.kafka.listener.idle-between-polls",
                    v -> parseLongOrZero(v) > 1_000L,
                    "spring.kafka.listener.idle-between-polls={value} — Spring inserts an artificial sleep between consecutive consumer polls. Throughput drops, and idle time pushes toward max.poll.interval.ms (5 min) so a slow batch on top fences the consumer. Remove the override or stay under 1000 ms.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_POLL_TIMEOUT_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_LISTENER_POLL_TIMEOUT_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_LISTENER_POLL_TIMEOUT_TOO_HIGH),
                    "spring.kafka.listener.poll-timeout",
                    v -> parseLongOrZero(v) > 30_000L,
                    "spring.kafka.listener.poll-timeout={value} — above 30s. The listener thread blocks inside consumer.poll() for that long, so container shutdown, rebalances, and lifecycle events stall by the same amount. Leave at the default 5000 ms.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_ASYNC_ACKS_TRUE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_LISTENER_ASYNC_ACKS_TRUE, sev.get(RuleId.SPRING_BOOT_LISTENER_ASYNC_ACKS_TRUE),
                    "spring.kafka.listener.async-acks", "true",
                    "spring.kafka.listener.async-acks=true — manual acknowledgements are queued on a background commit thread instead of running inline. Out-of-order commits are explicitly allowed: on a JVM crash between enqueue and broker-commit the offset is dropped (re-delivery after the side effect already ran), and within a batch with mixed success/failure offsets can be coalesced past a failed record. Default is false for at-least-once semantics.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SR_LATEST_COMPATIBILITY_STRICT_FALSE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SR_LATEST_COMPATIBILITY_STRICT_FALSE, sev.get(RuleId.SR_LATEST_COMPATIBILITY_STRICT_FALSE),
                    "spring.kafka.properties.latest.compatibility.strict", "false",
                    "spring.kafka.properties.latest.compatibility.strict=false — disables the runtime-vs-latest schema compatibility check. Combined with use.latest.version=true, the serializer projects records into the latest registered schema without verifying compatibility, silently dropping or coercing fields.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_NO_POLL_THRESHOLD_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_LISTENER_NO_POLL_THRESHOLD_TOO_LOW, sev.get(RuleId.SPRING_BOOT_LISTENER_NO_POLL_THRESHOLD_TOO_LOW),
                    "spring.kafka.listener.no-poll-threshold",
                    v -> {
                        if (v == null) return false;
                        try { double d = Double.parseDouble(v.trim()); return d > 0 && d < 2.0; }
                        catch (NumberFormatException e) { return false; }
                    },
                    "spring.kafka.listener.no-poll-threshold={value} — below 2.0. The NonResponsiveConsumerEvent fires inside the natural variance of poll-timeout, flooding logs and tripping false-positive alerts. Default 3.0 is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_BUFFER_MEMORY_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_BUFFER_MEMORY_TOO_LOW, sev.get(RuleId.SPRING_BOOT_PRODUCER_BUFFER_MEMORY_TOO_LOW),
                    "spring.kafka.producer.buffer-memory",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 16L * 1024L * 1024L; },
                    "spring.kafka.producer.buffer-memory={value} — below 16 MiB. The producer's accumulator fills under any burst; send() blocks for max.block.ms (60 s) and the application's hot path stalls. Default 33554432 (32 MiB) is right; raise linger.ms / tune batch.size instead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_BUFFER_MEMORY_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_BUFFER_MEMORY_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_PRODUCER_BUFFER_MEMORY_TOO_HIGH),
                    "spring.kafka.producer.buffer-memory",
                    v -> parseLongOrZero(v) > 256L * 1024L * 1024L,
                    "spring.kafka.producer.buffer-memory={value} — above 256 MiB. The accumulator reserves heap that the JVM cannot reclaim; under normal load the buffer is 99% empty. If you have measured back-pressure, raise linger.ms or add brokers/partitions instead of inflating the cap.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_CONCURRENCY_ZERO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_LISTENER_CONCURRENCY_ZERO, sev.get(RuleId.SPRING_BOOT_LISTENER_CONCURRENCY_ZERO),
                    "spring.kafka.listener.concurrency", "0",
                    "spring.kafka.listener.concurrency=0 — listener container creates zero consumer threads. Almost always a typo or env-substitution bug.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_LISTENER_CONCURRENCY_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_LISTENER_CONCURRENCY_TOO_HIGH, sev.get(RuleId.SPRING_BOOT_LISTENER_CONCURRENCY_TOO_HIGH),
                    "spring.kafka.listener.concurrency",
                    v -> { try { return Integer.parseInt(v.trim()) > 32; } catch (NumberFormatException e) { return false; } },
                    "spring.kafka.listener.concurrency={value} — above 32 threads in a single listener container. If the topic has fewer partitions, the surplus consumers join the group, get assigned zero partitions, and heartbeat forever while doing no work. Rebalances scale with group size; idle members make every rebalance slower for the whole group. Set concurrency × pod_count ≤ partitions; add partitions if you need more parallelism.",
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
        if (sev.get(RuleId.SPRING_BOOT_JSON_USE_TYPE_HEADERS_FALSE) != Severity.OFF) {
            Severity s = sev.get(RuleId.SPRING_BOOT_JSON_USE_TYPE_HEADERS_FALSE);
            String detail = "spring.json.use.type.headers=false — Spring's JsonDeserializer ignores the producer's __TypeId__ header and falls back to spring.json.value.default.type (default java.lang.Object → LinkedHashMap). Polymorphic topics deserialize to the wrong runtime type and downstream casts throw ClassCastException.";
            for (String key : new String[]{
                    "spring.kafka.consumer.properties.spring.json.use.type.headers",
                    "spring.kafka.producer.properties.spring.json.use.type.headers",
                    "spring.kafka.properties.spring.json.use.type.headers"}) {
                rules.add(PropertyFileRule.literal(
                        RuleId.SPRING_BOOT_JSON_USE_TYPE_HEADERS_FALSE, s,
                        key, "false", detail,
                        "org.springframework.kafka", "spring-kafka"));
            }
        }
        if (sev.get(RuleId.SPRING_BOOT_JSON_VALUE_DEFAULT_TYPE_OBJECT) != Severity.OFF) {
            Severity s = sev.get(RuleId.SPRING_BOOT_JSON_VALUE_DEFAULT_TYPE_OBJECT);
            String detail = "spring.json.value.default.type=java.lang.Object — explicit declaration of the framework's fallback type. Every record without a resolvable __TypeId__ header materializes as LinkedHashMap and downstream typed code (listener overloads, casts, convertValue) silently misroutes or throws. Set to a concrete class, or delete and rely on the type header.";
            for (String key : new String[]{
                    "spring.kafka.consumer.properties.spring.json.value.default.type",
                    "spring.kafka.properties.spring.json.value.default.type"}) {
                rules.add(PropertyFileRule.literal(
                        RuleId.SPRING_BOOT_JSON_VALUE_DEFAULT_TYPE_OBJECT, s,
                        key, "java.lang.Object", detail,
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
        if (sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_PARTITIONER_CLASS_DEPRECATED) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_PARTITIONER_CLASS_DEPRECATED, sev.get(RuleId.SPRING_BOOT_PRODUCER_PROPERTIES_PARTITIONER_CLASS_DEPRECATED),
                    "spring.kafka.producer.properties.partitioner.class",
                    v -> v != null && KafkaTypes.PARTITIONER_DEPRECATED_FQCNS.stream().anyMatch(v.trim()::equals),
                    "spring.kafka.producer.properties.partitioner.class={value} — deprecated by KIP-794 (Kafka 3.3). Both DefaultPartitioner and UniformStickyPartitioner are superseded by the built-in queue-and-RTT-aware strategy. Delete the line; the new partitioner is strictly better under uneven broker load.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_ONE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_ONE, sev.get(RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_ONE),
                    "spring.kafka.streams.replication-factor", "1",
                    "spring.kafka.streams.replication-factor=1 — Streams internal changelog and repartition topics will be created with replication-factor=1; a single broker restart loses state-store data. Set to 3 (or remove the line to inherit the broker's default.replication.factor).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_APPLICATION_ID_GENERIC) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_APPLICATION_ID_GENERIC, sev.get(RuleId.SPRING_BOOT_STREAMS_APPLICATION_ID_GENERIC),
                    "spring.kafka.streams.application-id",
                    v -> v != null && KafkaTypes.STREAMS_GENERIC_APPLICATION_IDS.contains(v.trim().toLowerCase()),
                    "spring.kafka.streams.application-id={value} — generic placeholder. application.id is the cluster-wide unique identity of the Streams app (consumer-group name, changelog topic prefix, state-dir prefix). Two apps with the same id collide on all three. Use a service-specific id including a topology version (e.g. payments-fraud-screening-v3).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_STATE_DIR_TMP) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_STATE_DIR_TMP, sev.get(RuleId.SPRING_BOOT_STREAMS_STATE_DIR_TMP),
                    "spring.kafka.streams.state-dir",
                    v -> v != null && (v.startsWith("/tmp") || v.startsWith("/var/tmp")),
                    "spring.kafka.streams.state-dir={value} — Streams RocksDB state stores on ephemeral /tmp or /var/tmp. systemd-tmpfiles wipes it on reboot; containers wipe it on restart. Every restart triggers a full restore-from-changelog (minutes-to-hours). Mount a persistent volume (e.g. /var/lib/<service>/streams).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_CLEANUP_ON_STARTUP) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_CLEANUP_ON_STARTUP, sev.get(RuleId.SPRING_BOOT_STREAMS_CLEANUP_ON_STARTUP),
                    "spring.kafka.streams.cleanup.on-startup", "true",
                    "spring.kafka.streams.cleanup.on-startup=true — Spring invokes KafkaStreams.cleanUp() on every boot, deleting the local state directory. Every restart then triggers a full restore-from-changelog (minutes-to-hours). Set to false (or remove the line; default is false).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_PROCESSING_GUARANTEE_EOS_V1) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_PROCESSING_GUARANTEE_EOS_V1, sev.get(RuleId.SPRING_BOOT_STREAMS_PROCESSING_GUARANTEE_EOS_V1),
                    "spring.kafka.streams.properties.processing.guarantee",
                    v -> v != null && KafkaTypes.STREAMS_EOS_V1_VALUES.contains(v.trim()),
                    "spring.kafka.streams.properties.processing.guarantee={value} — deprecated EOS-v1/EOS-beta value (KIP-732, Kafka 3.0). Removed in Kafka 4.0; app refuses to start on 4.x clusters. Use exactly_once_v2 (functionally identical to exactly_once_beta, available since 2.6).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_COMMIT_INTERVAL_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_COMMIT_INTERVAL_TOO_LOW, sev.get(RuleId.SPRING_BOOT_STREAMS_COMMIT_INTERVAL_TOO_LOW),
                    "spring.kafka.streams.properties.commit.interval.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 100L; },
                    "spring.kafka.streams.properties.commit.interval.ms={value} — below 100 ms. Pathological commit rate: every cycle is a RocksDB flush + changelog produce + offset commit. The runtime spends most of its time committing instead of processing. Kafka default is 100 ms under EOS-v2; lower than that is almost always a misunderstanding.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_NUM_STANDBY_REPLICAS_ZERO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_NUM_STANDBY_REPLICAS_ZERO, sev.get(RuleId.SPRING_BOOT_STREAMS_NUM_STANDBY_REPLICAS_ZERO),
                    "spring.kafka.streams.properties.num.standby.replicas", "0",
                    "spring.kafka.streams.properties.num.standby.replicas=0 — zero warm state-store copies. Any instance failure forces a full changelog restore on the takeover peer (minutes-to-hours for a non-trivial state store). Set to 1 in production.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_NUM_STREAM_THREADS_ONE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_NUM_STREAM_THREADS_ONE, sev.get(RuleId.SPRING_BOOT_STREAMS_NUM_STREAM_THREADS_ONE),
                    "spring.kafka.streams.properties.num.stream.threads", "1",
                    "spring.kafka.streams.properties.num.stream.threads=1 — entire topology on a single thread. Multi-vCPU pods leave most cores idle; any blocking processor call stalls all assigned tasks. Right value is roughly min(input-partitions/instances, cpu-cores).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_TOPOLOGY_OPTIMIZATION_NONE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_TOPOLOGY_OPTIMIZATION_NONE, sev.get(RuleId.SPRING_BOOT_STREAMS_TOPOLOGY_OPTIMIZATION_NONE),
                    "spring.kafka.streams.properties.topology.optimization", "none",
                    "spring.kafka.streams.properties.topology.optimization=none — optimizer disabled. Redundant repartition and duplicate source-KTable changelog topics are created on the broker. For new applications, set to 'all'.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_TASK_TIMEOUT_MS_ZERO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_TASK_TIMEOUT_MS_ZERO, sev.get(RuleId.SPRING_BOOT_STREAMS_TASK_TIMEOUT_MS_ZERO),
                    "spring.kafka.streams.properties.task.timeout.ms", "0",
                    "spring.kafka.streams.properties.task.timeout.ms=0 — Streams' per-task transient-error retry budget is zero. Routine leader elections (1-3 s on a healthy cluster) immediately kill the task. Default is 300000 (5 min); set that or remove the line.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_CACHE_DISABLED) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_CACHE_DISABLED, sev.get(RuleId.SPRING_BOOT_STREAMS_CACHE_DISABLED),
                    "spring.kafka.streams.properties.cache.max.bytes.buffering", "0",
                    "spring.kafka.streams.properties.cache.max.bytes.buffering=0 — Streams record cache disabled. Every state-store update produces an immediate changelog write and an immediate downstream record (potential 100× broker write-rate explosion on hot-key workloads). Default 10 MB; either remove or use statestore.cache.max.bytes (KIP-770).",
                    "org.springframework.kafka", "spring-kafka"));
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_CACHE_DISABLED, sev.get(RuleId.SPRING_BOOT_STREAMS_CACHE_DISABLED),
                    "spring.kafka.streams.properties.statestore.cache.max.bytes", "0",
                    "spring.kafka.streams.properties.statestore.cache.max.bytes=0 — Streams record cache disabled (KIP-770 replacement key for cache.max.bytes.buffering). Every state-store update bypasses the cache and produces an immediate changelog write. Set to a non-zero value (default 10 MB).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_TWO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_TWO, sev.get(RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_TWO),
                    "spring.kafka.streams.replication-factor", "2",
                    "spring.kafka.streams.replication-factor=2 — one follower only. Any rolling restart leaves changelogs single-replicated; min.insync.replicas=2 then blocks produces. Use 3 (with min.insync.replicas=2) for real fault-tolerance.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_DEFAULT_DSL_STORE_INMEMORY) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_DEFAULT_DSL_STORE_INMEMORY, sev.get(RuleId.SPRING_BOOT_STREAMS_DEFAULT_DSL_STORE_INMEMORY),
                    "spring.kafka.streams.properties.default.dsl.store", "in_memory",
                    "spring.kafka.streams.properties.default.dsl.store=in_memory — every DSL operator (joins, aggregations, windowed stores) materialises on-heap. Restore-from-changelog after a restart replays the whole changelog into RAM; OOM under non-trivial state. Use RocksDB (default) unless state is provably tiny and bounded.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_LOG_AND_CONTINUE) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_LOG_AND_CONTINUE,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_LOG_AND_CONTINUE),
                    "spring.kafka.streams.properties.default.deserialization.exception.handler",
                    v -> v != null && v.contains("LogAndContinueExceptionHandler"),
                    "spring.kafka.streams.properties.default.deserialization.exception.handler=LogAndContinueExceptionHandler — poison records are dropped with a log line and the topology keeps going. Downstream aggregations are silently incomplete; you only notice when business numbers drift. Route bad records to a DLQ instead (custom handler or LogAndFailExceptionHandler + retry topic).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_COMMIT_INTERVAL_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_COMMIT_INTERVAL_TOO_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_COMMIT_INTERVAL_TOO_HIGH),
                    "spring.kafka.streams.properties.commit.interval.ms",
                    v -> parseLongOrZero(v) > 60_000L,
                    "spring.kafka.streams.properties.commit.interval.ms above 60 s — long commit windows widen the worst-case re-processing window on crash and lengthen end-to-end latency for downstream consumers waiting on commits. Keep commit.interval.ms ≤ 30 s (default 30 s for at-least-once, 100 ms for EOS).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_PRODUCTION_EXCEPTION_HANDLER_ALWAYS_CONTINUE) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_PRODUCTION_EXCEPTION_HANDLER_ALWAYS_CONTINUE,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_PRODUCTION_EXCEPTION_HANDLER_ALWAYS_CONTINUE),
                    "spring.kafka.streams.properties.default.production.exception.handler",
                    v -> v != null && v.contains("AlwaysContinueProductionExceptionHandler"),
                    "spring.kafka.streams.properties.default.production.exception.handler=AlwaysContinueProductionExceptionHandler — every failed produce (downstream, changelog, repartition) is silently dropped. Changelog drops silently corrupt state stores. Use DefaultProductionExceptionHandler (fail fast) or a selective custom handler.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_LOG_AND_SKIP) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_LOG_AND_SKIP,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_LOG_AND_SKIP),
                    "spring.kafka.streams.properties.default.timestamp.extractor",
                    v -> v != null && v.contains("LogAndSkipOnInvalidTimestamp"),
                    "spring.kafka.streams.properties.default.timestamp.extractor=LogAndSkipOnInvalidTimestamp — bad-timestamp records are dropped silently from every aggregation with only a WARN log line. Use FailOnInvalidTimestamp (default) or a custom extractor that routes to a DLT.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_PROCESSING_GUARANTEE_AT_LEAST_ONCE_EXPLICIT) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_PROCESSING_GUARANTEE_AT_LEAST_ONCE_EXPLICIT,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_PROCESSING_GUARANTEE_AT_LEAST_ONCE_EXPLICIT),
                    "spring.kafka.streams.properties.processing.guarantee", "at_least_once",
                    "spring.kafka.streams.properties.processing.guarantee=at_least_once — explicitly setting the Streams default is a smell that often hides a downgrade from EOS. On stateful topologies use exactly_once_v2; on stateless, remove the line.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_WALL_CLOCK) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_WALL_CLOCK,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_WALL_CLOCK),
                    "spring.kafka.streams.properties.default.timestamp.extractor",
                    v -> v != null && v.contains("WallclockTimestampExtractor"),
                    "spring.kafka.streams.properties.default.timestamp.extractor=WallclockTimestampExtractor — every windowed aggregation, every join window, every time-based operator silently uses ingestion wall clock instead of event time. Use FailOnInvalidTimestamp (default).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_GLOBAL_CONSUMER_AUTO_OFFSET_RESET_LATEST) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_GLOBAL_CONSUMER_AUTO_OFFSET_RESET_LATEST,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_GLOBAL_CONSUMER_AUTO_OFFSET_RESET_LATEST),
                    "spring.kafka.streams.properties.global.consumer.auto.offset.reset", "latest",
                    "spring.kafka.streams.properties.global.consumer.auto.offset.reset=latest — the GlobalKTable bootstrap consumer skips the entire topic, leaving the global store permanently incomplete. Joins return null for any pre-startup key. Remove the override.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_RESTORE_CONSUMER_AUTO_OFFSET_RESET_LATEST) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_RESTORE_CONSUMER_AUTO_OFFSET_RESET_LATEST,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_RESTORE_CONSUMER_AUTO_OFFSET_RESET_LATEST),
                    "spring.kafka.streams.properties.restore.consumer.auto.offset.reset", "latest",
                    "spring.kafka.streams.properties.restore.consumer.auto.offset.reset=latest — the restore consumer skips the changelog replay, so a new task starts with empty state and silently produces wrong aggregation/join results until every key is re-populated. Remove the override.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_MAX_TASK_IDLE_MS_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_MAX_TASK_IDLE_MS_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_MAX_TASK_IDLE_MS_HIGH),
                    "spring.kafka.streams.properties.max.task.idle.ms",
                    v -> parseLongOrZero(v) > 30_000L,
                    "spring.kafka.streams.properties.max.task.idle.ms above 30 s — topology stalls waiting on quiet partitions, inflating end-to-end latency. Default 0 is the right starting point.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_TASK_TIMEOUT_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_TASK_TIMEOUT_MS_TOO_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_TASK_TIMEOUT_MS_TOO_HIGH),
                    "spring.kafka.streams.properties.task.timeout.ms",
                    v -> parseLongOrZero(v) > 1_800_000L,
                    "spring.kafka.streams.properties.task.timeout.ms above 30 minutes — stuck tasks swallow broker errors silently instead of failing fast and triggering recovery.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_NUM_STREAM_THREADS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_NUM_STREAM_THREADS_TOO_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_NUM_STREAM_THREADS_TOO_HIGH),
                    "spring.kafka.streams.properties.num.stream.threads",
                    v -> parseLongOrZero(v) > 64L,
                    "spring.kafka.streams.properties.num.stream.threads above 64 — threads beyond the assignable task count sit idle, claiming heap and metric overhead.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_TOO_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_TOO_HIGH),
                    "spring.kafka.streams.replication-factor",
                    v -> parseLongOrZero(v) > 5L,
                    "spring.kafka.streams.replication-factor above 5 — internal-topic disk and follower-fetch bandwidth scale linearly; RF=3 already survives any single AZ outage.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_APPLICATION_SERVER_LOCALHOST) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_APPLICATION_SERVER_LOCALHOST,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_APPLICATION_SERVER_LOCALHOST),
                    "spring.kafka.streams.properties.application.server",
                    v -> {
                        if (v == null) return false;
                        String host = v.trim().toLowerCase();
                        int colon = host.indexOf(':');
                        if (colon > 0) host = host.substring(0, colon);
                        return KafkaTypes.LOCALHOST_HOST_TOKENS.contains(host);
                    },
                    "spring.kafka.streams.properties.application.server points at loopback — interactive queries from peer instances dial their own loopback. Resolve the advertised hostname at startup (k8s downward API / InetAddress.getLocalHost()).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_NONE) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_NONE),
                    "spring.kafka.streams.properties.rack.aware.assignment.strategy", "none",
                    "spring.kafka.streams.properties.rack.aware.assignment.strategy=none — disables rack-aware task assignment. Active+standby may colocate in one AZ, defeating cross-AZ failover. Default min_traffic is correct.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_ACCEPTABLE_RECOVERY_LAG_ZERO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_ACCEPTABLE_RECOVERY_LAG_ZERO,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_ACCEPTABLE_RECOVERY_LAG_ZERO),
                    "spring.kafka.streams.properties.acceptable.recovery.lag", "0",
                    "spring.kafka.streams.properties.acceptable.recovery.lag=0 — a warm standby is only considered \"caught up\" when its lag is exactly zero, which is essentially never under live traffic. Rebalances will refuse to promote standbys and instead replay the changelog on the active node, multiplying downtime. Default 10000 records is the right starting point.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_MAX_WARMUP_REPLICAS_ZERO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_MAX_WARMUP_REPLICAS_ZERO,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_MAX_WARMUP_REPLICAS_ZERO),
                    "spring.kafka.streams.properties.max.warmup.replicas", "0",
                    "spring.kafka.streams.properties.max.warmup.replicas=0 — disables the high-availability task assignor's warmup phase. Scale-out and rolling restarts will move active tasks immediately to cold instances and replay the changelog inline, blocking processing until restore finishes. Default 2 is the right starting point.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_NUM_STANDBY_REPLICAS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_NUM_STANDBY_REPLICAS_TOO_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_NUM_STANDBY_REPLICAS_TOO_HIGH),
                    "spring.kafka.streams.properties.num.standby.replicas",
                    v -> parseLongOrZero(v) > 3L,
                    "spring.kafka.streams.properties.num.standby.replicas above 3 — every standby maintains a full hot replica of every state store. Changelog write amplification and disk usage scale linearly; broker fetch traffic balloons. Above 3 the marginal failover gain is dwarfed by steady-state cost.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_LOW,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_LOW),
                    "spring.kafka.streams.properties.probing.rebalance.interval.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 60_000L; },
                    "spring.kafka.streams.properties.probing.rebalance.interval.ms below 60 s — every probe is a group-wide cooperative rebalance. Topology spends more time rebalancing than processing. Default 600000 (10 min) is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_HIGH),
                    "spring.kafka.streams.properties.probing.rebalance.interval.ms",
                    v -> parseLongOrZero(v) > 7_200_000L,
                    "spring.kafka.streams.properties.probing.rebalance.interval.ms above 2 h — Streams won't check whether warm-up standbys are caught up for that long. Scale-out and failover wait the full interval; num.standby.replicas/max.warmup.replicas capacity sits idle. Default 600000 (10 min) is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_HIGH),
                    "spring.kafka.streams.properties.repartition.purge.interval.ms",
                    v -> parseLongOrZero(v) > 300_000L,
                    "spring.kafka.streams.properties.repartition.purge.interval.ms above 5 min — repartition-topic records sit on broker disk for the full interval after they're consumed; tens of GB of avoidable disk on busy topologies. Default 30000 (30 s) is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_LOW,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_REPARTITION_PURGE_INTERVAL_MS_TOO_LOW),
                    "spring.kafka.streams.properties.repartition.purge.interval.ms",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 5_000L; },
                    "spring.kafka.streams.properties.repartition.purge.interval.ms below 5 s — DeleteRecords admin RPCs hammer the controller queue and the admin-client inflight slots, contending with topic create/delete and leader election. Default 30000 (30 s) is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_BROKER_DEFAULT) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_BROKER_DEFAULT,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_REPLICATION_FACTOR_BROKER_DEFAULT),
                    "spring.kafka.streams.replication-factor", "-1",
                    "spring.kafka.streams.replication-factor=-1 — defers to broker default.replication.factor, which is 1 on every managed Kafka platform's default. Internal changelog/repartition topics silently end up at RF=1 on prod. Set an explicit positive value (3 is the canonical answer).",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_WINDOWSTORE_CHANGELOG_ADDITIONAL_RETENTION_MS_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_WINDOWSTORE_CHANGELOG_ADDITIONAL_RETENTION_MS_TOO_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_WINDOWSTORE_CHANGELOG_ADDITIONAL_RETENTION_MS_TOO_HIGH),
                    "spring.kafka.streams.properties.windowstore.changelog.additional.retention.ms",
                    v -> parseLongOrZero(v) > 604_800_000L,
                    "spring.kafka.streams.properties.windowstore.changelog.additional.retention.ms above 7 days — changelog-side safety buffer for windowed-store changelogs; bloats internal-topic disk and multiplies restore time after rebalances. Default 86400000 (24 h) is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_HIGH) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_HIGH,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_HIGH),
                    "spring.kafka.streams.properties.buffered.records.per.partition",
                    v -> parseLongOrZero(v) > 100_000L,
                    "spring.kafka.streams.properties.buffered.records.per.partition above 100k — removes the back-pressure ceiling; one skewed partition can OOM the JVM. Default 1000 is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_LOW) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SPRING_BOOT_STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_LOW,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_LOW),
                    "spring.kafka.streams.properties.buffered.records.per.partition",
                    v -> { long n = parseLongOrZero(v); return n > 0 && n < 100L; },
                    "spring.kafka.streams.properties.buffered.records.per.partition below 100 — task pauses partitions almost immediately; pause/resume churn dominates the processing loop. Default 1000 is right.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SPRING_BOOT_STREAMS_STATESTORE_CACHE_MAX_BYTES_ZERO) != Severity.OFF) {
            rules.add(PropertyFileRule.literal(
                    RuleId.SPRING_BOOT_STREAMS_STATESTORE_CACHE_MAX_BYTES_ZERO,
                    sev.get(RuleId.SPRING_BOOT_STREAMS_STATESTORE_CACHE_MAX_BYTES_ZERO),
                    "spring.kafka.streams.properties.statestore.cache.max.bytes", "0",
                    "spring.kafka.streams.properties.statestore.cache.max.bytes=0 — Streams state-store cache (KIP-770) disabled. Every put() flushes to RocksDB, the changelog and downstream; 10-100× write amplification on aggregations/KTables. Default 10485760 (10 MiB) is right.",
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
        if (sev.get(RuleId.SECURITY_SSL_ENABLED_PROTOCOLS_LEGACY) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SECURITY_SSL_ENABLED_PROTOCOLS_LEGACY, sev.get(RuleId.SECURITY_SSL_ENABLED_PROTOCOLS_LEGACY),
                    "spring.kafka.properties.ssl.enabled.protocols",
                    KafkaLinterMojo::sslEnabledProtocolsContainsLegacy,
                    "spring.kafka.properties.ssl.enabled.protocols={value} — legacy TLS version in the accept list. Pin to TLSv1.2,TLSv1.3 or leave unset.",
                    "org.springframework.kafka", "spring-kafka"));
            rules.add(SmallRyeChannelConfigRule.predicate(
                    RuleId.SECURITY_SSL_ENABLED_PROTOCOLS_LEGACY, sev.get(RuleId.SECURITY_SSL_ENABLED_PROTOCOLS_LEGACY),
                    null, "ssl.enabled.protocols",
                    KafkaLinterMojo::sslEnabledProtocolsContainsLegacy,
                    "mp.messaging.{direction}.{channel}.ssl.enabled.protocols={value} — legacy TLS version in the accept list. Pin to TLSv1.2,TLSv1.3 or leave unset."));
        }
        if (sev.get(RuleId.SECURITY_SSL_CIPHER_SUITES_LEGACY) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SECURITY_SSL_CIPHER_SUITES_LEGACY, sev.get(RuleId.SECURITY_SSL_CIPHER_SUITES_LEGACY),
                    "spring.kafka.properties.ssl.cipher.suites",
                    KafkaLinterMojo::sslCipherSuitesContainsLegacy,
                    "spring.kafka.properties.ssl.cipher.suites={value} — weak cipher in the list (RC4/MD5/DES/3DES/NULL/EXPORT/anon). Leave unset.",
                    "org.springframework.kafka", "spring-kafka"));
            rules.add(SmallRyeChannelConfigRule.predicate(
                    RuleId.SECURITY_SSL_CIPHER_SUITES_LEGACY, sev.get(RuleId.SECURITY_SSL_CIPHER_SUITES_LEGACY),
                    null, "ssl.cipher.suites",
                    KafkaLinterMojo::sslCipherSuitesContainsLegacy,
                    "mp.messaging.{direction}.{channel}.ssl.cipher.suites={value} — weak cipher in the list (RC4/MD5/DES/3DES/NULL/EXPORT/anon). Leave unset."));
        }
        if (sev.get(RuleId.CRED_SR_BEARER_AUTH_TOKEN_LITERAL) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.CRED_SR_BEARER_AUTH_TOKEN_LITERAL, sev.get(RuleId.CRED_SR_BEARER_AUTH_TOKEN_LITERAL),
                    "spring.kafka.properties.bearer.auth.token",
                    KafkaLinterMojo::isLiteralCredential,
                    "spring.kafka.properties.bearer.auth.token={value} — Schema Registry bearer token in source/config. Inject via ${ENV_VAR}.",
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
        if (sev.get(RuleId.SR_VALUE_SUBJECT_NAME_STRATEGY_NON_DEFAULT) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SR_VALUE_SUBJECT_NAME_STRATEGY_NON_DEFAULT, sev.get(RuleId.SR_VALUE_SUBJECT_NAME_STRATEGY_NON_DEFAULT),
                    "spring.kafka.properties.value.subject.name.strategy",
                    v -> v != null && KafkaTypes.SR_NON_DEFAULT_SUBJECT_NAME_STRATEGY_FQCNS.stream().anyMatch(v.trim()::equals),
                    "spring.kafka.properties.value.subject.name.strategy={value} — non-default Schema Registry subject strategy. Compatibility levels set on <topic>-value no longer apply; CI/governance tooling that indexes by topic-name silently misses breaking changes. Default TopicNameStrategy.",
                    "org.springframework.kafka", "spring-kafka"));
        }
        if (sev.get(RuleId.SR_KEY_SUBJECT_NAME_STRATEGY_NON_DEFAULT) != Severity.OFF) {
            rules.add(PropertyFileRule.predicate(
                    RuleId.SR_KEY_SUBJECT_NAME_STRATEGY_NON_DEFAULT, sev.get(RuleId.SR_KEY_SUBJECT_NAME_STRATEGY_NON_DEFAULT),
                    "spring.kafka.properties.key.subject.name.strategy",
                    v -> v != null && KafkaTypes.SR_NON_DEFAULT_SUBJECT_NAME_STRATEGY_FQCNS.stream().anyMatch(v.trim()::equals),
                    "spring.kafka.properties.key.subject.name.strategy={value} — non-default Schema Registry subject strategy. Shared Avro key types across topics collide under one registry subject; the topic's compatibility level is bypassed. Default TopicNameStrategy.",
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

    /**
     * Parse a Spring Boot Duration string to milliseconds. Accepts: bare digits (ms — Spring's
     * default unit for Kafka duration properties via @DurationUnit(ChronoUnit.MILLIS)), the simple
     * format used by DurationStyle.SIMPLE ("30s", "30000ms", "1m", "1h", "2d"), and ISO-8601 strings
     * ("PT30S", "PT1H"). Returns 0 on any parse failure so callers' numeric predicates evaluate false.
     */
    private static long parseSpringDurationMs(String v) {
        if (v == null) return 0L;
        String t = v.trim();
        if (t.isEmpty()) return 0L;
        if (t.matches("\\d+")) {
            try { return Long.parseLong(t); } catch (NumberFormatException e) { return 0L; }
        }
        String upper = t.toUpperCase(java.util.Locale.ROOT);
        if (upper.startsWith("PT") || (upper.startsWith("P") && upper.indexOf('D') >= 0)) {
            try { return java.time.Duration.parse(upper).toMillis(); } catch (Exception e) { /* fall through */ }
        }
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("^(\\d+)\\s*(ns|us|ms|s|m|h|d)$")
                .matcher(t.toLowerCase(java.util.Locale.ROOT));
        if (m.matches()) {
            long n; try { n = Long.parseLong(m.group(1)); } catch (NumberFormatException e) { return 0L; }
            switch (m.group(2)) {
                case "ns": return n / 1_000_000L;
                case "us": return n / 1_000L;
                case "ms": return n;
                case "s":  return n * 1_000L;
                case "m":  return n * 60_000L;
                case "h":  return n * 3_600_000L;
                case "d":  return n * 86_400_000L;
            }
        }
        return 0L;
    }

    /**
     * Parse a Spring Boot DataSize string to bytes. Accepts: bare digits (bytes — Spring's default
     * unit for Kafka DataSize properties), and the standard DataSize suffixes ("1B", "1KB", "1MB",
     * "1GB", "1TB") with case-insensitive matching. Spring DataSize uses binary scaling (KB = 1024 B,
     * MB = 1024 KB, …). Returns 0 on any parse failure so callers' numeric predicates evaluate false.
     */
    private static long parseSpringDataSizeBytes(String v) {
        if (v == null) return 0L;
        String t = v.trim();
        if (t.isEmpty()) return 0L;
        if (t.matches("\\d+")) {
            try { return Long.parseLong(t); } catch (NumberFormatException e) { return 0L; }
        }
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("^(\\d+)\\s*(b|kb|mb|gb|tb)$")
                .matcher(t.toLowerCase(java.util.Locale.ROOT));
        if (m.matches()) {
            long n; try { n = Long.parseLong(m.group(1)); } catch (NumberFormatException e) { return 0L; }
            switch (m.group(2)) {
                case "b":  return n;
                case "kb": return n * 1024L;
                case "mb": return n * 1024L * 1024L;
                case "gb": return n * 1024L * 1024L * 1024L;
                case "tb": return n * 1024L * 1024L * 1024L * 1024L;
            }
        }
        return 0L;
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

    /** True when ssl.enabled.protocols (comma-separated) lists any legacy TLS/SSL version. */
    private static boolean sslEnabledProtocolsContainsLegacy(String v) {
        if (v == null) return false;
        for (String token : v.split(",")) {
            if (KafkaTypes.SSL_PROTOCOL_LEGACY_VALUES.contains(token.trim())) return true;
        }
        return false;
    }

    /** True when ssl.cipher.suites (comma-separated) references a known-weak cipher family. */
    private static boolean sslCipherSuitesContainsLegacy(String v) {
        if (v == null) return false;
        String upper = v.toUpperCase(java.util.Locale.ROOT);
        for (String token : KafkaTypes.SSL_CIPHER_SUITES_LEGACY_TOKENS) {
            if (containsAsWord(upper, token)) return true;
        }
        return false;
    }

    /** Word-boundary contains: needle must be surrounded by non-alphanumerics (so "RC4" matches in "TLS_RSA_WITH_RC4_128_SHA" but "ANON" doesn't match "CANON"). */
    private static boolean containsAsWord(String haystack, String needle) {
        int from = 0;
        while (true) {
            int i = haystack.indexOf(needle, from);
            if (i < 0) return false;
            boolean leftOk = (i == 0) || !Character.isLetterOrDigit(haystack.charAt(i - 1));
            int end = i + needle.length();
            boolean rightOk = (end == haystack.length()) || !Character.isLetterOrDigit(haystack.charAt(end));
            if (leftOk && rightOk) return true;
            from = i + 1;
        }
    }
}
