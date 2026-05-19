package io.conductor.kafkalinter;

import io.conductor.kafkalinter.rules.ConsumerAutoCommitTrueRule;
import io.conductor.kafkalinter.rules.ConsumerCommitPerRecordRule;
import io.conductor.kafkalinter.rules.ConsumerPollZeroRule;
import io.conductor.kafkalinter.rules.ProducerFlushInLoopRule;
import io.conductor.kafkalinter.rules.ProducerInLoopRule;
import io.conductor.kafkalinter.rules.ProducerNoCompressionRule;
import io.conductor.kafkalinter.rules.ProducerSendBlockingGetRule;
import io.conductor.kafkalinter.rules.ProducerSendNoCallbackRule;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.KafkaTypes;
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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Mojo(name = "check", defaultPhase = LifecyclePhase.VERIFY, threadSafe = true)
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
     * values are ERROR, WARNING, or OFF.
     */
    @Parameter
    private Map<String, String> severities = new HashMap<>();

    @Override
    public void execute() throws MojoFailureException {
        if (skip) {
            getLog().info("kafka-linter: skipped via configuration.");
            return;
        }

        Path classesDir = Paths.get(classesDirectory);
        if (!classesDir.toFile().isDirectory()) {
            getLog().info("kafka-linter: no classes directory at " + classesDir + " — nothing to scan.");
            return;
        }

        Map<RuleId, Severity> resolved = resolveSeverities();
        List<Rule> rules = buildRules(resolved);

        List<Violation> violations;
        try {
            violations = new ProjectScanner(rules).scanDirectory(classesDir);
        } catch (Exception e) {
            throw new MojoFailureException("kafka-linter: failed to scan " + classesDir, e);
        }

        report(violations);

        long errors = violations.stream().filter(v -> v.severity() == Severity.ERROR).count();
        if (errors > 0) {
            throw new MojoFailureException("kafka-linter: " + errors + " error-severity violation(s) found.");
        }
    }

    private Map<RuleId, Severity> resolveSeverities() {
        Map<RuleId, Severity> map = new EnumMap<>(RuleId.class);
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
        if (sev.get(RuleId.PRODUCER_IN_LOOP) != Severity.OFF) {
            rules.add(new ProducerInLoopRule(RuleId.PRODUCER_IN_LOOP, sev.get(RuleId.PRODUCER_IN_LOOP),
                    Set.of(KafkaTypes.KAFKA_PRODUCER)));
        }
        if (sev.get(RuleId.CONSUMER_IN_LOOP) != Severity.OFF) {
            rules.add(new ProducerInLoopRule(RuleId.CONSUMER_IN_LOOP, sev.get(RuleId.CONSUMER_IN_LOOP),
                    Set.of(KafkaTypes.KAFKA_CONSUMER)));
        }
        if (sev.get(RuleId.PRODUCER_NO_COMPRESSION) != Severity.OFF) {
            rules.add(new ProducerNoCompressionRule(sev.get(RuleId.PRODUCER_NO_COMPRESSION)));
        }
        if (sev.get(RuleId.PRODUCER_SEND_BLOCKING_GET) != Severity.OFF) {
            rules.add(new ProducerSendBlockingGetRule(sev.get(RuleId.PRODUCER_SEND_BLOCKING_GET)));
        }
        if (sev.get(RuleId.PRODUCER_SEND_NO_CALLBACK) != Severity.OFF) {
            rules.add(new ProducerSendNoCallbackRule(sev.get(RuleId.PRODUCER_SEND_NO_CALLBACK)));
        }
        if (sev.get(RuleId.PRODUCER_FLUSH_IN_LOOP) != Severity.OFF) {
            rules.add(new ProducerFlushInLoopRule(sev.get(RuleId.PRODUCER_FLUSH_IN_LOOP)));
        }
        if (sev.get(RuleId.CONSUMER_AUTO_COMMIT_TRUE) != Severity.OFF) {
            rules.add(new ConsumerAutoCommitTrueRule(sev.get(RuleId.CONSUMER_AUTO_COMMIT_TRUE)));
        }
        if (sev.get(RuleId.CONSUMER_COMMIT_PER_RECORD) != Severity.OFF) {
            rules.add(new ConsumerCommitPerRecordRule(sev.get(RuleId.CONSUMER_COMMIT_PER_RECORD)));
        }
        if (sev.get(RuleId.CONSUMER_POLL_ZERO) != Severity.OFF) {
            rules.add(new ConsumerPollZeroRule(sev.get(RuleId.CONSUMER_POLL_ZERO)));
        }
        return rules;
    }

    private void report(List<Violation> violations) {
        if (violations.isEmpty()) {
            getLog().info("kafka-linter: 0 violations.");
            return;
        }
        long errors = violations.stream().filter(v -> v.severity() == Severity.ERROR).count();
        long warnings = violations.stream().filter(v -> v.severity() == Severity.WARNING).count();
        getLog().info("kafka-linter: " + violations.size() + " violation(s) — " + errors + " error, " + warnings + " warning.");
        for (Violation v : violations) {
            String line = v.format();
            if (v.severity() == Severity.ERROR) {
                getLog().error(line);
            } else {
                getLog().warn(line);
            }
        }
    }
}
