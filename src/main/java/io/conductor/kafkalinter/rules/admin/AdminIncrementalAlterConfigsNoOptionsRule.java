package io.conductor.kafkalinter.rules.admin;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of an {@link
 * org.apache.kafka.clients.admin.Admin#incrementalAlterConfigs
 * Admin.incrementalAlterConfigs} overload that does NOT
 * include an {@link
 * org.apache.kafka.clients.admin.AlterConfigsOptions
 * AlterConfigsOptions} argument. The single unsafe overload
 * is the 1-arg {@code incrementalAlterConfigs(Map<
 * ConfigResource, Collection<AlterConfigOp>>)}. The 2-arg
 * Options-bearing overload is the safe form and is
 * intentionally not flagged.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} on {@link
 * org.apache.kafka.clients.admin.Admin Admin}, {@code
 * INVOKEVIRTUAL} on {@link
 * org.apache.kafka.clients.admin.AdminClient AdminClient}, and
 * {@code INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Options {@code incrementalAlterConfigs} is a
 * cross-resource consistency hazard with no transactional
 * rollback</h2>
 *
 * <p>{@link
 * org.apache.kafka.clients.admin.Admin#incrementalAlterConfigs
 * Admin.incrementalAlterConfigs} is the KIP-339 successor to
 * the deprecated {@code alterConfigs} — it accepts SET, APPEND,
 * SUBTRACT, and DELETE operations on broker configs, topic
 * configs, broker logger levels, and (with KIP-554) client-side
 * SCRAM/quota configs. A single call typically targets multiple
 * {@code ConfigResource} entries — for example, "increase {@code
 * retention.ms} on these 12 topics" or "enable {@code
 * log.message.timestamp.difference.max.ms} validation on all 5
 * brokers." The per-resource futures inside the {@link
 * org.apache.kafka.clients.admin.AlterConfigsResult
 * AlterConfigsResult} resolve INDEPENDENTLY and CANNOT be
 * rolled back as a unit.
 *
 * <p>The no-Options overload inherits the default {@code
 * request.timeout.ms} (~30 s). When the future fires {@code
 * TimeoutException} the caller cannot distinguish "all
 * resources applied" from "some resources applied, others
 * timed out" from "no resources applied" — and the resources
 * that DID apply are NOT rolled back. The next time the caller
 * (typically a config-as-code reconciliation loop) queries
 * {@code describeConfigs()}, it sees a state that matches
 * neither the source-of-truth config nor the previous state.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Cluster-wide config drift — half the brokers got
 *       the new config, half did not, the next rolling restart
 *       exposes the inconsistency.</b> An SRE runs {@code
 *       incrementalAlterConfigs} to enable a new feature flag
 *       (e.g. {@code remote.log.storage.system.enable=true}
 *       for KIP-405 tiered storage) on all 12 brokers in one
 *       call. The controller writes the {@code
 *       BrokerConfigRecord} for 7 of the 12 brokers before the
 *       30 s timeout fires AdminClient-side. The SRE script
 *       logs FAILED. The next rolling restart (next night)
 *       happens to restart the 5 brokers that DID NOT get the
 *       config — those 5 brokers come up WITHOUT the feature
 *       flag while the 7 that already restarted have it. The
 *       cluster now has split-brain behavior: some partitions
 *       on tiered-storage-enabled brokers, others not. Several
 *       weeks of subtle data-fetch issues follow before
 *       someone diffs broker-side {@code describeConfigs()}
 *       and spots the inconsistency.</li>
 *   <li><b>Per-topic batch is partial — config-as-code tool
 *       believes it converged and stops reconciling.</b> A
 *       config-as-code tool (Strimzi {@code KafkaTopic},
 *       Confluent JulieOps) calls {@code
 *       incrementalAlterConfigs(map)} with retention/cleanup
 *       policy updates for 200 topics. The controller writes
 *       180 of the 200 records before the timeout fires. The
 *       tool catches the {@code TimeoutException}, computes
 *       "OK, I'll let the next reconcile loop pick it up," and
 *       enters a 5-minute back-off. On the next reconcile it
 *       calls {@code describeConfigs(allTopics)} and finds 180
 *       of 200 topics in their desired state — so it submits
 *       an {@code incrementalAlterConfigs} for the REMAINING
 *       20. But the controller is still processing the
 *       original timeout's queued writes; the 20-topic submit
 *       races against the residual writes; some of the 20
 *       resources race-win and converge, others get an
 *       InvalidConfigException because the resource is locked.
 *       Tool gives up on those topics; they stay drifted for
 *       hours.</li>
 *   <li><b>APPEND/SUBTRACT on a list-valued config is NOT
 *       idempotent.</b> A retention rule pushes APPEND to
 *       {@code follower.replication.throttled.replicas}; the
 *       first attempt timed out; the controller persisted the
 *       APPEND; the retry decorator resubmits the same APPEND;
 *       the controller appends AGAIN, doubling the entry.
 *       Subsequent broker startup parses the malformed list
 *       and rejects the config; brokers refuse to start.
 *       (SET is idempotent; APPEND/SUBTRACT/DELETE are
 *       sensitive to current value.)</li>
 *   <li><b>Broker-logger live-update timeout — caller cannot
 *       tell whether the log level was actually raised.</b> An
 *       incident-response runbook calls {@code
 *       incrementalAlterConfigs(brokerLoggers)} to raise
 *       {@code org.apache.kafka} to DEBUG during a tail-latency
 *       investigation. The default 30 s fires; the operator
 *       does not know whether DEBUG was applied; they retry;
 *       now they have raised log volume across the fleet twice
 *       and risk filling broker disks with DEBUG-level
 *       request-log lines.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       config-update helper built as {@code Function<Map<
 *       ConfigResource, Collection<AlterConfigOp>>,
 *       AlterConfigsResult> apply = admin::
 *       incrementalAlterConfigs} captures an {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle on the 1-arg overload.
 *       The user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Options overload, only the
 *       indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.clients.admin.AlterConfigsOptions
 * AlterConfigsOptions} pinning a per-call deadline — e.g.
 * {@code admin.incrementalAlterConfigs(map, new
 * AlterConfigsOptions().timeoutMs(120_000))}. Use {@code
 * validateOnly(true)} for plan validation before submit; iterate
 * {@code result.values().entrySet()} (per-resource futures)
 * rather than relying on {@code result.all()} so a partial
 * timeout still yields per-resource success/fail visibility;
 * for APPEND/SUBTRACT/DELETE on list-valued configs, ALWAYS
 * pre-fetch {@code describeConfigs()} and compute the
 * post-state explicitly rather than retrying on timeout.
 */
public final class AdminIncrementalAlterConfigsNoOptionsRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.ADMIN_OWNERS;
    private static final String METHOD_NAME = "incrementalAlterConfigs";
    private static final String OPTIONS_TYPE_TOKEN =
            "Lorg/apache/kafka/clients/admin/AlterConfigsOptions;";

    private final Severity severity;

    public AdminIncrementalAlterConfigsNoOptionsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_INCREMENTAL_ALTER_CONFIGS_NO_OPTIONS;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && mi.desc != null
                        && !mi.desc.contains(OPTIONS_TYPE_TOKEN)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
                    if (h != null && h.getDesc() != null && !h.getDesc().contains(OPTIONS_TYPE_TOKEN)) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.ADMIN_INCREMENTAL_ALTER_CONFIGS_NO_OPTIONS, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.incrementalAlterConfigs(Map<ConfigResource, "
                        + "Collection<AlterConfigOp>>) — a "
                        + "no-AlterConfigsOptions overload is reached "
                        + "here, either as a direct INVOKEINTERFACE on "
                        + "Admin / INVOKEVIRTUAL on AdminClient or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`admin::incrementalAlterConfigs` bound to "
                        + "Function<Map, AlterConfigsResult> or to a "
                        + "custom SAM whose erased implMethod descriptor "
                        + "matches the unsafe overload). "
                        + "incrementalAlterConfigs is the KIP-339 "
                        + "successor to the deprecated alterConfigs — it "
                        + "accepts SET, APPEND, SUBTRACT, and DELETE "
                        + "operations on broker configs, topic configs, "
                        + "broker logger levels, and (with KIP-554) "
                        + "client-side SCRAM/quota configs. A single "
                        + "call typically targets multiple ConfigResource "
                        + "entries — for example, increase retention.ms "
                        + "on these 12 topics or enable "
                        + "log.message.timestamp.difference.max.ms "
                        + "validation on all 5 brokers. The per-resource "
                        + "futures inside the AlterConfigsResult resolve "
                        + "INDEPENDENTLY and CANNOT be rolled back as a "
                        + "unit. The no-Options overload inherits the "
                        + "default request.timeout.ms (~30s). When the "
                        + "future fires TimeoutException the caller "
                        + "cannot distinguish all resources applied "
                        + "from some resources applied, others timed "
                        + "out from no resources applied — and the "
                        + "resources that DID apply are NOT rolled back. "
                        + "The next time the caller (typically a "
                        + "config-as-code reconciliation loop) queries "
                        + "describeConfigs(), it sees a state that "
                        + "matches neither the source-of-truth config "
                        + "nor the previous state. Concrete failure "
                        + "modes: (1) cluster-wide config drift — half "
                        + "the brokers got the new config, half did "
                        + "not, the next rolling restart exposes the "
                        + "inconsistency; an SRE runs "
                        + "incrementalAlterConfigs to enable a new "
                        + "feature flag (remote.log.storage.system."
                        + "enable=true for KIP-405 tiered storage) on "
                        + "all 12 brokers in one call; the controller "
                        + "writes the BrokerConfigRecord for 7 of the "
                        + "12 brokers before the 30s timeout fires "
                        + "AdminClient-side; the SRE script logs FAILED; "
                        + "the next rolling restart (next night) happens "
                        + "to restart the 5 brokers that DID NOT get the "
                        + "config — those 5 brokers come up WITHOUT the "
                        + "feature flag while the 7 that already "
                        + "restarted have it; the cluster now has "
                        + "split-brain behavior: some partitions on "
                        + "tiered-storage-enabled brokers, others not; "
                        + "several weeks of subtle data-fetch issues "
                        + "follow before someone diffs broker-side "
                        + "describeConfigs() and spots the "
                        + "inconsistency; (2) per-topic batch is partial "
                        + "— config-as-code tool believes it converged "
                        + "and stops reconciling; a config-as-code tool "
                        + "(Strimzi KafkaTopic, Confluent JulieOps) "
                        + "calls incrementalAlterConfigs(map) with "
                        + "retention/cleanup policy updates for 200 "
                        + "topics; the controller writes 180 of the 200 "
                        + "records before the timeout fires; the tool "
                        + "catches the TimeoutException, computes OK "
                        + "I'll let the next reconcile loop pick it up, "
                        + "and enters a 5-minute back-off; on the next "
                        + "reconcile it calls describeConfigs("
                        + "allTopics) and finds 180 of 200 topics in "
                        + "their desired state — so it submits an "
                        + "incrementalAlterConfigs for the REMAINING 20; "
                        + "but the controller is still processing the "
                        + "original timeout's queued writes; the "
                        + "20-topic submit races against the residual "
                        + "writes; some of the 20 resources race-win "
                        + "and converge, others get an "
                        + "InvalidConfigException because the resource "
                        + "is locked; tool gives up on those topics; "
                        + "they stay drifted for hours; (3) "
                        + "APPEND/SUBTRACT on a list-valued config is "
                        + "NOT idempotent — a retention rule pushes "
                        + "APPEND to follower.replication.throttled."
                        + "replicas; the first attempt timed out; the "
                        + "controller persisted the APPEND; the retry "
                        + "decorator resubmits the same APPEND; the "
                        + "controller appends AGAIN, doubling the "
                        + "entry; subsequent broker startup parses the "
                        + "malformed list and rejects the config; "
                        + "brokers refuse to start (SET is idempotent; "
                        + "APPEND/SUBTRACT/DELETE are sensitive to "
                        + "current value); (4) broker-logger "
                        + "live-update timeout — caller cannot tell "
                        + "whether the log level was actually raised; "
                        + "an incident-response runbook calls "
                        + "incrementalAlterConfigs(brokerLoggers) to "
                        + "raise org.apache.kafka to DEBUG during a "
                        + "tail-latency investigation; the default 30s "
                        + "fires; the operator does not know whether "
                        + "DEBUG was applied; they retry; now they have "
                        + "raised log volume across the fleet twice "
                        + "and risk filling broker disks with "
                        + "DEBUG-level request-log lines; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "Function<Map<ConfigResource, "
                        + "Collection<AlterConfigOp>>, "
                        + "AlterConfigsResult> apply = admin::"
                        + "incrementalAlterConfigs captures an "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle on the 1-arg "
                        + "overload; the user-class bytecode contains "
                        + "zero direct INVOKEINTERFACE on the "
                        + "no-Options overload, only the indy site. "
                        + "Migration: pass an explicit "
                        + "AlterConfigsOptions pinning a per-call "
                        + "deadline — admin.incrementalAlterConfigs("
                        + "map, new AlterConfigsOptions().timeoutMs("
                        + "120_000)). Use validateOnly(true) for plan "
                        + "validation before submit; iterate "
                        + "result.values().entrySet() (per-resource "
                        + "futures) rather than relying on "
                        + "result.all() so a partial timeout still "
                        + "yields per-resource success/fail "
                        + "visibility; for APPEND/SUBTRACT/DELETE on "
                        + "list-valued configs, ALWAYS pre-fetch "
                        + "describeConfigs() and compute the "
                        + "post-state explicitly rather than retrying "
                        + "on timeout.");
    }
}
