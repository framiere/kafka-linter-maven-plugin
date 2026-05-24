package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Flags an admin-lifecycle call (createTopics/deleteTopics/describeCluster/...) made
 * on a local slot AFTER {@code close()} was called on the same slot in the same method.
 *
 * <p>Direct mirror of {@link ProducerUsedAfterCloseRule} and
 * {@link ConsumerUsedAfterCloseRule}: same detection state machine, same backward
 * stack-effect simulation for receiver resolution, same method-reference capture
 * handling via {@code INVOKEDYNAMIC}. The structural difference is the construction
 * shape — {@code AdminClient} has no public constructor; the canonical (and only
 * documented) construction is the static factory {@code Admin.create(props)} or
 * {@code AdminClient.create(props)}. The rule therefore looks for
 * {@code INVOKESTATIC} on {@code Admin}/{@code AdminClient} with name {@code create}
 * followed by {@code ASTORE N} to register the slot.
 *
 * <p>Unlike the consumer, there is no equivalent of {@code wakeup()} for the admin —
 * every documented method is unsafe after close — so the lifecycle set is the full
 * curated admin API surface, with no excluded methods.
 */
public final class AdminUsedAfterCloseRule implements Rule {

    private static final Set<String> LIFECYCLE_METHODS = Set.of(
            // topic lifecycle
            "createTopics", "deleteTopics", "listTopics", "describeTopics",
            "createPartitions",
            // ACL lifecycle
            "createAcls", "deleteAcls", "describeAcls",
            // config lifecycle
            "describeConfigs", "alterConfigs", "incrementalAlterConfigs",
            // log-dir / replica
            "alterReplicaLogDirs", "describeLogDirs",
            // cluster
            "describeCluster", "describeFeatures", "updateFeatures",
            // consumer-group admin
            "listConsumerGroups", "describeConsumerGroups", "deleteConsumerGroups",
            "listConsumerGroupOffsets", "alterConsumerGroupOffsets", "deleteConsumerGroupOffsets",
            "removeMembersFromConsumerGroup",
            // offsets
            "listOffsets",
            // quotas
            "describeClientQuotas", "alterClientQuotas",
            // SCRAM
            "describeUserScramCredentials", "alterUserScramCredentials",
            // leader election / reassignment
            "electLeaders", "alterPartitionReassignments", "listPartitionReassignments",
            // delegation tokens
            "createDelegationToken", "renewDelegationToken", "expireDelegationToken", "describeDelegationToken",
            // transactions / producers
            "listTransactions", "describeTransactions", "abortTransaction", "fenceProducers", "describeProducers",
            // misc
            "unregisterBroker", "describeMetadataQuorum",
            "metrics");

    private final Severity severity;

    public AdminUsedAfterCloseRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_USED_AFTER_CLOSE;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            scanMethod(out, ctx, mn);
        }
        return out;
    }

    private void scanMethod(List<Violation> out, RuleContext ctx, MethodNode mn) {
        Set<Integer> adminSlots = new HashSet<>();
        Set<Integer> closedSlots = new HashSet<>();

        for (AbstractInsnNode insn : mn.instructions) {
            // (1) Track AdminClient constructions: static factory Admin.create(...) / AdminClient.create(...)
            //     followed by ASTORE N. KafkaAdminClient's constructor is package-private — the static
            //     factory is the only documented construction path.
            if (insn instanceof MethodInsnNode factory
                    && factory.getOpcode() == Opcodes.INVOKESTATIC
                    && "create".equals(factory.name)
                    && KafkaTypes.ADMIN_OWNERS.contains(factory.owner)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(factory);
                if (next instanceof VarInsnNode v && v.getOpcode() == Opcodes.ASTORE) {
                    adminSlots.add(v.var);
                }
                continue;
            }

            // (2) Method-reference capture of an admin lifecycle method, e.g.
            //     executor.submit(admin::listTopics)  →  INVOKEDYNAMIC whose bsmArgs
            //     contain a REF_invokeVirtual/REF_invokeInterface Admin.listTopics(...)
            //     handle. Treat it as a use of the captured slot: if the slot is
            //     already closed, the deferred call will throw IllegalStateException
            //     when the captured functional interface is invoked on an executor /
            //     async thread.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                Handle h = AsmUtil.indyTargetHandle(indy, KafkaTypes.ADMIN_OWNERS, null, null);
                if (h == null) continue;
                if (!LIFECYCLE_METHODS.contains(h.getName())) continue;
                Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, adminSlots);
                for (Integer slot : captured) {
                    if (closedSlots.contains(slot)) {
                        out.add(new Violation(
                                RuleId.ADMIN_USED_AFTER_CLOSE, severity,
                                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                                h.getName() + "(...) reference captured on an AdminClient that was already "
                                        + "closed earlier in this method. close() is one-way — when the deferred "
                                        + "invocation runs (typically on an executor or async thread) it throws "
                                        + "IllegalStateException; in an async hand-off the exception is silently "
                                        + "swallowed and the cluster mutation never happens. Move close() to the "
                                        + "very last call on the admin."));
                        break;
                    }
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.ADMIN_OWNERS.contains(mi.owner)) continue;

            if (mi.name.startsWith("close")) {
                Integer slot = AsmUtil.resolveReceiverSlot(mi, adminSlots);
                if (slot != null) closedSlots.add(slot);
                continue;
            }

            if (!LIFECYCLE_METHODS.contains(mi.name)) continue;
            Integer slot = AsmUtil.resolveReceiverSlot(mi, adminSlots);
            if (slot == null || !closedSlots.contains(slot)) continue;

            out.add(new Violation(
                    RuleId.ADMIN_USED_AFTER_CLOSE, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                    mi.name + "(...) called on an AdminClient that was already closed earlier "
                            + "in this method. close() is one-way — any subsequent call throws "
                            + "IllegalStateException; in an async hand-off the exception is silently "
                            + "swallowed and the cluster mutation never happens. Move close() to the "
                            + "very last call on the admin."));
        }
    }
}
