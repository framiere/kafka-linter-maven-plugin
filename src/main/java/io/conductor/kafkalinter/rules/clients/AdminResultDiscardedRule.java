package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Flags a STATE-MUTATING admin operation whose returned {@code *Result} is immediately
 * discarded (the next significant bytecode instruction is {@code POP}).
 *
 * <p>Every state-mutating method on {@link org.apache.kafka.clients.admin.Admin Admin}
 * returns a {@code *Result} wrapping one or more {@code KafkaFuture<T>} handles for the
 * in-flight broker operation(s). The contract is that callers must observe at least one
 * of those futures — typically via {@code .all().get()} — to learn whether the broker
 * accepted the mutation. Discarding the Result with {@code POP} means the AdminClient's
 * background network thread will dispatch the request, the broker will respond (success
 * or structured error), but the response lands in a future that is immediately
 * garbage-collected. Failures (TOPIC_ALREADY_EXISTS, AUTHORIZATION_FAILED,
 * INVALID_REPLICATION_FACTOR, POLICY_VIOLATION, fencing, broker timeout, …) are dropped
 * on the floor and the application proceeds as if the mutation had succeeded.
 *
 * <p>The flagged method set is intentionally narrow: ONLY state-mutating operations
 * (createTopics / deleteTopics / createAcls / alterConfigs / incrementalAlterConfigs /
 * createPartitions / electLeaders / alter* / delete*ConsumerGroup* / etc.). Read-only
 * describe* / list* operations are excluded — discarding their Result wastes a
 * broker round-trip but does not corrupt cluster state, and many legitimate fan-out
 * patterns ('best-effort describe, ignore failures') deliberately discard the result.
 *
 * <p>Detection is structural and unambiguous: javac emits a lone {@code POP} only for
 * an expression-statement that returned a reference. Any legitimate observation pattern
 * — chain (.all().get(), .values().get(name).get(), .whenComplete(...)), store
 * (ASTORE N), return (ARETURN), capture (DUP+ASTORE for try-with-resources synthetics)
 * — emits an INVOKE*, ASTORE, ARETURN, or DUP instead of POP. So a POP-immediately
 * after a state-mutating admin call is, by construction, the fire-and-forget bug.
 */
public final class AdminResultDiscardedRule implements Rule {

    /**
     * State-mutating admin operations. Discarding the Result on any of these is a
     * correctness bug: the broker may reject the mutation and the application has no
     * way to learn that the cluster state diverges from its expectations.
     *
     * <p>Read-only {@code describe*} / {@code list*} methods are deliberately omitted —
     * discarding their Result is wasteful (one round-trip for nothing) but does not
     * corrupt state.
     */
    private static final Set<String> STATE_MUTATING_METHODS = Set.of(
            // topics
            "createTopics", "deleteTopics", "createPartitions",
            // ACLs
            "createAcls", "deleteAcls",
            // configs
            "alterConfigs", "incrementalAlterConfigs",
            // log dirs
            "alterReplicaLogDirs",
            // features
            "updateFeatures",
            // consumer groups
            "deleteConsumerGroups", "alterConsumerGroupOffsets", "deleteConsumerGroupOffsets",
            "removeMembersFromConsumerGroup",
            // leader / reassignment
            "electLeaders", "alterPartitionReassignments",
            // delegation tokens
            "createDelegationToken", "renewDelegationToken", "expireDelegationToken",
            // quotas / SCRAM
            "alterClientQuotas", "alterUserScramCredentials",
            // broker / transactions
            "unregisterBroker", "abortTransaction", "fenceProducers",
            // records
            "deleteRecords");

    private final Severity severity;

    public AdminResultDiscardedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_RESULT_DISCARDED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.ADMIN_OWNERS.contains(mi.owner)) continue;
                if (!STATE_MUTATING_METHODS.contains(mi.name)) continue;

                AbstractInsnNode next = AsmUtil.nextSignificant(insn);
                if (!(next instanceof InsnNode in)) continue;
                int op = in.getOpcode();
                if (op != Opcodes.POP && op != Opcodes.POP2) continue;

                out.add(new Violation(
                        RuleId.ADMIN_RESULT_DISCARDED, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        mi.name + "(...) returned a *Result whose KafkaFuture(s) are immediately discarded "
                                + "(POP after the call). The AdminClient still dispatches the request to the broker, "
                                + "but any failure (topic exists, ACL denied, invalid config, broker timeout, "
                                + "policy violation, fencing) lands in a future that nobody observes — the cluster "
                                + "may reject the mutation and the application proceeds as if it succeeded. "
                                + "Chain .all().get(timeout, unit) (or per-future .get(timeout, unit)) so an "
                                + "ExecutionException surfaces the underlying failure."));
            }
        }
        return out;
    }
}
