package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of the {@link
 * org.apache.kafka.streams.kstream.BranchedKStream#branch(
 * org.apache.kafka.streams.kstream.Predicate)
 * BranchedKStream.branch(Predicate)} overload — the only branch
 * overload that does NOT take a {@link
 * org.apache.kafka.streams.kstream.Branched Branched} argument
 * and therefore leaves the branch sub-graph auto-named from the
 * topology graph index.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls
 * (BranchedKStream is an interface) and {@code INVOKEDYNAMIC}
 * method-reference captures (e.g. {@code branched::branch} bound
 * to a SAM whose erased implMethod descriptor matches the unsafe
 * overload).
 *
 * <h2>Why no-Branched branch is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#split
 * KStream.split()} returns a {@link
 * org.apache.kafka.streams.kstream.BranchedKStream} on which the
 * caller chains {@code branch(Predicate)} for each branch and
 * finally {@code defaultBranch()} or {@code noDefaultBranch()}
 * to materialise a {@code Map<String, KStream<K, V>>}. Each
 * branch in the returned map is keyed by the BRANCH NAME.
 *
 * <p>Without an explicit {@link
 * org.apache.kafka.streams.kstream.Branched#as(String)} (or
 * {@link
 * org.apache.kafka.streams.kstream.Branched#withFunction
 * withFunction(name, ...)} / {@link
 * org.apache.kafka.streams.kstream.Branched#withConsumer
 * withConsumer(name, ...)} variants), Streams synthesises a
 * branch name of the form {@code X-PREDICATE-N} from the
 * topology graph index — e.g. {@code KSTREAM-SPLIT-
 * 0000000003-PREDICATE-0}, {@code KSTREAM-SPLIT-0000000003-
 * PREDICATE-1}, etc. The returned {@code Map<String, KStream>}
 * uses those auto-names as keys.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>{@code branches.get("X-PREDICATE-N")} returns null
 *       after any topology edit.</b> A team has {@code
 *       stream.split().branch(isHighValue).branch(isLowValue)
 *       .defaultBranch()} producing a Map of 3 branches keyed
 *       {@code KSTREAM-SPLIT-0000000003-PREDICATE-0},
 *       {@code KSTREAM-SPLIT-0000000003-PREDICATE-1},
 *       {@code KSTREAM-SPLIT-0000000003-PREDICATE-2}. Downstream
 *       code does {@code branches.get("KSTREAM-SPLIT-0000000003-
 *       PREDICATE-0").to("high-value-topic")} hard-coded against
 *       the auto-name. Team adds one upstream filter; the graph
 *       index shifts; the new split node is now {@code KSTREAM-
 *       SPLIT-0000000005}; the branch keys are now {@code
 *       KSTREAM-SPLIT-0000000005-PREDICATE-0/1/2}; {@code
 *       branches.get("KSTREAM-SPLIT-0000000003-PREDICATE-0")}
 *       returns null at runtime; {@code .to("high-value-topic")}
 *       on null throws {@code NullPointerException} on startup;
 *       the entire Streams app fails to assemble its topology
 *       and crashes on first record. If the get is wrapped in a
 *       null check (defensive), the high-value branch is
 *       silently dropped and never reaches the downstream
 *       topic.</li>
 *   <li><b>Branch-tagged metric panels silently empty after a
 *       topology edit.</b> Kafka Streams emits per-processor-
 *       node process-rate, dropped-records-rate, and
 *       processing-latency metrics tagged with the node id —
 *       the branch sub-graph uses the auto-name as its node id
 *       suffix. Grafana panels filtered on {@code node-
 *       id="KSTREAM-SPLIT-0000000003-PREDICATE-0"} read zero
 *       after any topology edit; oncall stops looking at those
 *       panels because they look broken; a real production
 *       collapse on the high-value branch two weeks later is
 *       invisible until a customer complaint.</li>
 *   <li><b>{@code topology.describe()} runbook drift.</b> SRE
 *       runbooks reference branches by their auto-generated
 *       names — {@code "if KSTREAM-SPLIT-0000000003-PREDICATE-0
 *       processing-latency exceeds 500ms, scale out the
 *       high-value consumer group"}. After any topology edit
 *       the referenced branch no longer exists by that name;
 *       the runbook step silently no-ops; the operator believes
 *       they have scaled the high-value branch but the wrong
 *       branch (or no branch at all) was targeted.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       branch factory built as {@code Function&lt;Predicate&lt;
 *       String, String&gt;, BranchedKStream&lt;String, String&gt;
 *       &gt; factory = branched::branch} compiles to {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle pointing at {@code
 *       BranchedKStream.branch(Predicate)BranchedKStream}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Branched overload, only the
 *       indy site.</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the single unsafe overload
 * descriptor {@code (Lorg/apache/kafka/streams/kstream/
 * Predicate;)Lorg/apache/kafka/streams/kstream/BranchedKStream;}.
 * Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Branched#as(String)} —
 * e.g. {@code branched.branch(isHighValue, Branched.as(
 * "high-value"))}. The chosen branch name is stable across
 * topology edits and becomes the key in the returned {@code
 * Map<String, KStream>}, modulo the {@code KSTREAM-SPLIT-N-}
 * prefix Streams adds — so {@code branches.get(splitName +
 * "high-value")} works deterministically.
 */
public final class StreamsBranchedNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.BRANCHED_KSTREAM);
    private static final String METHOD_NAME = "branch";
    private static final String UNSAFE_DESC =
            "(Lorg/apache/kafka/streams/kstream/Predicate;)"
                    + "Lorg/apache/kafka/streams/kstream/BranchedKStream;";

    private final Severity severity;

    public StreamsBranchedNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_BRANCHED_NO_NAMED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && UNSAFE_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_BRANCHED_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "BranchedKStream.branch(Predicate) — the no-Branched "
                        + "overload is reached here — either as a "
                        + "direct INVOKEINTERFACE on the method or as "
                        + "an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `branched::branch` bound to "
                        + "Function<Predicate, BranchedKStream>). "
                        + "KStream.split() returns a BranchedKStream on "
                        + "which the caller chains branch(Predicate) "
                        + "for each branch and finally defaultBranch() "
                        + "or noDefaultBranch() to materialise a "
                        + "Map<String, KStream<K, V>>; each branch in "
                        + "the returned map is keyed by the BRANCH "
                        + "NAME; without Branched.as(\"...\") Streams "
                        + "synthesises a branch name like `KSTREAM-"
                        + "SPLIT-0000000003-PREDICATE-0` from the "
                        + "topology graph index. Concrete failure "
                        + "modes: (1) branches.get(\"X-PREDICATE-N\") "
                        + "returns null after any topology edit — team "
                        + "has stream.split().branch(isHighValue)."
                        + "branch(isLowValue).defaultBranch() producing "
                        + "a Map of 3 branches keyed `KSTREAM-SPLIT-"
                        + "0000000003-PREDICATE-0/1/2`; downstream "
                        + "code does branches.get(\"KSTREAM-SPLIT-"
                        + "0000000003-PREDICATE-0\").to(\"high-value-"
                        + "topic\") hard-coded against the auto-name; "
                        + "team adds one upstream filter; the graph "
                        + "index shifts; the new split node is now "
                        + "KSTREAM-SPLIT-0000000005; branch keys are "
                        + "now KSTREAM-SPLIT-0000000005-PREDICATE-"
                        + "0/1/2; branches.get(\"KSTREAM-SPLIT-"
                        + "0000000003-PREDICATE-0\") returns null at "
                        + "runtime; .to(\"high-value-topic\") on null "
                        + "throws NullPointerException on startup; the "
                        + "entire Streams app fails to assemble its "
                        + "topology and crashes on first record; if "
                        + "the get is wrapped in a null check "
                        + "(defensive), the high-value branch is "
                        + "silently dropped and never reaches the "
                        + "downstream topic; (2) branch-tagged metric "
                        + "panels silently empty after a topology "
                        + "edit — Kafka Streams emits per-processor-"
                        + "node process-rate, dropped-records-rate, "
                        + "and processing-latency metrics tagged with "
                        + "the node id; the branch sub-graph uses the "
                        + "auto-name as its node id suffix; Grafana "
                        + "panels filtered on node-id=\"KSTREAM-SPLIT-"
                        + "0000000003-PREDICATE-0\" read zero after "
                        + "any topology edit; oncall stops looking at "
                        + "those panels because they look broken; a "
                        + "real production collapse on the high-value "
                        + "branch two weeks later is invisible until a "
                        + "customer complaint; (3) topology.describe() "
                        + "runbook drift — SRE runbooks reference "
                        + "branches by their auto-generated names "
                        + "(\"if KSTREAM-SPLIT-0000000003-PREDICATE-0 "
                        + "processing-latency exceeds 500ms, scale out "
                        + "the high-value consumer group\"); after "
                        + "any topology edit the referenced branch no "
                        + "longer exists by that name; the runbook "
                        + "step silently no-ops; the operator "
                        + "believes they have scaled the high-value "
                        + "branch but the wrong branch (or no branch "
                        + "at all) was targeted; (4) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — Function<"
                        + "Predicate<String, String>, BranchedKStream<"
                        + "String, String>> factory = branched::branch "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle "
                        + "pointing at BranchedKStream.branch("
                        + "Predicate)BranchedKStream; the user-class "
                        + "bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Branched "
                        + "overload, only the indy site. Migration: "
                        + "pass an explicit Branched.as(\"...\") — "
                        + "`branched.branch(isHighValue, Branched.as("
                        + "\"high-value\"))`. The chosen branch name "
                        + "is stable across topology edits and becomes "
                        + "the key in the returned Map<String, "
                        + "KStream>, modulo the KSTREAM-SPLIT-N- "
                        + "prefix Streams adds — so branches.get("
                        + "splitName + \"high-value\") works "
                        + "deterministically. The branch(Predicate, "
                        + "Branched) overload is never flagged by "
                        + "this rule.");
    }
}
