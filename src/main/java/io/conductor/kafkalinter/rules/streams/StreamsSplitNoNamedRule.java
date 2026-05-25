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
 * org.apache.kafka.streams.kstream.KStream#split() KStream.split()}
 * overload — the nullary overload that does NOT take a {@link
 * org.apache.kafka.streams.kstream.Named Named} argument and
 * therefore leaves the split parent node auto-named from the
 * topology graph index. Every downstream {@code Branched.as(
 * "x")} key gets prefixed with that unstable auto-name.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code stream::split} bound to {@link
 * java.util.function.Supplier
 * Supplier&lt;BranchedKStream&gt;} or to a custom SAM whose
 * erased implMethod descriptor matches the unsafe overload).
 *
 * <h2>Why no-Named split() is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#split
 * KStream.split} attaches a branch-parent node to the topology
 * and returns a {@link
 * org.apache.kafka.streams.kstream.BranchedKStream
 * BranchedKStream} on which each subsequent {@code branch(
 * predicate, Branched.as("x"))} produces a child node and a map
 * entry. The KEY of every entry in the resulting
 * {@code Map<String, KStream<K, V>>} is built as
 * {@code "<split-parent-name>-<branch-as-suffix>"}. Without an
 * explicit {@link
 * org.apache.kafka.streams.kstream.Named}, the split-parent name
 * is auto-derived from the topology graph index — e.g. {@code
 * KSTREAM-BRANCH-0000000007}. Every downstream map lookup, every
 * processor name, every metric tag, every {@code
 * topology.describe()} block referencing those branch children
 * carries the unstable graph-index prefix.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Branched map lookups silently key-miss after a
 *       topology edit.</b> A team has {@code Map&lt;String,
 *       KStream&lt;K, V&gt;&gt; branches = stream.split()
 *       .branch((k, v) -> v.isVip(), Branched.as("vip"))
 *       .branch((k, v) -> true, Branched.defaultBranch("rest"))
 *       .noDefaultBranch()}. They look up the VIP branch as
 *       {@code branches.get("KSTREAM-BRANCH-0000000007-vip")}.
 *       Team adds one upstream filter; the graph index shifts;
 *       the map keys are now {@code "KSTREAM-BRANCH-
 *       0000000009-vip"}; the lookup returns null silently; the
 *       VIP downstream pipeline reads zero records; the
 *       business-critical VIP routing breaks with no error log.</li>
 *   <li><b>Branch-tagged metric panels silently empty after a
 *       topology edit.</b> Grafana panels filtered on
 *       {@code processor-node-id="KSTREAM-BRANCH-0000000007-
 *       vip"} for records-processed-rate go to zero
 *       indefinitely; oncall stops looking; a real
 *       branch-processing collapse two weeks later is
 *       invisible. The metrics still exist under the NEW
 *       processor-node-id, but no dashboard knows where to look.</li>
 *   <li><b>{@code topology.describe()} runbook drift.</b> SRE
 *       runbooks reference branch nodes by their auto-generated
 *       names — {@code "if KSTREAM-BRANCH-0000000007-vip
 *       records-processed-rate drops below 100/s, page the
 *       on-call"}. After any topology edit the referenced
 *       branch node no longer exists by that name; the runbook
 *       step silently no-ops; the operator believes they are
 *       monitoring the VIP branch but the wrong node (or no
 *       node at all) was targeted.</li>
 *   <li><b>Cross-app branch-key drift on shared map contracts.</b>
 *       Team A produces a {@code Map<String, KStream>} that
 *       Team B is expected to consume by key. Team A defines
 *       branch suffix {@code "vip"}; Team B reads
 *       {@code map.get(prefix + "-vip")}. Without an explicit
 *       Named on the split-parent, the prefix Team B must use
 *       depends on Team A's current graph index — a non-
 *       contract value that changes on every Team A topology
 *       edit. Team B's integration silently breaks every time
 *       Team A deploys a topology change.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       split-factory built as {@code Supplier&lt;BranchedKStream
 *       &lt;K, V&gt;&gt; splitter = stream::split} compiles to
 *       {@code INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle pointing at {@code
 *       KStream.split()BranchedKStream}. The user-class
 *       bytecode contains zero direct {@code INVOKEINTERFACE}
 *       on the no-Named overload, only the indy site.</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the single unsafe overload
 * descriptor {@code ()Lorg/apache/kafka/streams/kstream/
 * BranchedKStream;}. Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the split-
 * parent — e.g. {@code stream.split(Named.as(
 * "order-routing-split"))}. The chosen name is stable across
 * topology edits because the user wrote it down, and every
 * downstream branch map key inherits the stable prefix.
 */
public final class StreamsSplitNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "split";
    private static final String UNSAFE_DESC =
            "()Lorg/apache/kafka/streams/kstream/BranchedKStream;";

    private final Severity severity;

    public StreamsSplitNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_SPLIT_NO_NAMED;
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
                RuleId.STREAMS_SPLIT_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.split() — the no-Named overload is "
                        + "reached here — either as a direct "
                        + "INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::split` bound to "
                        + "Supplier<BranchedKStream> or to a custom "
                        + "SAM whose erased implMethod descriptor "
                        + "matches the unsafe overload). The split "
                        + "parent node is auto-named from the "
                        + "topology graph index (e.g. KSTREAM-BRANCH-"
                        + "0000000007); every downstream "
                        + "branch(predicate, Branched.as(\"x\")) "
                        + "produces a map entry whose KEY is built as "
                        + "`<split-parent-name>-<branch-as-suffix>`. "
                        + "Concrete failure modes: (1) branched map "
                        + "lookups silently key-miss after a topology "
                        + "edit — team has "
                        + "`Map<String, KStream<K, V>> branches = "
                        + "stream.split().branch((k, v) -> v.isVip(), "
                        + "Branched.as(\"vip\")).branch((k, v) -> "
                        + "true, Branched.defaultBranch(\"rest\"))."
                        + "noDefaultBranch()`; they look up the VIP "
                        + "branch as branches.get(\"KSTREAM-BRANCH-"
                        + "0000000007-vip\"); team adds one upstream "
                        + "filter; the graph index shifts; the map "
                        + "keys are now \"KSTREAM-BRANCH-0000000009-"
                        + "vip\"; the lookup returns null silently; "
                        + "the VIP downstream pipeline reads zero "
                        + "records; the business-critical VIP "
                        + "routing breaks with no error log; (2) "
                        + "branch-tagged metric panels silently "
                        + "empty after a topology edit — Grafana "
                        + "panels filtered on processor-node-id="
                        + "\"KSTREAM-BRANCH-0000000007-vip\" for "
                        + "records-processed-rate go to zero "
                        + "indefinitely; oncall stops looking; a "
                        + "real branch-processing collapse two "
                        + "weeks later is invisible; the metrics "
                        + "still exist under the NEW processor-"
                        + "node-id, but no dashboard knows where "
                        + "to look; (3) topology.describe() runbook "
                        + "drift — SRE runbooks reference branch "
                        + "nodes by their auto-generated names "
                        + "(\"if KSTREAM-BRANCH-0000000007-vip "
                        + "records-processed-rate drops below "
                        + "100/s, page the on-call\"); after any "
                        + "topology edit the referenced branch "
                        + "node no longer exists by that name; the "
                        + "runbook step silently no-ops; the "
                        + "operator believes they are monitoring "
                        + "the VIP branch but the wrong node (or "
                        + "no node at all) was targeted; (4) "
                        + "cross-app branch-key drift on shared "
                        + "map contracts — Team A produces a "
                        + "Map<String, KStream> that Team B is "
                        + "expected to consume by key; Team A "
                        + "defines branch suffix \"vip\"; Team B "
                        + "reads map.get(prefix + \"-vip\"); "
                        + "without an explicit Named on the split-"
                        + "parent, the prefix Team B must use "
                        + "depends on Team A's current graph "
                        + "index — a non-contract value that "
                        + "changes on every Team A topology edit; "
                        + "Team B's integration silently breaks "
                        + "every time Team A deploys a topology "
                        + "change; (5) INVOKEDYNAMIC method-"
                        + "reference captures bypass naive "
                        + "MethodInsnNode-only lint — "
                        + "`Supplier<BranchedKStream<K, V>> "
                        + "splitter = stream::split` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KStream.split()BranchedKStream; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named "
                        + "overload, only the indy site. "
                        + "Migration: pass an explicit Named "
                        + "naming the split-parent — "
                        + "`stream.split(Named.as(\"order-routing-"
                        + "split\"))`. The chosen name is stable "
                        + "across topology edits because the user "
                        + "wrote it down, and every downstream "
                        + "branch map key inherits the stable "
                        + "prefix. The split(Named) overload is "
                        + "never flagged by this rule.");
    }
}
