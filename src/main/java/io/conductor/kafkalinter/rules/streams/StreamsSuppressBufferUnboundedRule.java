package io.conductor.kafkalinter.rules.streams;

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
 * Fires for every reach of {@link
 * org.apache.kafka.streams.kstream.Suppressed.BufferConfig#unbounded()
 * Suppressed.BufferConfig.unbounded()} — the static factory that
 * configures a suppress() operator with an UNCAPPED in-memory
 * buffer, deferring window-closed records (or session-closed
 * records) inside the JVM heap until the suppress operator
 * flushes downstream.
 *
 * <p>Catches both the direct {@code INVOKESTATIC} call on the
 * {@code BufferConfig} interface and {@code INVOKEDYNAMIC}
 * method-reference captures (e.g. {@code BufferConfig::unbounded}
 * bound to a {@link java.util.function.Supplier Supplier} of
 * {@code BufferConfig}, or a custom SAM-typed factory pointing
 * at the same).
 *
 * <h2>What unbounded() actually does — the operational hazard</h2>
 *
 * <p>The {@code suppress()} operator on a windowed {@code
 * KTable} (or session-windowed table) buffers update events in
 * memory until either (a) the window/session closes per stream-
 * time + grace period, or (b) the buffer's capacity limit is
 * reached. With {@code BufferConfig.unbounded()} option (b) is
 * removed: the buffer has NO capacity limit and the operator
 * defers indefinitely. Every consequence of this choice is
 * severe and silent:
 *
 * <ul>
 *   <li><b>Heap exhaustion on stalled downstream.</b> A slow
 *       downstream consumer or a producer that throttles on
 *       broker backpressure stalls the commit pipeline; the
 *       suppress buffer keeps accepting upstream window updates;
 *       buffer occupancy grows without bound until the JVM
 *       triggers OutOfMemoryError. The streams thread dies; the
 *       Streams instance enters ERROR state; the task is
 *       reassigned to another instance which then ALSO buffers
 *       unboundedly because nothing changed; the entire app
 *       crash-loops.</li>
 *   <li><b>Latency goes unbounded with no early signal.</b>
 *       Window-closed events held in the buffer are not
 *       observable downstream. End-to-end latency on the
 *       suppress'd topology rises silently from the expected
 *       window-grace duration into minutes, hours, then heap
 *       exhaustion. There is no metric that early-warns "the
 *       buffer is getting big" — by the time GC pauses become
 *       visible the buffer is already at 80% of heap.</li>
 *   <li><b>Stream-time on a single late key holds back ALL
 *       keys.</b> A single late-arriving key with high event-
 *       time skew delays window close across every partition
 *       co-located on the same task. Combined with unbounded()
 *       this means one slow upstream key can balloon the buffer
 *       to gigabytes while every other key's output is held
 *       hostage to its commit.</li>
 *   <li><b>Restore amplifies the damage.</b> On task
 *       reassignment Streams replays the changelog of the
 *       suppress'd KTable; the new task's buffer is reseeded
 *       from the changelog; if the original buffer was already
 *       large at the time of the rebalance, the new task starts
 *       with an equally-large buffer and is just as close to
 *       OOM. Without the implicit bound from {@code maxBytes()}
 *       or {@code maxRecords()} the operator has no recovery
 *       path.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       topology-builder helper that captures
 *       {@code BufferConfig::unbounded} as a SAM-typed factory
 *       compiles to {@code INVOKEDYNAMIC} whose bsm-args
 *       contain a {@code REF_invokeStatic} Handle pointing at
 *       {@code Suppressed$BufferConfig.unbounded()}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKESTATIC} on the method, only the indy site.</li>
 * </ul>
 *
 * <h2>Migration</h2>
 *
 * <p>Replace {@code BufferConfig.unbounded()} with an explicit
 * bound:
 *
 * <ul>
 *   <li>{@code BufferConfig.maxBytes(n).shutDownWhenFull()} —
 *       cap by buffer-size in bytes; on overflow Streams shuts
 *       the instance down (loud, recoverable failure) rather
 *       than balloon heap.</li>
 *   <li>{@code BufferConfig.maxRecords(n).shutDownWhenFull()} —
 *       cap by record count.</li>
 *   <li>{@code .emitEarlyWhenFull()} — on overflow emit
 *       intermediate (possibly-incorrect-but-bounded) results
 *       instead of shutting down. Use only if the downstream
 *       contract permits intermediate emissions.</li>
 * </ul>
 *
 * <p>The bound MUST be expressed; the only correct value for
 * the unbounded variant is "we have proven this buffer cannot
 * grow", which is rarely true.
 */
public final class StreamsSuppressBufferUnboundedRule implements Rule {

    private static final String OWNER = KafkaTypes.SUPPRESSED_BUFFER_CONFIG;
    private static final Set<String> OWNERS = Set.of(OWNER);
    private static final String METHOD_NAME = "unbounded";

    private final Severity severity;

    public StreamsSuppressBufferUnboundedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_SUPPRESS_BUFFER_UNBOUNDED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNER.equals(mi.owner)
                        && METHOD_NAME.equals(mi.name)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
                    if (h != null) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_SUPPRESS_BUFFER_UNBOUNDED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Suppressed.BufferConfig.unbounded() — the static "
                        + "factory that configures a suppress() "
                        + "operator with an UNCAPPED in-memory buffer, "
                        + "deferring window-closed records (or session-"
                        + "closed records) inside the JVM heap until "
                        + "the suppress operator flushes downstream — "
                        + "reached here either as a direct INVOKESTATIC "
                        + "on Suppressed$BufferConfig or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`BufferConfig::unbounded` bound to a "
                        + "Supplier<BufferConfig> or a custom SAM-typed "
                        + "factory pointing at the same). The "
                        + "suppress() operator on a windowed KTable "
                        + "(or session-windowed table) buffers update "
                        + "events in memory until either (a) the "
                        + "window/session closes per stream-time + "
                        + "grace period, or (b) the buffer's capacity "
                        + "limit is reached. With BufferConfig."
                        + "unbounded() option (b) is removed: the "
                        + "buffer has NO capacity limit and the "
                        + "operator defers indefinitely. Every "
                        + "consequence is severe and silent: (1) heap "
                        + "exhaustion on stalled downstream — a slow "
                        + "downstream consumer or a producer that "
                        + "throttles on broker backpressure stalls the "
                        + "commit pipeline, the suppress buffer keeps "
                        + "accepting upstream window updates, buffer "
                        + "occupancy grows without bound until the JVM "
                        + "triggers OutOfMemoryError, the streams "
                        + "thread dies, the Streams instance enters "
                        + "ERROR state, the task is reassigned to "
                        + "another instance which then ALSO buffers "
                        + "unboundedly because nothing changed, the "
                        + "entire app crash-loops; (2) latency goes "
                        + "unbounded with no early signal — window-"
                        + "closed events held in the buffer are not "
                        + "observable downstream, end-to-end latency "
                        + "on the suppress'd topology rises silently "
                        + "from the expected window-grace duration "
                        + "into minutes, hours, then heap exhaustion, "
                        + "there is no metric that early-warns \"the "
                        + "buffer is getting big\" — by the time GC "
                        + "pauses become visible the buffer is already "
                        + "at 80% of heap; (3) stream-time on a single "
                        + "late key holds back ALL keys — a single "
                        + "late-arriving key with high event-time skew "
                        + "delays window close across every partition "
                        + "co-located on the same task, combined with "
                        + "unbounded() this means one slow upstream "
                        + "key can balloon the buffer to gigabytes "
                        + "while every other key's output is held "
                        + "hostage to its commit; (4) restore "
                        + "amplifies the damage — on task reassignment "
                        + "Streams replays the changelog of the "
                        + "suppress'd KTable, the new task's buffer is "
                        + "reseeded from the changelog, if the "
                        + "original buffer was already large at the "
                        + "time of the rebalance the new task starts "
                        + "with an equally-large buffer and is just "
                        + "as close to OOM, without the implicit bound "
                        + "from maxBytes() or maxRecords() the "
                        + "operator has no recovery path; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — a "
                        + "topology-builder helper that captures "
                        + "`BufferConfig::unbounded` as a SAM-typed "
                        + "factory compiles to INVOKEDYNAMIC whose "
                        + "bsm-args contain a REF_invokeStatic Handle "
                        + "pointing at Suppressed$BufferConfig."
                        + "unbounded(), the user-class bytecode "
                        + "contains zero direct INVOKESTATIC on the "
                        + "method, only the indy site. Migration: "
                        + "replace BufferConfig.unbounded() with an "
                        + "explicit bound — BufferConfig.maxBytes(n)."
                        + "shutDownWhenFull() caps by buffer-size in "
                        + "bytes and on overflow Streams shuts the "
                        + "instance down (loud, recoverable failure) "
                        + "rather than balloon heap; BufferConfig."
                        + "maxRecords(n).shutDownWhenFull() caps by "
                        + "record count; .emitEarlyWhenFull() on "
                        + "overflow emits intermediate (possibly-"
                        + "incorrect-but-bounded) results instead of "
                        + "shutting down (use only if the downstream "
                        + "contract permits intermediate emissions). "
                        + "The bound MUST be expressed; the only "
                        + "correct value for the unbounded variant is "
                        + "\"we have proven this buffer cannot grow\", "
                        + "which is rarely true.");
    }
}
