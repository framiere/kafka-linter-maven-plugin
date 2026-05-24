package io.conductor.kafkalinter.rules.streams;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every {@code KafkaStreams.cleanUp()} call site found in the
 * project's compiled main classes ({@code target/classes}). The linter does
 * NOT scan {@code target/test-classes}, so any call discovered here is by
 * construction in non-test code.
 *
 * <p>{@code cleanUp()} deletes everything under the application's
 * {@code state.dir} for this {@code application.id}. On the next
 * {@code start()}, Streams must replay the entire changelog topic to rebuild
 * RocksDB — turning a seconds-long restart into a minutes-or-hours-long
 * outage during which the topology consumes and produces nothing.
 *
 * <h2>Method-reference capture is also flagged</h2>
 *
 * <p>{@code KafkaStreams.cleanUp()} has signature {@code ()V} — a perfect
 * fit for {@code Runnable}, and through that for every standard async
 * sink: {@code Executor.execute}, {@code Executor.submit},
 * {@code new Thread(Runnable)}, {@code CompletableFuture.runAsync},
 * {@code Runtime.addShutdownHook}, scheduler tasks, etc. When the user
 * writes {@code executor.execute(streams::cleanUp)} the actual
 * {@code cleanUp()} invocation materialises inside the SAM adapter on
 * whatever worker thread later runs the captured target — and the
 * user-class bytecode contains <strong>zero {@code INVOKE*}
 * instructions targeting {@code cleanUp}</strong>.
 *
 * <p>A MethodInsnNode-only walk reports {@code "no cleanUp → no rule"}
 * and emits ZERO violations on this entire pattern, leaving the
 * canonical "wipe state from a background task" anti-pattern silently
 * unmarked. With the indy walk plus the handle-descriptor discriminator
 * ({@code ()V}), the rule fires once per capture site.
 *
 * <p>No SAM-return discriminator is needed — {@code cleanUp()} returns
 * {@code void}, so the SAM adapter cannot mask the call's side effect
 * (state-store deletion). The hazard is the API choice itself, not the
 * observability of any return value.
 */
public final class StreamsCleanupInProdRule implements Rule {

    private static final String CLEAN_UP = "cleanUp";
    private static final String CLEAN_UP_DESC = "()V";

    private final Severity severity;

    public StreamsCleanupInProdRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_CLEANUP_IN_PROD;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && mi.getOpcode() == Opcodes.INVOKEVIRTUAL
                        && KafkaTypes.KAFKA_STREAMS.equals(mi.owner)
                        && CLEAN_UP.equals(mi.name)
                        && CLEAN_UP_DESC.equals(mi.desc)) {
                    out.add(directViolation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(
                            indy, Set.of(KafkaTypes.KAFKA_STREAMS), CLEAN_UP, CLEAN_UP_DESC);
                    if (h == null) continue;
                    out.add(methodRefViolation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation directViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_CLEANUP_IN_PROD, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KafkaStreams.cleanUp() is called here in non-test code. cleanUp() "
                        + "deletes everything under state.dir for this application.id — every "
                        + "RocksDB state store this instance owns. On the next start(), Streams "
                        + "must replay the entire changelog topic from offset 0 to rebuild the "
                        + "stores; during that window the topology is paused (no records "
                        + "consumed, no records produced). Cold-start time grows from seconds to "
                        + "minutes or hours, proportional to changelog size. cleanUp() exists "
                        + "for the test pattern of 'fresh state for each test run'; in "
                        + "production, even a small bug that triggers it on a hot path is a "
                        + "multi-hour outage. If you genuinely need to reset state in prod, do "
                        + "it from an operational runbook (stop the app, delete state.dir on "
                        + "disk, start the app) — not from application code.");
    }

    private Violation methodRefViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_CLEANUP_IN_PROD, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "cleanUp() reference captured (e.g. executor.execute(streams::cleanUp), "
                        + "new Thread(streams::cleanUp).start(), CompletableFuture.runAsync("
                        + "streams::cleanUp)) in non-test code. Whichever worker thread later "
                        + "runs the captured Runnable, the call deletes everything under "
                        + "state.dir for this application.id — every RocksDB state store this "
                        + "instance owns. The next start() then has to replay the entire "
                        + "changelog topic from offset 0 to rebuild the stores; the topology "
                        + "pauses for that window (no records consumed, no records produced) "
                        + "and the outage scales with changelog size. cleanUp() exists for the "
                        + "test pattern of 'fresh state for each test run'; if you genuinely "
                        + "need to reset state in prod, do it from an operational runbook (stop "
                        + "the app, delete state.dir on disk, start the app) — not from "
                        + "application code captured into an executor.");
    }
}
