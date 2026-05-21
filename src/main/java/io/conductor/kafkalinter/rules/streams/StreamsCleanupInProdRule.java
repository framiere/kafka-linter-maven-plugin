package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

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
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL) continue;
                if (!KafkaTypes.KAFKA_STREAMS.equals(mi.owner)) continue;
                if (!CLEAN_UP.equals(mi.name)) continue;
                if (!CLEAN_UP_DESC.equals(mi.desc)) continue;
                out.add(new Violation(
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
                                + "disk, start the app) — not from application code."));
            }
        }
        return out;
    }
}
