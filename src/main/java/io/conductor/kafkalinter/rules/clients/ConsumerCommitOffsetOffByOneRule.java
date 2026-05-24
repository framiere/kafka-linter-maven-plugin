package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Fires on {@code new OffsetAndMetadata(record.offset())} — the off-by-one
 * commit shape where the offset of the JUST-PROCESSED record is committed
 * instead of {@code record.offset() + 1}. The kafka-clients commit contract
 * is unambiguous: the committed offset is the offset the consumer should
 * READ NEXT, not the offset it last processed. So passing
 * {@code record.offset()} (instead of {@code record.offset() + 1}) tells the
 * broker "next subscribe should resume AT N" — re-delivering the record at
 * offset N — and at-least-once becomes at-least-twice for the last record of
 * every committed batch on every consumer restart or rebalance.
 *
 * <p>Detection is structural and conservative. We walk every method's
 * instruction list and on every {@code INVOKESPECIAL} of
 * {@code OffsetAndMetadata.<init>} whose descriptor starts with {@code (J}
 * (i.e. all three relevant constructors:
 * {@code (J)V}, {@code (JLjava/lang/String;)V},
 * {@code (JLjava/util/Optional;Ljava/lang/String;)V}), we look back from the
 * arguments-on-stack region for the producer of the long offset value. The
 * only firing shape: the IMMEDIATELY preceding significant instruction is
 * {@code INVOKEVIRTUAL ConsumerRecord.offset()J} (descriptor {@code ()J})
 * with NO intervening {@code LCONST_1 + LADD} (the bytecode of {@code + 1}).
 * Through-local-variable shapes
 * ({@code long o = record.offset(); ... new OffsetAndMetadata(o)}) compile
 * to {@code LSTORE} + {@code LLOAD} between the {@code offset()} call and
 * the {@code <init>}, so {@code prevSignificant} is {@code LLOAD} rather
 * than {@code INVOKEVIRTUAL} and the rule does NOT fire on them — an
 * intentional false-negative to keep the false-positive rate at zero.
 */
public final class ConsumerCommitOffsetOffByOneRule implements Rule {

    private static final String INIT = "<init>";
    private static final String OFFSET_METHOD = "offset";
    private static final String OFFSET_DESC = "()J";

    private final Severity severity;

    public ConsumerCommitOffsetOffByOneRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_COMMIT_OFFSET_OFF_BY_ONE;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            if (mn.instructions == null) continue;
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (mi.getOpcode() != Opcodes.INVOKESPECIAL) continue;
                if (!KafkaTypes.OFFSET_AND_METADATA.equals(mi.owner)) continue;
                if (!INIT.equals(mi.name)) continue;
                if (mi.desc == null || !mi.desc.startsWith("(J")) continue;

                // Walk back past the trailing args (everything after the leading J).
                // Each remaining arg is assumed to occupy exactly one
                // significant instruction (ALOAD, LDC, ACONST_NULL,
                // GETSTATIC, etc.). This covers the three overloads
                // (J)V, (JLString;)V, (JLOptional;LString;)V; method-call
                // computed metadata strings would not match, which is the
                // intentional conservative cut.
                Type[] argTypes = Type.getArgumentTypes(mi.desc);
                int extraArgs = argTypes.length - 1;
                AbstractInsnNode cursor = AsmUtil.prevSignificant(mi);
                for (int i = 0; i < extraArgs && cursor != null; i++) {
                    cursor = AsmUtil.prevSignificant(cursor);
                }
                if (!(cursor instanceof MethodInsnNode prevMi)) continue;
                if (prevMi.getOpcode() != Opcodes.INVOKEVIRTUAL) continue;
                if (!KafkaTypes.CONSUMER_RECORD.equals(prevMi.owner)) continue;
                if (!OFFSET_METHOD.equals(prevMi.name)) continue;
                if (!OFFSET_DESC.equals(prevMi.desc)) continue;

                out.add(new Violation(
                        RuleId.CONSUMER_COMMIT_OFFSET_OFF_BY_ONE, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(mi),
                        "new OffsetAndMetadata(record.offset()) — off-by-one commit. The committed offset "
                                + "must be the NEXT offset to consume (record.offset() + 1), not the offset of "
                                + "the record just processed. Passing record.offset() tells the broker to "
                                + "resume AT this offset on the next subscribe, so the record at this offset is "
                                + "re-delivered on every consumer restart, rebalance, or partition reassignment "
                                + "— at-least-once silently becomes at-least-twice for the last record of every "
                                + "committed batch. The bug is invisible during steady-state operation (the "
                                + "in-memory `position` advances correctly within the same JVM) and only "
                                + "manifests when another consumer reads `__consumer_offsets` for the starting "
                                + "position. Fix: change `new OffsetAndMetadata(record.offset())` to "
                                + "`new OffsetAndMetadata(record.offset() + 1)`."));
            }
        }
        return out;
    }
}
