package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Class-scoped rule: fires when {@code Consumer.pause(...)} is invoked anywhere
 * in the class but {@code Consumer.resume(...)} is invoked nowhere in the same
 * class.
 *
 * <h2>Why class scope</h2>
 *
 * <p>Pause and resume are commonly split across methods of the same handler
 * class — typically {@code onBackpressureFull()} pauses and
 * {@code onBackpressureDrained()} resumes. Method scope would produce false
 * positives on this legitimate split. Project scope would produce false
 * negatives when an unrelated consumer helper elsewhere in the project happens
 * to call {@code resume}. Class scope is the right granularity for the
 * pause/resume symmetry contract.
 *
 * <h2>Matched call shapes</h2>
 *
 * <p>Both {@code pause} and {@code resume} on {@code KafkaConsumer} /
 * {@code Consumer} take {@code Collection<TopicPartition>} and are invoked via
 * {@code INVOKEVIRTUAL} (concrete class) or {@code INVOKEINTERFACE} (interface).
 * The rule matches by owner + name regardless of descriptor — defensive against
 * any future overload addition.
 *
 * <h2>Deliberate non-matches</h2>
 *
 * <p>Method-reference captures of {@code resume} (e.g.
 * {@code executor.submit(consumer::resume)}) are NOT recognised as evidence of
 * a resume call. Such shapes are extremely rare in practice and the
 * cross-method-reference walk needed to confirm the executor actually fires
 * the handle would widen the rule beyond the direct-call signal it targets.
 */
public final class ConsumerPauseNoResumeRule implements Rule {

    private static final String PAUSE = "pause";
    private static final String RESUME = "resume";

    private final Severity severity;

    public ConsumerPauseNoResumeRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PAUSE_NO_RESUME;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<PauseSite> pauseSites = new ArrayList<>();
        boolean hasResume = false;

        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if (PAUSE.equals(mi.name)) {
                    pauseSites.add(new PauseSite(mn, AsmUtil.lineOf(insn)));
                } else if (RESUME.equals(mi.name)) {
                    hasResume = true;
                }
            }
        }

        if (pauseSites.isEmpty() || hasResume) return List.of();

        List<Violation> out = new ArrayList<>();
        String simpleClass = ctx.classNode().name.substring(ctx.classNode().name.lastIndexOf('/') + 1);
        for (PauseSite site : pauseSites) {
            out.add(new Violation(
                    RuleId.CONSUMER_PAUSE_NO_RESUME, severity,
                    ctx.classNode().name, site.method.name, site.line,
                    "Consumer.pause(...) at " + simpleClass + "#" + site.method.name
                            + " but Consumer.resume(...) is not called anywhere in this class — the paused "
                            + "partitions stay in the consumer's assignment, the consumer keeps heartbeating, "
                            + "and the group coordinator sees the consumer as ACTIVE, but poll() returns zero "
                            + "records for those partitions until resume() is called on the SAME KafkaConsumer "
                            + "instance. The bug is invisible from the outside: no exception, no log line, no "
                            + "rebalance — only a per-partition lag that climbs indefinitely while the other "
                            + "partitions of the same consumer's assignment process normally. If you truly want "
                            + "to stop consuming from those partitions for the rest of the consumer's life, use "
                            + "`consumer.unsubscribe()` or `consumer.assign(remainingPartitions)` so the group "
                            + "rebalances them to a consumer that can. If pause is the right primitive, wire the "
                            + "matching resume on the downstream-drained / work-completed path."));
        }
        return out;
    }

    private record PauseSite(MethodNode method, int line) {}
}
