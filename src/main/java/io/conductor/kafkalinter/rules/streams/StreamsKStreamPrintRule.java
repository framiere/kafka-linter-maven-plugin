package io.conductor.kafkalinter.rules.streams;

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
 * Fires for every {@code KStream.print(...)} call. {@code print()} is a
 * debugging operator: it inserts a {@code PrintForeachAction} processor
 * that runs {@code System.out.println(record)} on every record. In a
 * production topology this becomes the bottleneck — {@code System.out} is
 * line-synchronized, every StreamThread serialises through one lock — and
 * the records go to container stdout where nobody reads them.
 *
 * <p>The rule matches by owner ({@code KStream}) + method name
 * ({@code print}) only, ignoring the descriptor. {@code print} is reserved
 * for debugging in the public KStream API; matching the name alone keeps
 * the rule resilient to overload additions in future Kafka versions.
 */
public final class StreamsKStreamPrintRule implements Rule {

    private static final String PRINT = "print";

    private final Severity severity;

    public StreamsKStreamPrintRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_KSTREAM_PRINT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.KSTREAM.equals(mi.owner)) continue;
                if (!PRINT.equals(mi.name)) continue;
                out.add(new Violation(
                        RuleId.STREAMS_KSTREAM_PRINT, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "KStream.print(...) is called here. print() inserts a "
                                + "PrintForeachAction processor that calls System.out.println(record) "
                                + "on every key/value passing through the stream. This is a debugging "
                                + "operator that has no business in a deployed topology: on a topology "
                                + "handling thousands of records per second, System.out — which is "
                                + "line-synchronized — becomes the bottleneck, serialising every "
                                + "StreamThread through one lock. Even when redirected to a file the "
                                + "synchronous I/O hop sits in the middle of the processing graph. And "
                                + "the records themselves go to the container's stdout, often capped "
                                + "at a few MB of ring buffer, where nobody reads them. If you need to "
                                + "inspect records in production, use peek() paired with a metrics "
                                + "counter or a structured log call, or route them to a dedicated "
                                + "debug topic via to(); never wire System.out into a stream."));
            }
        }
        return out;
    }
}
