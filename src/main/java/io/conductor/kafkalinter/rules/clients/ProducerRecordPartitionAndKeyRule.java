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
 * Flags {@code new ProducerRecord<>(topic, partition, key, value)} (and the 5/6-arg
 * variants with headers/timestamp) where an explicit {@code Integer partition} is
 * supplied. With an explicit partition the partitioner is bypassed and the key
 * stops driving placement — so key-based ordering, log-compaction-by-key, and
 * adaptive partitioning (KIP-794) all silently lose their guarantees.
 *
 * <p>Detection is by INVOKESPECIAL descriptor prefix
 * {@code (Ljava/lang/String;Ljava/lang/Integer;...}. The legitimate fan-out form
 * {@code new ProducerRecord<>(topic, p, null, value)} matches the same prefix —
 * silence it with a per-class severity override; explicitly-null keys in
 * production code are rare enough to keep the false-positive rate acceptable.
 */
public final class ProducerRecordPartitionAndKeyRule implements Rule {

    private static final String DESC_PREFIX = "(Ljava/lang/String;Ljava/lang/Integer;";

    private final Severity severity;

    public ProducerRecordPartitionAndKeyRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_RECORD_PARTITION_AND_KEY;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!"<init>".equals(mi.name)) continue;
                if (!KafkaTypes.PRODUCER_RECORD.equals(mi.owner)) continue;
                if (!mi.desc.startsWith(DESC_PREFIX)) continue;

                out.add(new Violation(
                        RuleId.PRODUCER_RECORD_PARTITION_AND_KEY, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "new ProducerRecord(topic, partition, ...) — explicit partition bypasses the partitioner; key-based ordering and compaction-by-key no longer apply. Use new ProducerRecord(topic, key, value) unless you have a replay/admin reason to pin the partition."));
            }
        }
        return out;
    }
}
