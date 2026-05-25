package io.conductor.kafkalinter.rules.clients;

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
 * Fires for every reach of the deprecated 7-arg
 * {@link org.apache.kafka.clients.producer.RecordMetadata} constructor
 * with a {@code Long checksum} parameter — whether the call lands
 * directly via {@code INVOKESPECIAL} or indirectly through an
 * {@code INVOKEDYNAMIC} constructor-reference capture (e.g.
 * {@code RecordMetadata::new} bound to a factory functional interface
 * used by mock-producer scaffolding, parameterized-test sources, or
 * replay tooling that synthesizes producer-side acknowledgment metadata).
 *
 * <h2>Why this constructor surface is deprecated, not merely cosmetic</h2>
 *
 * <p>{@code RecordMetadata} is the per-record acknowledgment receipt the
 * producer hands back to the user from {@code Future<RecordMetadata>} or
 * the {@code Callback} parameter of {@code Producer.send(...)}. Test
 * scaffolding, mock-producer libraries, and replay tools hand-rolled
 * {@code RecordMetadata} instances to drive callback-side assertions
 * without a real broker round-trip. The deprecated constructor's
 * {@code Long checksum} parameter dates from v0/v1 message format,
 * where the broker stamped a CRC32 onto every record and surfaced it
 * back to the producer for end-to-end integrity checks.
 *
 * <p>KIP-98 (Kafka 0.11, v2 message format) moved the CRC from the
 * per-record level up to the batch level — the broker now computes a
 * single CRC over the entire {@code MemoryRecordsBuilder} batch and the
 * per-record checksum slot in {@code RecordMetadata} was retained only
 * for legacy decode. KIP-101 / KIP-82 then formally deprecated the
 * constructor that exposes this slot. Three consequences follow:
 *
 * <ul>
 *   <li><b>The checksum value carries no useful information.</b> Real
 *       producers populate this field with {@code -1L} (the documented
 *       sentinel for &laquo;unknown&raquo;) when the broker batch CRC
 *       cannot be attributed to a single record. Synthetic
 *       {@code RecordMetadata} built via the deprecated constructor
 *       passes whatever value the test wrote — typically a random Long
 *       — and any downstream code that asserts on {@code metadata.checksum()}
 *       observes the synthetic value, not the {@code -1L} the real
 *       producer would emit. Tests pass; production produces different
 *       data.</li>
 *   <li><b>The deprecated constructor's {@code Long} type discriminates
 *       it from the modern primitive-typed constructor.</b> The
 *       non-deprecated 6-arg constructor takes
 *       {@code (TopicPartition, baseOffset, batchIndex, timestamp,
 *       keySize, valueSize)} — all primitive — and the absence of
 *       {@code Long} in the descriptor is the rule's discriminator.</li>
 *   <li><b>INVOKEDYNAMIC {@code RecordMetadata::new} captures silently
 *       bind to the deprecated constructor whenever the factory SAM
 *       arity matches 7.</b> A mock-producer test harness that defines
 *       a {@code RecordMetadataFactory} functional interface with the
 *       full constructor parameter list (so callers can vary any field)
 *       resolves {@code RecordMetadata::new} to whichever overload
 *       matches that arity — for a 7-arg SAM, the deprecated overload
 *       is the only candidate. The user-class bytecode contains zero
 *       direct {@code INVOKESPECIAL} on the legacy ctor and a name-only
 *       MethodInsnNode walk misses the call entirely.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>The non-deprecated 6-arg constructor takes
 * {@code (TopicPartition, baseOffset, batchIndex, timestamp,
 * serializedKeySize, serializedValueSize)} — no checksum parameter,
 * fully primitive, matching the shape the modern producer ack path
 * surfaces. Mock-producer libraries should migrate every
 * {@code new RecordMetadata(...)} site to this constructor.
 *
 * <h2>Descriptor discrimination</h2>
 *
 * <p>{@code <init>} is overloaded on {@code RecordMetadata}: the legacy
 * 7-arg ctor has descriptor
 * {@code (Lorg/apache/kafka/common/TopicPartition;JJJLjava/lang/Long;II)V}
 * and the modern 6-arg ctor has
 * {@code (Lorg/apache/kafka/common/TopicPartition;JIJII)V}. The rule
 * matches the substring {@code Ljava/lang/Long;II)V} — present in the
 * legacy descriptor (boxed {@code Long} followed by two {@code int}s
 * followed by void return), absent from the modern descriptor (only
 * primitive {@code I} / {@code J} types). This precise discrimination
 * is what lets the rule fire on the legacy ctor without false-positiving
 * on the supported migration target.
 *
 * <h2>Constructor-reference capture path</h2>
 *
 * <p>{@code RecordMetadata::new} bound to a factory functional interface
 * compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_newInvokeSpecial} handle pointing at the resolved
 * constructor. The rule's bsm-arg walk catches this case by checking
 * the handle's {@code (owner, name, desc)} triple against the same
 * filter used for direct calls.
 */
public final class ProducerRecordMetadataLegacyChecksumCtorDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.RECORD_METADATA);
    private static final String CTOR_NAME = "<init>";
    private static final String LEGACY_FRAGMENT = "Ljava/lang/Long;II)V";

    private final Severity severity;

    public ProducerRecordMetadataLegacyChecksumCtorDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_RECORD_METADATA_LEGACY_CHECKSUM_CTOR_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && CTOR_NAME.equals(mi.name)
                        && isLegacyChecksumDescriptor(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, CTOR_NAME, null);
                    if (h != null && isLegacyChecksumDescriptor(h.getDesc())) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private static boolean isLegacyChecksumDescriptor(String desc) {
        return desc != null && desc.contains(LEGACY_FRAGMENT);
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.PRODUCER_RECORD_METADATA_LEGACY_CHECKSUM_CTOR_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "RecordMetadata 7-arg constructor with `Long checksum` parameter is reached "
                        + "here — either as a direct call or as an INVOKEDYNAMIC "
                        + "constructor-reference capture (e.g. `RecordMetadata::new` bound to "
                        + "a factory BiFunction / Function used by mock-producer scaffolding, "
                        + "parameterized-test sources, or replay tooling that synthesizes "
                        + "producer-side acknowledgment metadata). This constructor surface "
                        + "is deprecated since Kafka 2.0 (KIP-101 / KIP-82) because the v2 "
                        + "message format (KIP-98 in Kafka 0.11) moved the CRC from the "
                        + "per-record level up to the batch level — the broker computes a "
                        + "single CRC over the entire MemoryRecordsBuilder batch, and the "
                        + "per-record checksum slot in RecordMetadata is retained only for "
                        + "legacy v0/v1 decode. Three consequences for mock-producer "
                        + "scaffolding built on this constructor: (1) real producers populate "
                        + "checksum() with -1L (the documented `unknown` sentinel when the "
                        + "broker batch CRC cannot be attributed to a single record), while "
                        + "synthetic metadata built via this ctor passes whatever the test "
                        + "wrote — typically a random Long — so callback assertions on "
                        + "metadata.checksum() observe synthetic values production never "
                        + "emits; (2) the deprecated ctor's boxed `Long` type discriminates "
                        + "it from the modern fully-primitive 6-arg ctor, and a name-only "
                        + "match cannot tell them apart; (3) INVOKEDYNAMIC "
                        + "`RecordMetadata::new` captures silently bind to this overload "
                        + "whenever the factory SAM arity is 7 — the user-class bytecode "
                        + "contains zero direct INVOKESPECIAL on the legacy ctor. Migrate to "
                        + "the non-deprecated 6-arg constructor `new RecordMetadata("
                        + "topicPartition, baseOffset, (int) batchIndex, timestamp, "
                        + "serializedKeySize, serializedValueSize)` — fully primitive, no "
                        + "checksum slot, matching the shape the modern producer ack path "
                        + "surfaces.");
    }
}
