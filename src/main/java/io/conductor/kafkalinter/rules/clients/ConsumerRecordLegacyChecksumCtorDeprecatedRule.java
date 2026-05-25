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
 * Fires for every reach of a {@code ConsumerRecord} constructor that carries
 * a {@code long} or {@code Long checksum} parameter — whether the call lands
 * directly via {@code INVOKESPECIAL} or indirectly through an
 * {@code INVOKEDYNAMIC} constructor-reference capture (e.g.
 * {@code ConsumerRecord::new} bound to a factory functional interface used
 * by a parameterized-test source, fuzz-input generator, or mock-record
 * producer).
 *
 * <h2>Why this constructor surface is deprecated, not merely cosmetic</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.ConsumerRecord} historically
 * exposed a per-record CRC32 checksum: brokers in the v0/v1 message format
 * carried a 32-bit CRC over every individual record. Test scaffolding,
 * mock producers, and replay tools hand-rolling {@code ConsumerRecord}
 * instances filled this slot with a synthetic value (or
 * {@code DefaultRecord.computeChecksum(...)}) to make the synthetic record
 * &laquo;look real&raquo; to downstream code that inspected the field.
 *
 * <p>KIP-98 (Kafka 0.11, the v2 message format) moved the CRC from the
 * per-record level up to the batch level — the broker now computes a
 * single CRC over the entire {@code MemoryRecordsBuilder} batch and the
 * per-record checksum slot was retained only for legacy decode of v0/v1
 * batches. KIP-101 / KIP-82 then formally deprecated the
 * {@code ConsumerRecord} constructors that surface this slot. Three
 * consequences follow:
 *
 * <ul>
 *   <li><b>The checksum value carries no useful information.</b> Any
 *       value passed to the deprecated constructors is either discarded
 *       (modern broker decode) or treated as opaque legacy metadata. Test
 *       assertions or audit logs that read {@code record.checksum()} from
 *       a record built via the legacy constructor observe whatever the
 *       test wrote, not what the broker would have computed — false
 *       positives on integrity checks are inevitable.</li>
 *   <li><b>The deprecated constructors are missing modern parameters.</b>
 *       The non-deprecated modern constructor takes
 *       {@code Optional<Integer> leaderEpoch} (KIP-320 — the broker
 *       epoch that wrote the record, used by KIP-360 consumer-side log
 *       truncation detection) and {@code Headers} (KIP-82 — record
 *       headers for tracing, schema-id propagation, transactional
 *       metadata). Test fixtures built on the deprecated constructors
 *       silently set {@code leaderEpoch=Optional.empty()} and
 *       {@code headers=new RecordHeaders()} — any downstream
 *       leader-epoch-aware reset or header-driven router behaves
 *       differently against a legacy-constructed record than against a
 *       broker-produced one. Tests pass; production fails.</li>
 *   <li><b>Test refactors that introduce {@code ConsumerRecord::new}
 *       method references silently bind to the legacy constructor
 *       whenever the factory functional-interface arity matches.</b>
 *       A {@code BiFunction<Long, Long, ConsumerRecord<K,V>>} test
 *       harness that captures {@code (offset, checksum) -> record} will
 *       resolve to the legacy 10-arg constructor by descriptor erasure
 *       and produce records that drift further from production shape
 *       with every Kafka version bump.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>The non-deprecated 11-arg constructor takes
 * {@code (topic, partition, offset, timestamp, TimestampType, serializedKeySize,
 * serializedValueSize, key, value, Headers, Optional<Integer> leaderEpoch)} —
 * no checksum parameter, but with both KIP-82 headers and KIP-320 leader
 * epoch. For most test scaffolding the 5-arg shortcut
 * {@code (topic, partition, offset, key, value)} is sufficient.
 *
 * <h2>Constructor-reference capture path</h2>
 *
 * <p>{@code ConsumerRecord::new} bound to a factory functional interface
 * compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_newInvokeSpecial} handle pointing at the resolved
 * constructor. The user-class bytecode contains zero direct
 * {@code INVOKESPECIAL} on the legacy constructor — the call lives only
 * in the {@code LambdaMetafactory}-synthesized bridge, and a name-only
 * MethodInsnNode walk would miss it.
 *
 * <h2>Descriptor discrimination is mandatory</h2>
 *
 * <p>{@code <init>} is overloaded on {@code ConsumerRecord} with at least
 * five shapes in current Kafka clients:
 *
 * <ul>
 *   <li>Legacy with primitive {@code long} checksum: descriptor contains
 *       {@code Lorg/apache/kafka/common/record/TimestampType;J} — i.e.
 *       {@code TimestampType} followed by primitive {@code J} (long).</li>
 *   <li>Legacy with boxed {@code Long} checksum: descriptor contains
 *       {@code Lorg/apache/kafka/common/record/TimestampType;Ljava/lang/Long;}
 *       — the same slot widened to {@code Long} to express
 *       {@code null = absent} when refactors started removing the field.</li>
 *   <li>Modern 11-arg without checksum: descriptor begins
 *       {@code (...TimestampType;II...} — {@code TimestampType} followed
 *       by primitive {@code I} (keySize), with no Long/long between.</li>
 *   <li>5-arg shortcut: descriptor does not contain
 *       {@code TimestampType} at all.</li>
 * </ul>
 *
 * <p>Matching on the {@code (owner, name, desc)} triple with the two
 * checksum-bearing descriptor fragments above leaves the modern
 * constructors untouched. The substring match on the fragment after
 * {@code TimestampType;} is sufficient because no other parameter in any
 * {@code ConsumerRecord} constructor descriptor is a primitive long or a
 * boxed {@code java.lang.Long} immediately following {@code TimestampType}
 * — the prefix uniquely identifies the deprecated shape.
 */
public final class ConsumerRecordLegacyChecksumCtorDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.CONSUMER_RECORD);
    private static final String CTOR_NAME = "<init>";
    private static final String LEGACY_LONG_FRAGMENT =
            "Lorg/apache/kafka/common/record/TimestampType;J";
    private static final String LEGACY_BOXED_FRAGMENT =
            "Lorg/apache/kafka/common/record/TimestampType;Ljava/lang/Long;";

    private final Severity severity;

    public ConsumerRecordLegacyChecksumCtorDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_RECORD_LEGACY_CHECKSUM_CTOR_DEPRECATED;
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
        return desc != null
                && (desc.contains(LEGACY_LONG_FRAGMENT) || desc.contains(LEGACY_BOXED_FRAGMENT));
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.CONSUMER_RECORD_LEGACY_CHECKSUM_CTOR_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "ConsumerRecord constructor with `long` or `Long checksum` parameter is "
                        + "reached here — either as a direct call or as an INVOKEDYNAMIC "
                        + "constructor-reference capture (e.g. `ConsumerRecord::new` bound "
                        + "to a factory BiFunction / Function used by a parameterized-test "
                        + "source, fuzz-input generator, or mock-record producer). This "
                        + "constructor surface is deprecated since Kafka 2.0 (KIP-101 / "
                        + "KIP-82) because the per-record CRC32 slot carries no useful "
                        + "information after KIP-98 (Kafka 0.11, v2 message format) moved "
                        + "the CRC to the batch level — the broker computes a single CRC "
                        + "over the entire MemoryRecordsBuilder batch, and the per-record "
                        + "checksum field is retained only for legacy v0/v1 decode. Three "
                        + "consequences for test scaffolding built on this constructor: "
                        + "(1) any value passed is either discarded by modern decode or "
                        + "treated as opaque legacy metadata, so test assertions that read "
                        + "record.checksum() observe what the test wrote, not what the "
                        + "broker would compute; (2) the deprecated constructors are "
                        + "missing modern parameters — KIP-320 `Optional<Integer> "
                        + "leaderEpoch` (consumer-side log-truncation detection) and "
                        + "KIP-82 `Headers` (tracing, schema-id propagation, transactional "
                        + "metadata) — so fixtures silently set leaderEpoch=empty and "
                        + "headers=new RecordHeaders(), and any downstream leader-epoch "
                        + "reset or header-driven router behaves differently against the "
                        + "synthetic record than against a broker-produced one; (3) "
                        + "INVOKEDYNAMIC `ConsumerRecord::new` captures resolve by "
                        + "descriptor erasure and silently bind to the legacy constructor "
                        + "whenever a factory functional-interface arity matches. Migrate "
                        + "to the non-deprecated 11-arg constructor `new ConsumerRecord("
                        + "topic, partition, offset, timestamp, TimestampType.CREATE_TIME, "
                        + "keySize, valueSize, key, value, new RecordHeaders(), "
                        + "Optional.empty())` or the 5-arg shortcut `new ConsumerRecord("
                        + "topic, partition, offset, key, value)` — both omit the "
                        + "informationless checksum slot and surface the modern "
                        + "leaderEpoch / Headers shape.");
    }
}
