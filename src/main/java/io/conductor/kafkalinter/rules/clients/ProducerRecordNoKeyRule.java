package io.conductor.kafkalinter.rules.clients;

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
 * Fires for every reach of the 2-arg
 * {@link org.apache.kafka.clients.producer.ProducerRecord} constructor
 * — descriptor {@code (Ljava/lang/String;Ljava/lang/Object;)V} —
 * which produces a {@link org.apache.kafka.clients.producer.ProducerRecord}
 * with a {@code null} key. Catches both direct
 * {@code INVOKESPECIAL} calls (from {@code new ProducerRecord<>(topic,
 * value)}) and indirect {@code INVOKEDYNAMIC} constructor-reference
 * captures (e.g. {@code ProducerRecord::new} bound to
 * {@link java.util.function.BiFunction BiFunction&lt;String, V,
 * ProducerRecord&lt;String, V&gt;&gt;} or a custom 2-arg SAM — the
 * indy bsm-args contain a {@code REF_newInvokeSpecial} Handle whose
 * name is {@code <init>} and descriptor matches).
 *
 * <h2>Why a null key is a correctness hazard</h2>
 *
 * <p>The {@link org.apache.kafka.clients.producer.ProducerRecord} key
 * controls three independent and load-bearing behaviors in the Kafka
 * producer:
 *
 * <ul>
 *   <li><b>Partition assignment via {@link
 *       org.apache.kafka.clients.producer.Partitioner}.</b> The
 *       default partitioner hashes the key (murmur2 of the serialized
 *       key bytes) and assigns the record to
 *       {@code hash % numPartitions}. Records with the same key land
 *       on the same partition and therefore preserve per-key
 *       ordering — the foundational invariant that makes Kafka usable
 *       as a stateful pipeline. A null key triggers the
 *       partitioner's "no key" branch which, since Kafka 2.4
 *       (KIP-480), is the <b>sticky partitioner</b>: records are
 *       batched into a single partition for the lifetime of a batch,
 *       then a new partition is picked at random for the next batch.
 *       Net effect: the records flow to partitions in batches with no
 *       relationship to record content, and per-key ordering is
 *       impossible — there is no key.</li>
 *   <li><b>Log compaction.</b> Topics configured with
 *       {@code cleanup.policy=compact} retain only the latest record
 *       for each key after compaction. A record with a null key on a
 *       compacted topic has special semantics: it is a <b>tombstone</b>
 *       that, on next compaction, deletes the previous value for the
 *       (already-null) "key" — which, since there is no key, means
 *       the record is essentially ill-defined on a compacted topic.
 *       More realistically the topic is not compacted but is meant to
 *       be queryable by key (state store, KTable source) — and a
 *       record with no key cannot be looked up, ever.</li>
 *   <li><b>Stream-table joins, KGroupedStream, KTable source
 *       semantics.</b> Every Streams operation that groups,
 *       partitions, or joins by key requires a non-null key. Kafka
 *       Streams will route null-keyed records through the
 *       partitioner's null-key branch — i.e. the sticky partitioner,
 *       which violates the co-partitioning assumption of every
 *       downstream join. Result: the join produces null on the right
 *       side for keys that should match, silently.</li>
 * </ul>
 *
 * <p>Concrete failure modes (carried verbatim into the violation
 * message):
 *
 * <ul>
 *   <li><b>Sticky partitioner skew on a compacted topic.</b> A user
 *       publishes user-update events to a topic intended to be
 *       compacted by user-id; passes {@code new ProducerRecord<>(
 *       "user-updates", userEvent)} (forgetting the userId as key).
 *       The sticky partitioner batches all events to one partition
 *       for the duration of each batch; compaction sees only null
 *       keys and either retains all duplicates (no compaction
 *       benefit) or, depending on cleanup-policy interpretation,
 *       deletes everything. Six months later the team discovers the
 *       topic has been growing unbounded.</li>
 *   <li><b>Per-user ordering silently breaks under load.</b> A
 *       payments service publishes per-account state-transition
 *       events to a topic intended to be partitioned by account-id;
 *       passes {@code new ProducerRecord<>("payments-events",
 *       eventBody)} (forgetting the accountId as key). Each event
 *       lands on a sticky-partitioner-chosen partition; for a given
 *       account, events land on different partitions across
 *       batches; downstream consumer processes the partitions in
 *       parallel; per-account ordering breaks; the account's state
 *       transitions can be applied out-of-order; idempotent state
 *       machines mask it; non-idempotent ones produce visible
 *       inconsistency (charge before authorization, refund before
 *       charge, balance going negative then positive).</li>
 *   <li><b>Stream-table join silently returns null.</b> A KStream of
 *       order events is joined to a KTable of customer state; the
 *       upstream producer of the order events passed
 *       {@code new ProducerRecord<>("orders", orderBody)} (no
 *       customerId as key). The Streams app does {@code
 *       ordersStream.join(customersTable, ...)} expecting a per-
 *       customer enrichment; the join key is null on the left side;
 *       the join produces null on the right side for every record
 *       (or, with a non-null fallback, the wrong customer). The
 *       application's metric for "matched orders" stays high
 *       (matches that should not exist are produced) and the
 *       business signal "order enrichment success rate" lies.</li>
 *   <li><b>Idempotent producer cannot deduplicate.</b> The idempotent
 *       producer (enable.idempotence=true) deduplicates retries via
 *       (producerId, epoch, sequence) — independent of key. But the
 *       higher-level "exactly-once" guarantee in Streams and in the
 *       transactional producer requires per-key state to be
 *       checkpointed, and a null key makes that checkpointing
 *       unsemantic. A retry that lands on a different partition
 *       (because the sticky partitioner picked a different one) is
 *       not a "retry" of the original record — it is a new record on
 *       a new partition.</li>
 *   <li><b>INVOKEDYNAMIC {@code ProducerRecord::new} captures bypass
 *       naïve MethodInsnNode-only lint.</b> A factory built as
 *       {@code BiFunction<String, V, ProducerRecord<String, V>>
 *       factory = ProducerRecord::new} compiles to {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_newInvokeSpecial} Handle pointing at
 *       {@code ProducerRecord.<init>(Ljava/lang/String;
 *       Ljava/lang/Object;)V}. The user-class bytecode contains zero
 *       direct {@code INVOKESPECIAL} on the 2-arg constructor — only
 *       the indy site. A rule that walks only {@code MethodInsnNode}
 *       misses every such site. Common in streaming pipelines that
 *       map a {@code List<V>} through a record factory:
 *       {@code inputs.stream().map(v -> factory.apply(topic, v))}.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — five constructor overloads, only
 * one is the key-less hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.producer.ProducerRecord}
 * declares five constructors:
 *
 * <ul>
 *   <li>Unsafe (no key): {@code (Ljava/lang/String;
 *       Ljava/lang/Object;)V} — ProducerRecord(topic, value)</li>
 *   <li>Safe: {@code (Ljava/lang/String;Ljava/lang/Object;
 *       Ljava/lang/Object;)V} — ProducerRecord(topic, key, value)</li>
 *   <li>Safe: {@code (Ljava/lang/String;Ljava/lang/Integer;
 *       Ljava/lang/Object;Ljava/lang/Object;)V} —
 *       ProducerRecord(topic, partition, key, value)</li>
 *   <li>Safe: {@code (Ljava/lang/String;Ljava/lang/Integer;
 *       Ljava/lang/Long;Ljava/lang/Object;Ljava/lang/Object;)V} —
 *       ProducerRecord(topic, partition, timestamp, key, value)</li>
 *   <li>Safe: {@code (Ljava/lang/String;Ljava/lang/Integer;
 *       Ljava/lang/Long;Ljava/lang/Object;Ljava/lang/Object;
 *       Ljava/lang/Iterable;)V} —
 *       ProducerRecord(topic, partition, timestamp, key, value,
 *       headers)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the 2-arg constructor
 * descriptor; the four key-carrying overloads have strictly different
 * signatures and are never flagged. Note: a constructor where the
 * caller explicitly passes {@code null} for the key (e.g. {@code new
 * ProducerRecord<>(topic, null, value)}) targets the 3-arg
 * constructor and is not flagged by this rule — it is the
 * <b>structural</b> absence of a key slot that this rule catches, not
 * a runtime null value in a present slot.
 */
public final class ProducerRecordNoKeyRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.PRODUCER_RECORD);
    private static final String CTOR_NAME = "<init>";
    private static final String NO_KEY_DESC = "(Ljava/lang/String;Ljava/lang/Object;)V";

    private final Severity severity;

    public ProducerRecordNoKeyRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_RECORD_NO_KEY;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && CTOR_NAME.equals(mi.name)
                        && NO_KEY_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, CTOR_NAME, NO_KEY_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.PRODUCER_RECORD_NO_KEY, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "new ProducerRecord<>(topic, value) — 2-arg constructor "
                        + "is reached here — either as a direct "
                        + "INVOKESPECIAL on the constructor or as an "
                        + "INVOKEDYNAMIC constructor-reference capture "
                        + "(e.g. `ProducerRecord::new` bound to "
                        + "BiFunction<String, V, ProducerRecord<String, "
                        + "V>> or a custom 2-arg SAM). The 2-arg "
                        + "constructor sets the key to null, which "
                        + "breaks three independent and load-bearing "
                        + "behaviors of the Kafka producer: (a) "
                        + "partition assignment — the default "
                        + "partitioner hashes the key to assign the "
                        + "record to a partition (records with the same "
                        + "key land on the same partition and preserve "
                        + "per-key ordering); a null key triggers the "
                        + "sticky partitioner (KIP-480, Kafka 2.4+) "
                        + "which batches records to a single partition "
                        + "for the lifetime of a batch then picks a new "
                        + "partition at random for the next batch — "
                        + "records flow to partitions with no "
                        + "relationship to content; per-key ordering is "
                        + "impossible because there is no key; (b) log "
                        + "compaction — topics with cleanup.policy="
                        + "compact retain only the latest record for "
                        + "each key; a null key on a compacted topic is "
                        + "a tombstone that deletes the previous value "
                        + "for the (already-null) 'key', or — more "
                        + "realistically — the topic is meant to be "
                        + "queryable by key (state store, KTable "
                        + "source) and a record with no key cannot be "
                        + "looked up; (c) stream-table joins, "
                        + "KGroupedStream, KTable source semantics — "
                        + "every Streams operation that groups, "
                        + "partitions, or joins by key requires a "
                        + "non-null key; null-keyed records violate the "
                        + "co-partitioning assumption of every "
                        + "downstream join; the join produces null on "
                        + "the right side for keys that should match, "
                        + "silently. Concrete failure modes: (1) "
                        + "sticky-partitioner skew on a compacted topic "
                        + "— user-update events published with `new "
                        + "ProducerRecord<>('user-updates', userEvent)` "
                        + "(forgetting userId as key); sticky "
                        + "partitioner batches all events to one "
                        + "partition; compaction sees only null keys; "
                        + "either retains all duplicates (no compaction "
                        + "benefit) or deletes everything; six months "
                        + "later the team discovers the topic has been "
                        + "growing unbounded; (2) per-user ordering "
                        + "silently breaks under load — payments "
                        + "service publishes per-account state-"
                        + "transition events with `new ProducerRecord<>"
                        + "('payments-events', eventBody)` (forgetting "
                        + "accountId as key); each event lands on a "
                        + "sticky-partitioner-chosen partition; for a "
                        + "given account, events land on different "
                        + "partitions across batches; downstream "
                        + "consumer processes partitions in parallel; "
                        + "per-account ordering breaks; account's state "
                        + "transitions applied out-of-order; "
                        + "non-idempotent state machines produce "
                        + "visible inconsistency (charge before "
                        + "authorization, refund before charge, balance "
                        + "going negative then positive); (3) stream-"
                        + "table join silently returns null — KStream "
                        + "of order events joined to KTable of customer "
                        + "state; upstream producer passed `new "
                        + "ProducerRecord<>('orders', orderBody)` (no "
                        + "customerId as key); join key is null on the "
                        + "left side; join produces null on the right "
                        + "side for every record (or, with a non-null "
                        + "fallback, the wrong customer); business "
                        + "signal 'order enrichment success rate' lies; "
                        + "(4) idempotent producer cannot deduplicate "
                        + "per-key — exactly-once guarantee in Streams "
                        + "and in the transactional producer requires "
                        + "per-key state to be checkpointed; a null key "
                        + "makes that checkpointing unsemantic; a retry "
                        + "that lands on a different partition (because "
                        + "the sticky partitioner picked a different "
                        + "one) is not a 'retry' of the original record "
                        + "— it is a new record on a new partition; "
                        + "(5) INVOKEDYNAMIC `ProducerRecord::new` "
                        + "captures bypass naive MethodInsnNode-only "
                        + "lint — `BiFunction<String, V, "
                        + "ProducerRecord<String, V>> factory = "
                        + "ProducerRecord::new` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_newInvokeSpecial Handle pointing at "
                        + "ProducerRecord.<init>(Ljava/lang/String;"
                        + "Ljava/lang/Object;)V; the user-class "
                        + "bytecode contains zero direct INVOKESPECIAL "
                        + "on the 2-arg constructor, only the indy "
                        + "site; common in streaming pipelines that map "
                        + "a List<V> through a record factory: "
                        + "`inputs.stream().map(v -> factory.apply("
                        + "topic, v))`. Migration: pass an explicit "
                        + "key as the second argument — use the 3-arg "
                        + "constructor `new ProducerRecord<>(topic, "
                        + "key, value)` where the key is the natural "
                        + "partitioning attribute (userId, accountId, "
                        + "orderId, tenantId, deviceId). The four "
                        + "key-carrying overloads have descriptors "
                        + "(String, Object, Object)V (3-arg), (String, "
                        + "Integer, Object, Object)V (4-arg with "
                        + "explicit partition), (String, Integer, Long, "
                        + "Object, Object)V (5-arg with timestamp), "
                        + "(String, Integer, Long, Object, Object, "
                        + "Iterable)V (6-arg with headers) and are "
                        + "never flagged by this rule. Note: a 3-arg "
                        + "constructor with `null` explicitly passed "
                        + "for the key targets a different constructor "
                        + "and is NOT flagged — the rule catches the "
                        + "structural absence of a key slot, not a "
                        + "runtime null value in a present slot.");
    }
}
