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
 * Fires for every reach of {@code Stores.inMemoryKeyValueStore},
 * {@code Stores.inMemoryWindowStore}, or {@code
 * Stores.inMemorySessionStore} on {@link
 * org.apache.kafka.streams.state.Stores Stores}. Each of these
 * three static factories returns a {@code *BytesStoreSupplier}
 * backed by a {@link java.util.HashMap HashMap} (key/value) or
 * {@code TreeMap} (windowed/sessionized) that lives entirely
 * inside the JVM heap of the StreamThread task. The
 * {@code persistentKeyValueStore}, {@code
 * persistentTimestampedKeyValueStore}, {@code
 * persistentVersionedKeyValueStore}, {@code
 * persistentWindowStore}, {@code
 * persistentTimestampedWindowStore}, and {@code
 * persistentSessionStore} counterparts are RocksDB-backed and
 * are the right default for production — they are intentionally
 * not flagged.
 *
 * <p>Catches both direct {@code INVOKESTATIC} calls and {@code
 * INVOKEDYNAMIC} method-reference captures (e.g. {@code
 * Stores::inMemoryKeyValueStore} bound to {@code
 * Function<String, KeyValueBytesStoreSupplier>}).
 *
 * <h2>Why an in-memory state store is a multi-failure-mode
 * antipattern in any production Streams application that
 * survives restart, scale-out, or partition reassignment</h2>
 *
 * <p>A Kafka Streams state store is the per-task local copy of
 * the data needed for stateful operations (aggregations, joins,
 * windowed counts, etc.). The DSL guarantees the local store
 * is always in sync with the partition's logical view by
 * shadowing every write to an INTERNAL "changelog" topic on the
 * broker — the changelog is the source of truth, the local
 * store is the read-side cache. When a StreamThread starts,
 * stops, fails over, scales out, or is reassigned by the group
 * coordinator, the local store has to be brought into sync
 * with the changelog before processing can resume — this is
 * the "state-store restore" phase.
 *
 * <p>For a {@link
 * org.apache.kafka.streams.state.Stores#persistentKeyValueStore
 * persistentKeyValueStore} (RocksDB), the local store survives
 * process restart and the restore is incremental — Streams
 * tracks the last offset it consumed from the changelog,
 * resumes from there on restart, and only the records produced
 * AFTER that offset need to be replayed (typically seconds-to-
 * minutes on a normal rolling deploy).
 *
 * <p>For an {@link
 * org.apache.kafka.streams.state.Stores#inMemoryKeyValueStore
 * inMemoryKeyValueStore} (HashMap), the local store is GONE
 * the moment the JVM exits — the HashMap was on heap, no on-
 * disk artifact exists, and no checkpoint file is written.
 * On every restart Streams MUST re-read the ENTIRE changelog
 * topic from offset 0 to rebuild the HashMap. For a non-trivial
 * state (say 50 GB of compacted state, with the topology's
 * application id partitioned across 32 partitions, ~1.6 GB per
 * partition on average), this restore takes minutes-to-hours
 * depending on broker fetch throughput and the application's
 * available CPU; during the restore the partition is in the
 * RESTORING state and the application produces NO output for
 * those partitions.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Every restart (deploy, OOM kill, kubelet eviction,
 *       node failure) triggers full changelog replay — minutes
 *       of zero throughput per affected partition.</b> The
 *       rolling deploy that takes 30 s of unavailability with
 *       persistent stores takes 30 MINUTES with in-memory
 *       stores for the same state size. Multiply by the number
 *       of pod replacements in a normal Kubernetes operation
 *       (deploys, scale-up, scale-down, node drains, kernel
 *       reboots) and the application spends a significant
 *       fraction of every week in RESTORING.</li>
 *   <li><b>Cooperative-rebalance scale-out moves partition
 *       ownership AND wipes the new owner's store.</b> When a
 *       new StreamThread is added (scale-out from 4 pods to 8),
 *       the partitions reassigned to the new pods all start in
 *       RESTORING — the new pods have empty HashMaps. The
 *       moved-from pod still has the in-memory state but the
 *       partition no longer routes to it; the moved-to pod has
 *       a partition to process but no state to process it
 *       against. Scale-out becomes a multi-minute outage on
 *       every horizontal-scale operation, exactly when the
 *       application is most likely to be under load.</li>
 *   <li><b>JVM heap pressure — the entire state set must fit
 *       in heap, and every record costs ~80 bytes of HashMap
 *       overhead.</b> A 50 GB compacted changelog with 200 M
 *       keys requires AT LEAST 16 GB of additional heap just
 *       for the HashMap Node[] overhead, on top of the actual
 *       key/value bytes (~50 GB more). A 64 GB heap is now 80%
 *       full just from one state store, leaving no room for
 *       the rest of the topology's working set, processor
 *       buffers, repartition buffers, etc. GC behavior degrades
 *       to multi-second pauses; the StreamThread heartbeat
 *       misses, the group coordinator considers it dead and
 *       rebalances the partitions, triggering further restore.
 *       For a persistent store the same data costs ~50 GB of
 *       RocksDB block-cache + disk, with the OS page cache
 *       doing the heavy lifting and no heap pressure.</li>
 *   <li><b>{@code num.standby.replicas} cannot save you — the
 *       standby store is also in-memory.</b> A team configuring
 *       {@code num.standby.replicas=1} hopes to mask restore
 *       latency by having a warm standby ready to take over.
 *       With persistent stores this works (the standby has a
 *       RocksDB on disk continuously caught up to the
 *       changelog); with in-memory stores the standby has its
 *       own HashMap in its own JVM, and an OOM that takes out
 *       the active also threatens the standby's JVM on the
 *       same node-class. Worse, the standby was provisioned
 *       assuming heap-for-state — doubling the heap budget
 *       just to support warm standbys is rarely budgeted for.</li>
 *   <li><b>Window store and session store in-memory variants
 *       have the same failure mode plus retention scan
 *       amplification.</b> {@code inMemoryWindowStore} keeps
 *       all windowed keys in a TreeMap indexed by window start
 *       time — on restart, the entire windowed changelog must
 *       be replayed and the TreeMap reconstructed in window-
 *       time order. Restore time scales with both the number
 *       of distinct keys AND the retention period in windows.
 *       A 24-hour retention window store with 1-minute window
 *       size and 1 M distinct keys must materialize 1.44 B
 *       window-key entries before any processing can resume.</li>
 *   <li><b>RocksDB tuning is one config away — heap and disk
 *       are NOT.</b> Teams that hit performance issues with
 *       {@code persistentKeyValueStore} can tune RocksDB via
 *       {@code RocksDBConfigSetter} (block cache size, write
 *       buffer count, compaction style, compression). Teams
 *       that hit performance issues with {@code
 *       inMemoryKeyValueStore} have no comparable lever — the
 *       HashMap is hard-coded, the only fix is to migrate
 *       away from the in-memory store, which is a full
 *       application state rebuild from changelog.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       store-supplier factory built as {@code
 *       Function<String, KeyValueBytesStoreSupplier> f =
 *       Stores::inMemoryKeyValueStore} captures an {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeStatic} Handle on {@code
 *       Stores.inMemoryKeyValueStore(String)
 *       KeyValueBytesStoreSupplier}. The user-class bytecode
 *       contains zero direct {@code INVOKESTATIC} on the
 *       in-memory factory, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: use the {@code persistent*} counterpart —
 * {@code Stores.persistentKeyValueStore(name)},
 * {@code Stores.persistentWindowStore(name, retentionPeriod,
 * windowSize, retainDuplicates)},
 * {@code Stores.persistentSessionStore(name, retentionPeriod)}.
 * RocksDB's on-disk format is the right default for any
 * production application with state that does not fit
 * comfortably in heap or that needs to survive restart. The
 * in-memory variants are appropriate only for tests, demos,
 * and applications with truly ephemeral state (e.g. a join-on-
 * stream of short-lived event correlations with seconds-to-
 * minutes retention).
 */
public final class StreamsInMemoryKvStoreRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.STREAMS_STORES);
    private static final Set<String> METHOD_NAMES = Set.of(
            "inMemoryKeyValueStore",
            "inMemoryWindowStore",
            "inMemorySessionStore");

    private final Severity severity;

    public StreamsInMemoryKvStoreRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_IN_MEMORY_KV_STORE;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAMES.contains(mi.name)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    for (String name : METHOD_NAMES) {
                        Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, name, null);
                        if (h != null) {
                            out.add(violation(ctx, mn, insn));
                            break;
                        }
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_IN_MEMORY_KV_STORE, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Stores.inMemoryKeyValueStore / "
                        + "inMemoryWindowStore / inMemorySessionStore "
                        + "is reached here — either as a direct "
                        + "INVOKESTATIC or as an INVOKEDYNAMIC "
                        + "method-reference capture (e.g. "
                        + "`Stores::inMemoryKeyValueStore` bound to "
                        + "Function<String, KeyValueBytesStoreSupplier> "
                        + "whose erased implMethod descriptor matches "
                        + "the static factory). Each of these three "
                        + "static factories returns a *BytesStoreSupplier "
                        + "backed by a HashMap (key/value) or TreeMap "
                        + "(windowed/sessionized) that lives entirely "
                        + "inside the JVM heap of the StreamThread "
                        + "task. A Kafka Streams state store is the "
                        + "per-task local copy of the data needed for "
                        + "stateful operations; the DSL guarantees the "
                        + "local store is always in sync with the "
                        + "partition's logical view by shadowing every "
                        + "write to an INTERNAL changelog topic on the "
                        + "broker — the changelog is the source of "
                        + "truth, the local store is the read-side "
                        + "cache. When a StreamThread starts, stops, "
                        + "fails over, scales out, or is reassigned, "
                        + "the local store has to be brought into sync "
                        + "with the changelog before processing can "
                        + "resume. For a persistentKeyValueStore "
                        + "(RocksDB) the local store survives process "
                        + "restart and the restore is incremental — "
                        + "Streams tracks the last offset consumed "
                        + "from the changelog, resumes from there on "
                        + "restart, and only the records produced "
                        + "AFTER that offset need to be replayed "
                        + "(typically seconds-to-minutes on a normal "
                        + "rolling deploy). For an "
                        + "inMemoryKeyValueStore (HashMap) the local "
                        + "store is GONE the moment the JVM exits — "
                        + "the HashMap was on heap, no on-disk "
                        + "artifact exists, and no checkpoint file is "
                        + "written; on every restart Streams MUST re-"
                        + "read the ENTIRE changelog topic from offset "
                        + "0 to rebuild the HashMap; for a non-trivial "
                        + "state (say 50 GB of compacted state across "
                        + "32 partitions) this restore takes minutes-"
                        + "to-hours depending on broker fetch "
                        + "throughput; during the restore the "
                        + "partition is in RESTORING and the "
                        + "application produces NO output for those "
                        + "partitions. Concrete failure modes: (1) "
                        + "every restart (deploy, OOM kill, kubelet "
                        + "eviction, node failure) triggers full "
                        + "changelog replay — minutes of zero "
                        + "throughput per affected partition; the "
                        + "rolling deploy that takes 30 s of "
                        + "unavailability with persistent stores "
                        + "takes 30 MINUTES with in-memory stores "
                        + "for the same state size; multiply by the "
                        + "number of pod replacements in a normal "
                        + "Kubernetes operation (deploys, scale-up, "
                        + "scale-down, node drains, kernel reboots) "
                        + "and the application spends a significant "
                        + "fraction of every week in RESTORING; (2) "
                        + "cooperative-rebalance scale-out moves "
                        + "partition ownership AND wipes the new "
                        + "owner's store — when a new StreamThread is "
                        + "added (scale-out from 4 pods to 8) the "
                        + "partitions reassigned to the new pods all "
                        + "start in RESTORING; the new pods have "
                        + "empty HashMaps; the moved-from pod still "
                        + "has the in-memory state but the partition "
                        + "no longer routes to it; the moved-to pod "
                        + "has a partition to process but no state "
                        + "to process it against; scale-out becomes "
                        + "a multi-minute outage on every horizontal-"
                        + "scale operation, exactly when the "
                        + "application is most likely to be under "
                        + "load; (3) JVM heap pressure — the entire "
                        + "state set must fit in heap, every record "
                        + "costs ~80 bytes of HashMap overhead; a 50 "
                        + "GB compacted changelog with 200 M keys "
                        + "requires AT LEAST 16 GB of additional "
                        + "heap just for the HashMap Node[] overhead "
                        + "on top of the actual key/value bytes "
                        + "(~50 GB more); a 64 GB heap is now 80% "
                        + "full just from one state store leaving "
                        + "no room for the rest of the topology's "
                        + "working set, processor buffers, "
                        + "repartition buffers; GC behavior "
                        + "degrades to multi-second pauses, the "
                        + "StreamThread heartbeat misses, the group "
                        + "coordinator considers it dead and "
                        + "rebalances triggering further restore; "
                        + "for a persistent store the same data "
                        + "costs ~50 GB of RocksDB block-cache + "
                        + "disk, with the OS page cache doing the "
                        + "heavy lifting and no heap pressure; (4) "
                        + "num.standby.replicas cannot save you — "
                        + "the standby store is also in-memory; a "
                        + "team configuring num.standby.replicas=1 "
                        + "hopes to mask restore latency by having "
                        + "a warm standby ready to take over; with "
                        + "persistent stores this works (the standby "
                        + "has a RocksDB on disk continuously caught "
                        + "up to the changelog); with in-memory "
                        + "stores the standby has its own HashMap in "
                        + "its own JVM and an OOM that takes out the "
                        + "active also threatens the standby's JVM "
                        + "on the same node-class; worse the standby "
                        + "was provisioned assuming heap-for-state, "
                        + "doubling the heap budget just to support "
                        + "warm standbys is rarely budgeted for; (5) "
                        + "window store and session store in-memory "
                        + "variants have the same failure mode plus "
                        + "retention scan amplification — "
                        + "inMemoryWindowStore keeps all windowed "
                        + "keys in a TreeMap indexed by window start "
                        + "time, on restart the entire windowed "
                        + "changelog must be replayed and the "
                        + "TreeMap reconstructed in window-time "
                        + "order; restore time scales with both the "
                        + "number of distinct keys AND the retention "
                        + "period in windows; a 24-hour retention "
                        + "window store with 1-minute window size "
                        + "and 1 M distinct keys must materialize "
                        + "1.44 B window-key entries before any "
                        + "processing can resume; (6) RocksDB "
                        + "tuning is one config away, heap and disk "
                        + "are NOT — teams that hit performance "
                        + "issues with persistentKeyValueStore can "
                        + "tune RocksDB via RocksDBConfigSetter "
                        + "(block cache size, write buffer count, "
                        + "compaction style, compression); teams "
                        + "that hit performance issues with "
                        + "inMemoryKeyValueStore have no comparable "
                        + "lever, the HashMap is hard-coded, the "
                        + "only fix is to migrate away which is a "
                        + "full application state rebuild from "
                        + "changelog; (7) INVOKEDYNAMIC method-"
                        + "reference captures bypass naive "
                        + "MethodInsnNode-only lint — "
                        + "`Function<String, KeyValueBytesStoreSupplier"
                        + "> f = Stores::inMemoryKeyValueStore` "
                        + "captures an INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeStatic Handle on "
                        + "Stores.inMemoryKeyValueStore(String)"
                        + "KeyValueBytesStoreSupplier; the user-class "
                        + "bytecode contains zero direct "
                        + "INVOKESTATIC on the in-memory factory, "
                        + "only the indy site. Migration: use the "
                        + "persistent* counterpart — Stores."
                        + "persistentKeyValueStore(name), Stores."
                        + "persistentWindowStore(name, "
                        + "retentionPeriod, windowSize, "
                        + "retainDuplicates), Stores."
                        + "persistentSessionStore(name, "
                        + "retentionPeriod). RocksDB's on-disk "
                        + "format is the right default for any "
                        + "production application with state that "
                        + "does not fit comfortably in heap or that "
                        + "needs to survive restart. The in-memory "
                        + "variants are appropriate only for tests, "
                        + "demos, and applications with truly "
                        + "ephemeral state.");
    }
}
