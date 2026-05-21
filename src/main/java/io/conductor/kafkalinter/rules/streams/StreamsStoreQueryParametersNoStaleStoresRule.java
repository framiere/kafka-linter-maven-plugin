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
 * Fires when a method invokes
 * {@code StoreQueryParameters.fromNameAndType(name, type)} but never calls
 * {@code .enableStaleStores()} in the same method.
 *
 * <p>The default {@code StoreQueryParameters} returned by
 * {@code fromNameAndType} has {@code staleStores=false}, which means any IQ
 * lookup against a store whose owning task is REBALANCING / RESTORING throws
 * {@code InvalidStateStoreException}. Configuring {@code num.standby.replicas}
 * is moot if the IQ router cannot fall over to a standby copy during restore —
 * which is the only window in which standbys matter.
 */
public final class StreamsStoreQueryParametersNoStaleStoresRule implements Rule {

    private static final String FROM_NAME_AND_TYPE = "fromNameAndType";
    private static final String ENABLE_STALE_STORES = "enableStaleStores";

    private final Severity severity;

    public StreamsStoreQueryParametersNoStaleStoresRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_STORE_QUERY_PARAMETERS_NO_STALE_STORES;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            scanMethod(out, ctx, mn);
        }
        return out;
    }

    private void scanMethod(List<Violation> out, RuleContext ctx, MethodNode mn) {
        List<AbstractInsnNode> fromCalls = new ArrayList<>();
        boolean hasEnableStaleStores = false;

        for (AbstractInsnNode insn : mn.instructions) {
            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.STORE_QUERY_PARAMETERS.equals(mi.owner)) continue;
            if (mi.getOpcode() == Opcodes.INVOKESTATIC && FROM_NAME_AND_TYPE.equals(mi.name)) {
                fromCalls.add(insn);
            } else if (mi.getOpcode() == Opcodes.INVOKEVIRTUAL && ENABLE_STALE_STORES.equals(mi.name)) {
                hasEnableStaleStores = true;
            }
        }

        if (fromCalls.isEmpty() || hasEnableStaleStores) return;

        for (AbstractInsnNode site : fromCalls) {
            out.add(new Violation(
                    RuleId.STREAMS_STORE_QUERY_PARAMETERS_NO_STALE_STORES, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(site),
                    "StoreQueryParameters.fromNameAndType(name, type) is called here but "
                            + ".enableStaleStores() is never called in this method. The resulting "
                            + "StoreQueryParameters has staleStores=false, so every "
                            + "KafkaStreams.store(...) query throws InvalidStateStoreException while "
                            + "the active task is REBALANCING / RESTORING. Configuring "
                            + "num.standby.replicas is moot if the IQ router cannot read from the "
                            + "standby copy during restore — which is precisely the window in which "
                            + "standbys matter. Chain .enableStaleStores() onto the builder: "
                            + "streams.store(StoreQueryParameters.fromNameAndType(name, type).enableStaleStores())."));
        }
    }
}
