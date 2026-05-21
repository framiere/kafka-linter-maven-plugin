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
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Flags a {@code KafkaConsumer} method call (other than {@code wakeup()}) reachable from a
 * lambda body that is then dispatched to another thread — submitted to an
 * {@code ExecutorService}, passed to a {@code Thread} constructor, scheduled on a
 * {@code ScheduledExecutorService}, run via {@code CompletableFuture.runAsync} /
 * {@code supplyAsync}, or invoked on a {@code ForkJoinPool}.
 *
 * <p>{@code KafkaConsumer} is documented as not thread-safe. The only thread-safe method is
 * {@code wakeup()}, which is explicitly designed for cross-thread interruption — lambda
 * bodies that only call {@code wakeup()} on the consumer are NOT flagged.
 */
public final class ConsumerNotThreadSafeRule implements Rule {

    private static final Map<String, Set<String>> DISPATCH_METHODS = Map.ofEntries(
            Map.entry("java/util/concurrent/Executor", Set.of("execute")),
            Map.entry("java/util/concurrent/ExecutorService",
                    Set.of("submit", "execute", "invokeAll", "invokeAny")),
            Map.entry("java/util/concurrent/ScheduledExecutorService",
                    Set.of("schedule", "scheduleAtFixedRate", "scheduleWithFixedDelay")),
            Map.entry("java/util/concurrent/ForkJoinPool",
                    Set.of("submit", "execute", "invoke")),
            Map.entry("java/util/concurrent/CompletableFuture",
                    Set.of("runAsync", "supplyAsync", "thenRunAsync", "thenApplyAsync", "thenAcceptAsync")),
            Map.entry("java/lang/Thread", Set.of("<init>")));

    private final Severity severity;

    public ConsumerNotThreadSafeRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_NOT_THREAD_SAFE;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        ClassNode cn = ctx.classNode();
        Map<String, MethodNode> methodsByName = indexMethods(cn);
        Set<String> consumerTouchingLambdas = findConsumerTouchingLambdas(cn, methodsByName);
        if (consumerTouchingLambdas.isEmpty()) return List.of();

        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : cn.methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof InvokeDynamicInsnNode indy)) continue;
                Handle impl = findImplHandle(indy);
                if (impl == null) continue;
                if (!impl.getOwner().equals(cn.name)) continue;
                if (!consumerTouchingLambdas.contains(impl.getName())) continue;

                AbstractInsnNode next = AsmUtil.nextSignificant(indy);
                if (!(next instanceof MethodInsnNode dispatch)) continue;
                Set<String> names = DISPATCH_METHODS.get(dispatch.owner);
                if (names == null || !names.contains(dispatch.name)) continue;

                out.add(new Violation(
                        RuleId.CONSUMER_NOT_THREAD_SAFE, severity,
                        cn.name, mn.name, AsmUtil.lineOf(indy),
                        "KafkaConsumer is captured by a lambda dispatched to another thread via "
                                + dispatch.owner.substring(dispatch.owner.lastIndexOf('/') + 1)
                                + "#" + dispatch.name + " — KafkaConsumer is not thread-safe (only "
                                + "wakeup() is). The first concurrent call from a worker thread "
                                + "throws ConcurrentModificationException: 'KafkaConsumer is not "
                                + "safe for multi-threaded access'. Keep the consumer on the poll "
                                + "thread; offload only the per-record processing to workers and "
                                + "commit on the poll thread after the batch."));
            }
        }
        return out;
    }

    private static Map<String, MethodNode> indexMethods(ClassNode cn) {
        Map<String, MethodNode> map = new HashMap<>();
        for (MethodNode mn : cn.methods) {
            map.put(mn.name, mn);
        }
        return map;
    }

    private static Set<String> findConsumerTouchingLambdas(ClassNode cn,
                                                           Map<String, MethodNode> methodsByName) {
        Set<String> result = new HashSet<>();
        for (MethodNode mn : cn.methods) {
            if (!mn.name.startsWith("lambda$")) continue;
            if (!lambdaTouchesConsumerNonWakeup(mn)) continue;
            result.add(mn.name);
        }
        return result;
    }

    private static boolean lambdaTouchesConsumerNonWakeup(MethodNode mn) {
        for (AbstractInsnNode insn : mn.instructions) {
            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
            if ("wakeup".equals(mi.name)) continue;
            return true;
        }
        return false;
    }

    private static Handle findImplHandle(InvokeDynamicInsnNode indy) {
        if (indy.bsmArgs == null) return null;
        for (Object arg : indy.bsmArgs) {
            if (arg instanceof Handle h) return h;
        }
        return null;
    }
}
