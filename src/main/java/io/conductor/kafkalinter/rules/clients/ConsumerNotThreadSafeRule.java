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
 *
 * <h2>Two capture shapes detected</h2>
 *
 * <p>The rule walks every {@code INVOKEDYNAMIC} in the class and inspects the
 * {@code implMethod} handle (bsmArg[1] of the metafactory call site). That handle
 * is one of two things, depending on how the user wrote the capture:
 *
 * <ol>
 *   <li><b>Lambda body</b> — {@code executor.execute(() -> consumer.commitSync())}.
 *       The compiler desugars the lambda into a synthetic {@code lambda$N$M} method
 *       on the <em>user class</em>; the indy's {@code implMethod} handle points at
 *       that synthetic. To decide if the lambda is dangerous we first build a set
 *       of "consumer-touching" synthetic methods (those that contain at least one
 *       {@code INVOKE*} on a {@code KafkaConsumer}/{@code Consumer} receiver whose
 *       name is not {@code wakeup}), then fire when the indy's impl is in that set
 *       AND the next significant instruction is a recognised dispatcher.</li>
 *   <li><b>Direct method reference</b> — {@code executor.execute(consumer::commitSync)}.
 *       No synthetic body is generated; the indy's {@code implMethod} handle points
 *       <em>directly</em> at {@code KafkaConsumer.commitSync:()V} (or
 *       {@code Consumer.commitSync:()V} for the interface receiver). The user
 *       class's bytecode contains <strong>zero {@code INVOKE*} instructions
 *       targeting that consumer method</strong>. A naive "look for lambda bodies
 *       in this class" walk emits ZERO violations on this entire pattern. To
 *       catch it, the rule also fires when the indy's impl handle owner is in
 *       {@code CONSUMER_OWNERS} and its name is not {@code wakeup} — same
 *       dispatch-site check applies. This is the canonical "I'll offload the
 *       blocking commit to a worker" anti-pattern in its terser form.</li>
 * </ol>
 *
 * <p>For both shapes the {@code wakeup} suppression is the same: the JVM API
 * contract permits {@code wakeup()} to be invoked from any thread (it's the only
 * thread-safe method, designed for cross-thread interruption — the canonical
 * shutdown idiom is {@code addShutdownHook(new Thread(consumer::wakeup))}). The
 * lambda-body path suppresses lambdas whose body invokes <em>only</em> wakeup;
 * the method-ref path suppresses when the captured method is wakeup itself.
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

        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : cn.methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof InvokeDynamicInsnNode indy)) continue;
                Handle impl = findImplHandle(indy);
                if (impl == null) continue;

                boolean lambdaBody = impl.getOwner().equals(cn.name)
                        && consumerTouchingLambdas.contains(impl.getName());
                boolean directMethodRef = KafkaTypes.CONSUMER_OWNERS.contains(impl.getOwner())
                        && !"wakeup".equals(impl.getName());
                if (!lambdaBody && !directMethodRef) continue;

                MethodInsnNode dispatch = AsmUtil.nextDispatchInvoke(indy);
                if (dispatch == null) continue;
                Set<String> names = DISPATCH_METHODS.get(dispatch.owner);
                if (names == null || !names.contains(dispatch.name)) continue;

                String dispatcher = dispatch.owner.substring(dispatch.owner.lastIndexOf('/') + 1)
                        + "#" + dispatch.name;
                String message = directMethodRef && !lambdaBody
                        ? methodRefMessage(impl, dispatcher)
                        : lambdaMessage(dispatcher);
                out.add(new Violation(
                        RuleId.CONSUMER_NOT_THREAD_SAFE, severity,
                        cn.name, mn.name, AsmUtil.lineOf(indy),
                        message));
            }
        }
        return out;
    }

    private static String lambdaMessage(String dispatcher) {
        return "KafkaConsumer is captured by a lambda dispatched to another thread via "
                + dispatcher + " — KafkaConsumer is not thread-safe (only wakeup() is). "
                + "The first concurrent call from a worker thread throws "
                + "ConcurrentModificationException: 'KafkaConsumer is not safe for "
                + "multi-threaded access'. Keep the consumer on the poll thread; offload "
                + "only the per-record processing to workers and commit on the poll thread "
                + "after the batch.";
    }

    private static String methodRefMessage(Handle impl, String dispatcher) {
        return "consumer::" + impl.getName() + " is captured as a method reference and "
                + "dispatched to another thread via " + dispatcher + " — KafkaConsumer is "
                + "not thread-safe (only wakeup() is). The user-class bytecode contains no "
                + "direct INVOKE of " + impl.getName() + " on the consumer, so a "
                + "MethodInsnNode-only scan would miss this entirely, but the call still "
                + "materialises on the worker thread when the SAM adapter runs and the "
                + "first concurrent touch from that thread throws "
                + "ConcurrentModificationException: 'KafkaConsumer is not safe for "
                + "multi-threaded access'. Keep the consumer on the poll thread; the only "
                + "method reference that may cross threads is consumer::wakeup (the "
                + "canonical shutdown idiom).";
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
