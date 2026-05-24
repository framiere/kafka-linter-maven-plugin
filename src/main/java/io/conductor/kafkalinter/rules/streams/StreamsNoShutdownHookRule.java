package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.ProjectContext;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Fires when the project compiles at least one class that calls
 * {@code KafkaStreams.start()} but no class anywhere in the project calls
 * {@code Runtime.getRuntime().addShutdownHook(Thread)} — meaning the JVM has
 * no registered hook to close Streams on SIGTERM. The kubelet's grace-period
 * SIGKILL fires while StreamThreads are still processing: RocksDB caches
 * unflushed, embedded producer accumulator undrained, EOS transaction
 * abandoned and the {@code transactional.id} fenced on the broker until
 * {@code transaction.timeout.ms} elapses.
 *
 * <h2>Method-reference capture is recognised as a register call</h2>
 *
 * <p>The hook is sometimes wired via a deferred method-ref, e.g.
 * {@code Optional.of(thread).ifPresent(Runtime.getRuntime()::addShutdownHook)}.
 * That method-ref compiles to an {@code INVOKEDYNAMIC} whose bootstrap-method args
 * include a direct {@code REF_invokeVirtual Runtime.addShutdownHook} handle. The
 * user-class bytecode contains <strong>zero {@code INVOKE*} instructions
 * targeting the method</strong>, so a MethodInsnNode-only scan would conclude
 * {@code hasHook == false} and fire a false-positive. The class walk below
 * treats any captured handle as evidence that a hook is wired.
 *
 * <h2>Detection is deliberately coarse (MEDIUM confidence)</h2>
 *
 * <p>The rule does NOT verify that the registered hook actually calls
 * {@code KafkaStreams.close()} — that would require cross-method body
 * inspection through {@code Thread} constructors and into Runnable lambda
 * bodies, multiplying the false-negative surface without meaningfully
 * improving the signal. The simpler heuristic ("any shutdown hook anywhere")
 * is conservative the right way: it accepts the false-negative that an
 * unrelated shutdown hook suppresses the rule, in exchange for zero
 * false-positives in apps that DO register a Streams-close hook.
 *
 * <p>Framework-managed deployments — Spring Boot's
 * {@code StreamsBuilderFactoryBean}, Quarkus's {@code @KafkaStreams} —
 * register the hook in framework bytecode that does not ship in
 * {@code target/classes}, so the rule will fire on apps where the framework
 * handles shutdown correctly. Operators using those frameworks should either
 * disable the rule, suppress it on the framework-bean class, or accept the
 * noise. MEDIUM confidence reflects this.
 */
public final class StreamsNoShutdownHookRule implements ProjectScopedRule {

    private static final String START = "start";
    private static final String START_DESC = "()V";
    private static final String RUNTIME = "java/lang/Runtime";
    private static final String ADD_SHUTDOWN_HOOK = "addShutdownHook";
    private static final Set<String> RUNTIME_OWNERS = Set.of(RUNTIME);

    private final Severity severity;

    public StreamsNoShutdownHookRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_NO_SHUTDOWN_HOOK;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Path classesDir = ctx.classesDir();
        if (classesDir == null || !Files.isDirectory(classesDir)) return List.of();

        List<StartSite> sites = new ArrayList<>();
        boolean hasHook = false;

        try (Stream<Path> walk = Files.walk(classesDir)) {
            for (Path cf : walk.filter(p -> p.toString().endsWith(".class")).filter(Files::isRegularFile).toList()) {
                ClassNode cn = readClass(cf);
                if (cn == null) continue;
                for (MethodNode mn : cn.methods) {
                    int currentLine = -1;
                    for (AbstractInsnNode insn : mn.instructions) {
                        if (insn instanceof LineNumberNode ln) {
                            currentLine = ln.line;
                            continue;
                        }
                        if (insn instanceof InvokeDynamicInsnNode indy
                                && AsmUtil.indyTargetHandle(indy, RUNTIME_OWNERS, ADD_SHUTDOWN_HOOK, null) != null) {
                            hasHook = true;
                            continue;
                        }
                        if (!(insn instanceof MethodInsnNode mi)) continue;
                        if (KafkaTypes.KAFKA_STREAMS.equals(mi.owner)
                                && START.equals(mi.name)
                                && START_DESC.equals(mi.desc)) {
                            sites.add(new StartSite(cn.name, mn.name, currentLine));
                        } else if (RUNTIME.equals(mi.owner) && ADD_SHUTDOWN_HOOK.equals(mi.name)) {
                            hasHook = true;
                        }
                    }
                }
            }
        } catch (IOException e) {
            return List.of();
        }

        if (sites.isEmpty() || hasHook) return List.of();

        List<Violation> out = new ArrayList<>();
        for (StartSite s : sites) {
            String simpleClass = s.className.substring(s.className.lastIndexOf('/') + 1);
            out.add(new Violation(
                    RuleId.STREAMS_NO_SHUTDOWN_HOOK, severity,
                    s.className, s.methodName, s.line,
                    "KafkaStreams.start() at " + simpleClass + "#" + s.methodName
                            + " but Runtime.getRuntime().addShutdownHook(...) is never called anywhere "
                            + "in this project — on SIGTERM, Streams gets no chance to flush RocksDB, "
                            + "commit offsets, send LeaveGroup, or close the open EOS transaction. The "
                            + "kubelet's grace-period SIGKILL fires mid-batch: in-cache RocksDB writes "
                            + "are lost (changelog restore on next start brings the store to a state the "
                            + "topology never emitted), the embedded producer's accumulator is dropped "
                            + "(silent data loss on in-flight records), and under EOS the transactional.id "
                            + "stays fenced on the broker until transaction.timeout.ms expires (default "
                            + "10 minutes for Streams) — the next deploy CrashLoopBackOffs with "
                            + "ProducerFencedException for that whole window. Register "
                            + "`Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close("
                            + "Duration.ofSeconds(30))))` and set terminationGracePeriodSeconds > the "
                            + "close-Duration, otherwise the hook is theatre."));
        }
        return out;
    }

    private static ClassNode readClass(Path cf) {
        try (InputStream in = Files.newInputStream(cf)) {
            ClassReader cr = new ClassReader(in);
            ClassNode cn = new ClassNode();
            cr.accept(cn, ClassReader.SKIP_FRAMES);
            return cn;
        } catch (IOException e) {
            return null;
        }
    }

    private record StartSite(String className, String methodName, int line) {}
}
