package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.ProjectContext;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Fires when the project compiles at least one class that calls
 * {@code KafkaStreams.start()} but no class anywhere in the project calls
 * {@code KafkaStreams.setUncaughtExceptionHandler(...)} (either overload —
 * the legacy {@code Thread.UncaughtExceptionHandler} or the KIP-671
 * {@code StreamsUncaughtExceptionHandler}).
 *
 * <p>Without an explicit handler, Streams falls back to the JVM's default
 * uncaught-exception handler (log to stderr) and the dead StreamThread is not
 * replaced. The KafkaStreams client only transitions to {@code ERROR} after the
 * <em>last</em> StreamThread dies; until then, the application looks healthy but
 * processes nothing on the dead thread's tasks.
 */
public final class StreamsNoUncaughtExceptionHandlerRule implements ProjectScopedRule {

    private static final String START = "start";
    private static final String START_DESC = "()V";
    private static final String SET_HANDLER = "setUncaughtExceptionHandler";

    private final Severity severity;

    public StreamsNoUncaughtExceptionHandlerRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_NO_UNCAUGHT_EXCEPTION_HANDLER;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Path classesDir = ctx.classesDir();
        if (classesDir == null || !Files.isDirectory(classesDir)) return List.of();

        List<StartSite> sites = new ArrayList<>();
        boolean hasHandler = false;

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
                        if (!(insn instanceof MethodInsnNode mi)) continue;
                        if (!KafkaTypes.KAFKA_STREAMS.equals(mi.owner)) continue;
                        if (START.equals(mi.name) && START_DESC.equals(mi.desc)) {
                            sites.add(new StartSite(cn.name, mn.name, currentLine));
                        } else if (SET_HANDLER.equals(mi.name)) {
                            hasHandler = true;
                        }
                    }
                }
            }
        } catch (IOException e) {
            return List.of();
        }

        if (sites.isEmpty() || hasHandler) return List.of();

        List<Violation> out = new ArrayList<>();
        for (StartSite s : sites) {
            String simpleClass = s.className.substring(s.className.lastIndexOf('/') + 1);
            out.add(new Violation(
                    RuleId.STREAMS_NO_UNCAUGHT_EXCEPTION_HANDLER, severity,
                    s.className, s.methodName, s.line,
                    "KafkaStreams.start() at " + simpleClass + "#" + s.methodName
                            + " but KafkaStreams.setUncaughtExceptionHandler(...) is never called anywhere "
                            + "in this project — a StreamThread that dies on an unrecoverable exception is "
                            + "not replaced, and the KafkaStreams client only transitions to ERROR after "
                            + "the LAST thread dies. The app keeps running, holds its consumer-group "
                            + "membership, and starves the cluster from rebalancing. Register a "
                            + "`StreamsUncaughtExceptionHandler` (KIP-671) and pick REPLACE_THREAD / "
                            + "SHUTDOWN_CLIENT / SHUTDOWN_APPLICATION before calling start()."));
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
