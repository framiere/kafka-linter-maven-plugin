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
 * {@code KafkaStreams.setStateListener(...)}.
 *
 * <p>Without a state listener, the application has no in-process hook to react
 * when Streams transitions to {@code ERROR} / {@code NOT_RUNNING} /
 * {@code PENDING_SHUTDOWN}. The JVM keeps running while the Streams client is
 * dead — holding consumer-group membership and starving the cluster from
 * rebalancing to a healthy peer.
 */
public final class StreamsNoStateListenerRule implements ProjectScopedRule {

    private static final String START = "start";
    private static final String START_DESC = "()V";
    private static final String SET_STATE_LISTENER = "setStateListener";

    private final Severity severity;

    public StreamsNoStateListenerRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_NO_STATE_LISTENER;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Path classesDir = ctx.classesDir();
        if (classesDir == null || !Files.isDirectory(classesDir)) return List.of();

        List<StartSite> sites = new ArrayList<>();
        boolean hasListener = false;

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
                        } else if (SET_STATE_LISTENER.equals(mi.name)) {
                            hasListener = true;
                        }
                    }
                }
            }
        } catch (IOException e) {
            return List.of();
        }

        if (sites.isEmpty() || hasListener) return List.of();

        List<Violation> out = new ArrayList<>();
        for (StartSite s : sites) {
            String simpleClass = s.className.substring(s.className.lastIndexOf('/') + 1);
            out.add(new Violation(
                    RuleId.STREAMS_NO_STATE_LISTENER, severity,
                    s.className, s.methodName, s.line,
                    "KafkaStreams.start() at " + simpleClass + "#" + s.methodName
                            + " but KafkaStreams.setStateListener(...) is never called anywhere "
                            + "in this project — the application has no in-process hook to observe "
                            + "ERROR / NOT_RUNNING / PENDING_SHUTDOWN transitions. When the Streams "
                            + "client enters ERROR, the JVM keeps running, holds its consumer-group "
                            + "membership, and starves the cluster from rebalancing the partitions to "
                            + "a healthy peer. Register a `KafkaStreams.StateListener` before "
                            + "calling start() and call System.exit(1) on ERROR (canonical safe "
                            + "default — Kubernetes / systemd restarts the pod, the consumer's "
                            + "session expires, and the partitions rebalance away)."));
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
