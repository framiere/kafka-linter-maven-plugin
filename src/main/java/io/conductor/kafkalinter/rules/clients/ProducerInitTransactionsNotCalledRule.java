package io.conductor.kafkalinter.rules.clients;

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
import java.util.Set;
import java.util.stream.Stream;

/**
 * Fires when the project compiles at least one class that calls a transactional
 * lifecycle method ({@code beginTransaction}, {@code commitTransaction},
 * {@code abortTransaction}, {@code sendOffsetsToTransaction}) but no class anywhere
 * in the project calls {@code Producer.initTransactions()}. The producer's
 * {@code TransactionManager} starts in {@code UNINITIALIZED}; every transactional
 * method throws {@code KafkaException} until {@code initTransactions()} promotes
 * it to {@code READY}.
 */
public final class ProducerInitTransactionsNotCalledRule implements ProjectScopedRule {

    private static final String INIT_TRANSACTIONS = "initTransactions";
    private static final String INIT_TRANSACTIONS_DESC = "()V";
    private static final Set<String> TXN_LIFECYCLE_METHODS = Set.of(
            "beginTransaction", "commitTransaction", "abortTransaction", "sendOffsetsToTransaction");

    private final Severity severity;

    public ProducerInitTransactionsNotCalledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_INIT_TRANSACTIONS_NOT_CALLED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Path classesDir = ctx.classesDir();
        if (classesDir == null || !Files.isDirectory(classesDir)) return List.of();

        List<TxnSite> sites = new ArrayList<>();
        boolean hasInit = false;

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
                        if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;
                        if (INIT_TRANSACTIONS.equals(mi.name) && INIT_TRANSACTIONS_DESC.equals(mi.desc)) {
                            hasInit = true;
                        } else if (TXN_LIFECYCLE_METHODS.contains(mi.name)) {
                            sites.add(new TxnSite(cn.name, mn.name, currentLine, mi.name));
                        }
                    }
                }
            }
        } catch (IOException e) {
            return List.of();
        }

        if (sites.isEmpty() || hasInit) return List.of();

        List<Violation> out = new ArrayList<>();
        for (TxnSite s : sites) {
            String simpleClass = s.className.substring(s.className.lastIndexOf('/') + 1);
            out.add(new Violation(
                    RuleId.PRODUCER_INIT_TRANSACTIONS_NOT_CALLED, severity,
                    s.className, s.methodName, s.line,
                    "Producer." + s.calledMethod + "() at " + simpleClass + "#" + s.methodName
                            + " but Producer.initTransactions() is not called anywhere in this project — "
                            + "the producer's TransactionManager is UNINITIALIZED, every transactional method throws "
                            + "`KafkaException: Cannot perform '<op>' before transactions have been initialized.` "
                            + "Call `producer.initTransactions()` exactly once, immediately after constructing the producer."));
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

    private record TxnSite(String className, String methodName, int line, String calledMethod) {}
}
