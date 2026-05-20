package io.conductor.kafkalinter.rules.quarkus;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Flags a SmallRye Reactive Messaging {@code @Incoming} method that does obviously
 * blocking work (e.g. {@code Thread.sleep}, JDBC, JPA, Apache HttpClient) but is not
 * marked {@code @Blocking}.
 *
 * <p>Incoming dispatches on Vert.x event-loop threads by default. Any blocking call on
 * that thread stalls the entire event loop — every channel, HTTP endpoint and timer in
 * the app goes silent until the call returns. The fix is {@code @Blocking} (or
 * {@code @Blocking("pool-name")}), which dispatches to a worker thread instead.
 *
 * <p>Detection is a heuristic: we scan the method body for calls into a small allow-list
 * of "obviously blocking" packages. False positives are possible when a wrapper hides
 * the blocking call.
 */
public final class QkBlockingMissingOnIncomingRule implements Rule {

    private static final String INCOMING_ANNOT = "Lorg/eclipse/microprofile/reactive/messaging/Incoming;";
    private static final String BLOCKING_ANNOT_SMALLRYE = "Lio/smallrye/reactive/messaging/annotations/Blocking;";

    private static final Set<String> BLOCKING_OWNER_PREFIXES = Set.of(
            "java/lang/Thread",
            "java/sql/",
            "javax/persistence/",
            "jakarta/persistence/",
            "org/apache/http/",
            "org/apache/hc/",
            "org/springframework/web/client/RestTemplate",
            "java/net/HttpURLConnection"
    );

    private final Severity severity;

    public QkBlockingMissingOnIncomingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.QK_BLOCKING_MISSING_ON_BLOCKING_LISTENER;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            if (!hasAnnotation(mn, INCOMING_ANNOT)) continue;
            if (hasAnnotation(mn, BLOCKING_ANNOT_SMALLRYE)) continue;

            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!ownerLooksBlocking(mi.owner)) continue;
                if ("java/lang/Thread".equals(mi.owner) && !"sleep".equals(mi.name)) continue;
                out.add(new Violation(
                        RuleId.QK_BLOCKING_MISSING_ON_BLOCKING_LISTENER, severity,
                        ctx.classNode().name, mn.name, io.conductor.kafkalinter.scanner.AsmUtil.lineOf(insn),
                        "@Incoming method does blocking work (" + mi.owner + "." + mi.name
                                + ") but is not annotated @Blocking — will stall the Vert.x event loop."));
                break;  // one finding per method is plenty
            }
        }
        return out;
    }

    private static boolean ownerLooksBlocking(String owner) {
        for (String prefix : BLOCKING_OWNER_PREFIXES) {
            if (owner.equals(prefix) || owner.startsWith(prefix)) return true;
        }
        return false;
    }

    private static boolean hasAnnotation(MethodNode mn, String desc) {
        return contains(mn.visibleAnnotations, desc) || contains(mn.invisibleAnnotations, desc);
    }

    private static boolean contains(List<AnnotationNode> annots, String desc) {
        if (annots == null) return false;
        for (AnnotationNode a : annots) {
            if (desc.equals(a.desc)) return true;
        }
        return false;
    }
}
