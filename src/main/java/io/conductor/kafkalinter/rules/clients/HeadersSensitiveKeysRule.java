package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Flags Kafka record-header writes whose key looks like a credential name.
 * Two emit sites are checked: {@code Headers.add(String, byte[])} and
 * {@code new RecordHeader(String, byte[])}. For each call, the rule walks
 * backwards collecting {@code LDC} String constants and flags any that
 * match a sensitive substring (password, token, secret, api-key, etc.)
 * and are not in the allow-list (idempotency-token, trace-token, csrf-token).
 */
public final class HeadersSensitiveKeysRule implements Rule {

    private static final Set<String> SENSITIVE_SUBSTRINGS = Set.of(
            "password", "passwd", "secret", "apikey", "api_key", "api-key",
            "token", "authorization", "bearer", "credential");
    private static final Set<String> ALLOW_LIST = Set.of(
            "idempotency-token", "trace-token", "csrf-token");
    private static final int BACKWARD_WINDOW = 30;

    private final Severity severity;

    public HeadersSensitiveKeysRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.HEADERS_SENSITIVE_KEYS;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!isHeaderEmit(mi)) continue;

                String sensitive = findSensitiveKey(insn);
                if (sensitive == null) continue;

                out.add(new Violation(
                        RuleId.HEADERS_SENSITIVE_KEYS, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "Kafka record header \"" + sensitive + "\" looks like a credential — headers are plaintext on the wire and persist in archival sinks. Pass secrets via mTLS/SASL/OAuth at the connection layer instead."));
            }
        }
        return out;
    }

    private static boolean isHeaderEmit(MethodInsnNode mi) {
        if (KafkaTypes.HEADERS_INTERFACE.equals(mi.owner) && "add".equals(mi.name)) return true;
        return KafkaTypes.RECORD_HEADER.equals(mi.owner) && "<init>".equals(mi.name);
    }

    private static String findSensitiveKey(AbstractInsnNode call) {
        AbstractInsnNode n = AsmUtil.prevSignificant(call);
        int steps = 0;
        while (n != null && steps < BACKWARD_WINDOW) {
            if (n instanceof MethodInsnNode mi && isHeaderEmit(mi)) break;
            if (n instanceof LdcInsnNode ldc && ldc.cst instanceof String s) {
                String lower = s.toLowerCase();
                if (ALLOW_LIST.contains(lower)) {
                    n = AsmUtil.prevSignificant(n);
                    steps++;
                    continue;
                }
                for (String needle : SENSITIVE_SUBSTRINGS) {
                    if (lower.contains(needle)) return s;
                }
            }
            n = AsmUtil.prevSignificant(n);
            steps++;
        }
        return null;
    }
}
