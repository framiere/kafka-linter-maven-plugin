package io.conductor.kafkalinter.report;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import org.apache.maven.plugin.logging.Log;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Multi-paragraph per rule. The output is shaped like a teaching session, not a
 * list of error codes — each rule gets its mechanism, operational impact, and a
 * "why this is subtle" paragraph, followed by every call site that tripped it.
 *
 * <p>Designed for local runs and onboarding. For CI, prefer {@link SimpleReporter}.
 */
public final class VerboseReporter implements Reporter {

    @Override
    public void report(List<Violation> violations, Log log) {
        if (violations.isEmpty()) {
            log.info("kafka-linter: 0 violations.");
            return;
        }
        SimpleReporter.Counts c = SimpleReporter.Counts.of(violations);
        log.info("kafka-linter: " + violations.size() + " violation(s) — "
                + c.errors() + " error, " + c.warnings() + " warning, " + c.infos() + " info.");
        log.info("");

        Map<RuleId, List<Violation>> byRule = new LinkedHashMap<>();
        for (Violation v : violations) {
            byRule.computeIfAbsent(v.rule(), k -> new java.util.ArrayList<>()).add(v);
        }

        for (Map.Entry<RuleId, List<Violation>> e : byRule.entrySet()) {
            RuleId rule = e.getKey();
            List<Violation> hits = e.getValue();
            Severity sev = hits.get(0).severity();

            String header = "═══ " + rule.id() + " ═══ [" + sev + " · " + rule.confidence()
                    + " · " + rule.category() + "]";
            emit(log, sev, header);
            emit(log, sev, "");
            emit(log, sev, "  " + rule.tagline());
            emit(log, sev, "");
            emit(log, sev, "  What's happening");
            emit(log, sev, "    " + rule.mechanism());
            emit(log, sev, "");
            emit(log, sev, "  Operational impact");
            emit(log, sev, "    " + rule.impact());
            emit(log, sev, "");
            emit(log, sev, "  Why this is worth catching");
            emit(log, sev, "    " + rule.whyMatters());
            emit(log, sev, "");
            emit(log, sev, "  Full write-up: docs/rules/" + rule.docPath());
            emit(log, sev, "");
            emit(log, sev, "  Call sites (" + hits.size() + "):");
            for (Violation v : hits) {
                emit(log, sev, "    • " + v.location() + " — " + v.message());
            }
            log.info("");
        }
    }

    private static void emit(Log log, Severity sev, String line) {
        switch (sev) {
            case ERROR -> log.error(line);
            case WARNING -> log.warn(line);
            case INFO -> log.info(line);
            case OFF -> { /* not emitted */ }
        }
    }
}
