package io.conductor.kafkalinter.report;

import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import org.apache.maven.plugin.logging.Log;

import java.util.List;

/**
 * One line per violation: severity, rule id, location, message. Suitable for CI logs.
 */
public final class SimpleReporter implements Reporter {

    @Override
    public void report(List<Violation> violations, Log log) {
        if (violations.isEmpty()) {
            log.info("kafka-linter: 0 violations.");
            return;
        }
        Counts c = Counts.of(violations);
        log.info("kafka-linter: " + violations.size() + " violation(s) — "
                + c.errors + " error, " + c.warnings + " warning, " + c.infos + " info.");
        for (Violation v : violations) {
            String line = v.format();
            switch (v.severity()) {
                case ERROR -> log.error(line);
                case WARNING -> log.warn(line);
                case INFO -> log.info(line);
                case OFF -> { /* not emitted */ }
            }
        }
    }

    record Counts(long errors, long warnings, long infos) {
        static Counts of(List<Violation> vs) {
            long e = vs.stream().filter(v -> v.severity() == Severity.ERROR).count();
            long w = vs.stream().filter(v -> v.severity() == Severity.WARNING).count();
            long i = vs.stream().filter(v -> v.severity() == Severity.INFO).count();
            return new Counts(e, w, i);
        }
    }
}
