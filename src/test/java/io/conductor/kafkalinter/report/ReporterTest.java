package io.conductor.kafkalinter.report;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReporterTest {

    @Test
    void factoryReturnsSimpleByDefault() {
        assertInstanceOf(SimpleReporter.class, Reporter.of(null));
        assertInstanceOf(SimpleReporter.class, Reporter.of("simple"));
        assertInstanceOf(SimpleReporter.class, Reporter.of(""));
        assertInstanceOf(SimpleReporter.class, Reporter.of("SIMPLE"));
    }

    @Test
    void factoryReturnsVerbose() {
        assertInstanceOf(VerboseReporter.class, Reporter.of("verbose"));
        assertInstanceOf(VerboseReporter.class, Reporter.of("Verbose"));
    }

    @Test
    void factoryRejectsUnknown() {
        assertThrows(IllegalArgumentException.class, () -> Reporter.of("json"));
    }

    @Test
    void countsRollUpBySeverity() {
        List<Violation> vs = List.of(
                violation(RuleId.PRODUCER_IN_LOOP, Severity.ERROR, "A"),
                violation(RuleId.PRODUCER_NO_COMPRESSION, Severity.ERROR, "B"),
                violation(RuleId.CONSUMER_AUTO_COMMIT_TRUE, Severity.WARNING, "C"));
        SimpleReporter.Counts c = SimpleReporter.Counts.of(vs);
        assertEquals(2, c.errors());
        assertEquals(1, c.warnings());
        assertEquals(0, c.infos());
    }

    @Test
    void simpleReporterRunsWithoutThrowing() {
        Log log = new SystemStreamLog();
        new SimpleReporter().report(List.of(), log);
        new SimpleReporter().report(List.of(violation(RuleId.PRODUCER_IN_LOOP, Severity.ERROR, "x")), log);
    }

    @Test
    void verboseReporterRunsWithoutThrowing() {
        Log log = new SystemStreamLog();
        new VerboseReporter().report(List.of(), log);
        new VerboseReporter().report(List.of(
                violation(RuleId.PRODUCER_IN_LOOP, Severity.ERROR, "x"),
                violation(RuleId.PRODUCER_IN_LOOP, Severity.ERROR, "y"),
                violation(RuleId.CONSUMER_AUTO_COMMIT_TRUE, Severity.WARNING, "z")), log);
    }

    @Test
    void violationDerivedMetadataMatchesRule() {
        Violation v = violation(RuleId.PRODUCER_SEND_BLOCKING_GET, Severity.ERROR, "demo");
        assertEquals(RuleId.PRODUCER_SEND_BLOCKING_GET.confidence(), v.confidence());
        assertEquals(RuleId.PRODUCER_SEND_BLOCKING_GET.category(), v.category());
        assertEquals(RuleId.PRODUCER_SEND_BLOCKING_GET.docPath(), v.docPath());
        assertTrue(v.format().contains("PRODUCER_SEND_BLOCKING_GET"));
        assertTrue(v.format().contains("demo"));
        assertFalse(v.format().contains("/"));
    }

    private static Violation violation(RuleId rule, Severity sev, String detail) {
        return new Violation(rule, sev, "io/example/Demo", "doStuff", 42, detail);
    }
}
