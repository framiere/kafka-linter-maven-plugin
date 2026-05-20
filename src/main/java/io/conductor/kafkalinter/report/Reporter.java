package io.conductor.kafkalinter.report;

import io.conductor.kafkalinter.Violation;
import org.apache.maven.plugin.logging.Log;

import java.util.List;

/**
 * Emits the lint result to the Maven log.
 *
 * <p>Pluggable so the Mojo doesn't grow a switch over format flavors. Two
 * implementations ship in-tree:
 * <ul>
 *   <li>{@link SimpleReporter} — one line per violation, build-log-friendly.</li>
 *   <li>{@link VerboseReporter} — multi-line per violation with tagline + doc path,
 *       intended for local runs and onboarding.</li>
 * </ul>
 */
public interface Reporter {
    void report(List<Violation> violations, Log log);

    static Reporter of(String name) {
        if (name == null) return new SimpleReporter();
        return switch (name.toLowerCase()) {
            case "verbose" -> new VerboseReporter();
            case "simple", "" -> new SimpleReporter();
            default -> throw new IllegalArgumentException(
                    "Unknown reporter '" + name + "' — expected 'simple' or 'verbose'.");
        };
    }
}
