package io.conductor.kafkalinter;

/**
 * How sure the lint is that a flagged construct is actually a defect.
 *
 * <ul>
 *   <li>{@link #HIGH} — direct mechanical match, almost no false positives expected.</li>
 *   <li>{@link #MEDIUM} — pattern match with known false-positive shapes the rule doc enumerates.</li>
 *   <li>{@link #CONTEXT} — depends on environment / topology / target broker; only meaningful
 *       once you know more than the static signal. Treat as a hint.</li>
 * </ul>
 */
public enum Confidence {
    HIGH,
    MEDIUM,
    CONTEXT
}
