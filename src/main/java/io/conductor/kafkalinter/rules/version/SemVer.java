package io.conductor.kafkalinter.rules.version;

import java.util.Comparator;

/**
 * Three-component semantic-version comparator: {@code MAJOR.MINOR.PATCH} with anything
 * after the patch (qualifiers like {@code -RC1}, {@code .Final}) ignored.
 *
 * <p>Sufficient for the version-gate / CVE rules — we never need to compare qualifiers,
 * only ranges like "< 3.5" or ">= 3.0".
 */
public record SemVer(int major, int minor, int patch) implements Comparable<SemVer> {

    private static final Comparator<SemVer> COMPARATOR =
            Comparator.comparingInt(SemVer::major)
                      .thenComparingInt(SemVer::minor)
                      .thenComparingInt(SemVer::patch);

    public static SemVer parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("blank version");
        }
        String trimmed = raw.trim();
        int dash = trimmed.indexOf('-');
        if (dash > 0) trimmed = trimmed.substring(0, dash);
        String[] parts = trimmed.split("\\.");
        try {
            int major = parts.length > 0 ? Integer.parseInt(parts[0]) : 0;
            int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            int patch = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;
            return new SemVer(major, minor, patch);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unparseable version: " + raw, e);
        }
    }

    @Override
    public int compareTo(SemVer o) {
        return COMPARATOR.compare(this, o);
    }

    public boolean lessThan(SemVer o) { return compareTo(o) < 0; }
    public boolean atLeast(SemVer o) { return compareTo(o) >= 0; }
    public boolean atMost(SemVer o) { return compareTo(o) <= 0; }

    @Override
    public String toString() {
        return major + "." + minor + "." + patch;
    }
}
