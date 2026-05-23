package sample;

import java.util.Properties;

/**
 * Control for PRODUCER_MAX_IN_FLIGHT_TOO_HIGH.
 *
 * <p>Each method exercises a shape the rule MUST stay silent on:
 * <ul>
 *   <li>literal value at the cap (5) — extractor returns 5,
 *       rule's {@code parsed <= CAP} guard fires.</li>
 *   <li>literal value below the cap (1) — same path.</li>
 *   <li>value-as-String at the cap ("5") — String parse path.</li>
 *   <li>different key — extractor would happily return 999, but
 *       the key-check guard fails before we look at the value.</li>
 *   <li>non-literal value (a method return) — extractor returns
 *       {@code null}; the rule's {@code parsed == null} guard
 *       skips it. Even if the runtime value is 50, this is OUT
 *       OF SCOPE for the static analysis.</li>
 * </ul>
 */
public final class GoodMaxInFlight {

    public Properties atCap() {
        Properties props = new Properties();
        props.put("max.in.flight.requests.per.connection", 5); // silent (ICONST_5)
        return props;
    }

    public Properties belowCap() {
        Properties props = new Properties();
        props.put("max.in.flight.requests.per.connection", 1); // silent (ICONST_1)
        return props;
    }

    public Properties stringAtCap() {
        Properties props = new Properties();
        props.put("max.in.flight.requests.per.connection", "5"); // silent (LDC "5")
        return props;
    }

    public Properties differentKey() {
        Properties props = new Properties();
        props.put("acks", "all");                            // silent (different key)
        props.put("retries", 999);                           // silent (different key)
        return props;
    }

    public Properties nonLiteralValue() {
        Properties props = new Properties();
        // Value comes from a method call — not a literal — rule's extractor returns null and skips.
        props.put("max.in.flight.requests.per.connection", computeRuntimeValue());
        return props;
    }

    private int computeRuntimeValue() {
        return Integer.parseInt(System.getProperty("MAX_IN_FLIGHT", "5"));
    }
}
