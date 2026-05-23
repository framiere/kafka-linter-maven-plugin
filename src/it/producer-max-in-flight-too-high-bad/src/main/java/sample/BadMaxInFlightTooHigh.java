package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * RULE: PRODUCER_MAX_IN_FLIGHT_TOO_HIGH.
 *
 * <p>Fires when a configuration {@code put(key, value)} (or
 * {@code setProperty}) writes the key
 * {@code "max.in.flight.requests.per.connection"} with a literal
 * value &gt; 5 — regardless of which numeric encoding the bytecode
 * compiler chose.
 *
 * <p>Why &gt; 5 is forbidden in a modern producer:
 * <ol>
 *   <li>{@code enable.idempotence} defaults to {@code true} since
 *       kafka-clients 3.0. Idempotent mode is what gives the
 *       producer its "exactly once on a single partition" guarantee:
 *       each batch is tagged with the producer's PID and a
 *       monotonic sequence number, and the broker rejects any
 *       batch it has already seen.</li>
 *   <li>To detect duplicates and reorderings, the broker keeps a
 *       small per-PID sliding window of recently accepted batches.
 *       The size of that window is HARD-CODED to 5 batches in the
 *       broker's idempotent-producer code path. The client must
 *       not have more than 5 in-flight requests per connection,
 *       otherwise the broker can no longer guarantee dedupe
 *       correctness.</li>
 *   <li>Specifically the broker check is, in effect:
 *       {@code if (config.transactional.id != null ||
 *       config.enable.idempotence) require(maxInFlight &lt;= 5)}.
 *       The producer reads this constraint at start-up and refuses
 *       to construct: {@code ConfigException("Must be set to at
 *       most 5 to use the idempotent producer.")}.</li>
 *   <li>Even if you turn idempotence off explicitly, &gt; 5 is at
 *       best DEAD config (you can't go back to idempotent without
 *       fixing this) and at worst a foot-gun: someone re-enables
 *       idempotence in a year and the producer no longer starts.</li>
 * </ol>
 *
 * <p>Why the rule is purely literal-based:
 * <ol>
 *   <li>The check is cheap and exact when the value is a literal:
 *       parse the int, compare to 5. No false positives.</li>
 *   <li>Non-literal values (e.g. {@code Integer.parseInt(env)} or
 *       a method call) are intentionally OUT OF SCOPE — the rule
 *       returns {@code null} from its literal-extractor and
 *       silently skips. Tracking values across method boundaries
 *       would need data-flow analysis, and the typical real bug
 *       is a literal "10" or "100" pasted in from an old guide
 *       (or from a doc page that predates kafka-clients 3.0).</li>
 *   <li>The rule does NOT verify that {@code enable.idempotence}
 *       is actually set to true in the same Properties. The
 *       motivation: even with idempotence explicitly off, the
 *       broker behavior changed enough across versions that a
 *       hard cap of 5 is the safer default. Confidence:
 *       intentionally HIGH because the value is a literal we can
 *       see at lint time.</li>
 * </ol>
 *
 * <p>What the rule catches (per-method scan):
 * <ol>
 *   <li>Walks every {@code MethodInsnNode} where owner is one of
 *       {@code Properties} / {@code Map} / {@code HashMap} and the
 *       method name is {@code put} or {@code setProperty}.</li>
 *   <li>Reads the value via {@code prevSignificant(putInsn)} and
 *       the key via {@code prevSignificant(valueInsn)}.</li>
 *   <li>Skips if key is not the LDC string
 *       {@code "max.in.flight.requests.per.connection"}.</li>
 *   <li>Extracts an integer from the value insn — supports:
 *       <ul>
 *         <li>{@code ICONST_M1..ICONST_5} (opcodes 0x02-0x08,
 *             values −1..5; none can exceed the cap)</li>
 *         <li>{@code BIPUSH} (8-bit signed int, −128..127)</li>
 *         <li>{@code SIPUSH} (16-bit signed int, −32768..32767)</li>
 *         <li>{@code LDC Integer} (any 32-bit int constant)</li>
 *         <li>{@code LDC Long} where the long fits in an int
 *             (covers the rare {@code Properties.put(key, 10L)})</li>
 *         <li>{@code LDC String} that parses as an int (covers
 *             the very common {@code Properties.put(key, "10")}
 *             since Properties values are conventionally strings)</li>
 *       </ul>
 *   </li>
 *   <li>Fires only when the extracted int is &gt; 5.</li>
 * </ol>
 *
 * <p>This fixture exercises every supported encoding by laying them
 * out across six methods that each trigger exactly one violation:
 * BIPUSH (10), BIPUSH at the wider end (100), SIPUSH (1000),
 * LDC String numeric, LDC Long, and {@code setProperty} (which
 * unlike {@code put} requires a String value, exercising the
 * string-numeric path through a different method name).
 */
public final class BadMaxInFlightTooHigh {

    /** Anti-pattern: BIPUSH 10. Common copy-paste from pre-3.0 producer guides. */
    public Properties bipushTen() {
        Properties props = new Properties();
        props.put("max.in.flight.requests.per.connection", 10); // reported (BIPUSH 10)
        return props;
    }

    /** Anti-pattern: BIPUSH 100. Inside the BIPUSH range (-128..127) so still 1-byte encoded. */
    public Properties bipushHundred() {
        Properties props = new Properties();
        props.put("max.in.flight.requests.per.connection", 100); // reported (BIPUSH 100)
        return props;
    }

    /** Anti-pattern: SIPUSH 1000. Above the BIPUSH range so the compiler emits SIPUSH. */
    public Properties sipushThousand() {
        Properties props = new Properties();
        props.put("max.in.flight.requests.per.connection", 1000); // reported (SIPUSH 1000)
        return props;
    }

    /** Anti-pattern: value as String. Very common because Properties values are conventionally Strings. */
    public Properties stringNumeric() {
        Properties props = new Properties();
        props.put("max.in.flight.requests.per.connection", "20"); // reported (LDC String "20")
        return props;
    }

    /** Anti-pattern: LDC Long value (literal {@code 7L}). The rule accepts longs that fit in int. */
    public Map<String, Object> longLiteralOnMap() {
        Map<String, Object> props = new HashMap<>();
        props.put("max.in.flight.requests.per.connection", 7L); // reported (LDC Long 7)
        return props;
    }

    /** Anti-pattern: setProperty (the alternate Properties API) with a string-encoded value. */
    public Properties setPropertyForm() {
        Properties props = new Properties();
        props.setProperty("max.in.flight.requests.per.connection", "50"); // reported (setProperty + LDC "50")
        return props;
    }
}
