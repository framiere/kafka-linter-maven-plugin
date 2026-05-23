package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * RULE: CONSUMER_AUTO_COMMIT_TRUE.
 *
 * <p>Fires when a consumer-config holder ({@link Properties}, {@link Map},
 * {@link HashMap}) is populated with the literal pair
 * {@code "enable.auto.commit" -> "true"} via {@code put(...)} /
 * {@code setProperty(...)}. This is the default for the Java consumer,
 * but the rule deliberately targets the EXPLICIT write — code that
 * declares "I have considered offset management and chosen automatic
 * commit," which is almost never the right choice.
 *
 * <p>Auto-commit semantics that surprise newcomers:
 * <ul>
 *   <li>The commit happens INSIDE {@code poll()}, on the timer
 *       {@code auto.commit.interval.ms} (default 5 seconds). It does
 *       NOT happen after each record, and it does NOT happen at the
 *       end of processing.</li>
 *   <li>The committed offset is the offset of the NEXT record poll
 *       will return — i.e. it assumes that everything previously
 *       returned by {@code poll()} has been processed.</li>
 *   <li>If the consumer crashes between {@code poll()} returning N
 *       records and the application finishing processing them, the
 *       NEXT poll commits N anyway (5s elapsed) — those N records
 *       are silently lost from this consumer's point of view.</li>
 *   <li>If the consumer crashes inside processing AFTER the auto-commit
 *       fired but BEFORE the last record was processed, the application
 *       has committed past records it never finished — silent loss.</li>
 *   <li>If a rebalance happens mid-batch, the assigned partitions
 *       are revoked WHILE processing is still running, and the
 *       offset commit may or may not include the partial work —
 *       silent double-processing on the consumer that gets the
 *       re-assigned partition.</li>
 * </ul>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A team writes a consumer that calls {@code poll()} in a loop,
 *       processes records, then loops. They leave
 *       {@code enable.auto.commit=true} because "that's the default."</li>
 *   <li>Steady-state is fine. {@code poll()} fires every few seconds,
 *       processing finishes within the loop, the auto-commit timer
 *       lands at a sensible point.</li>
 *   <li>One day the consumer runs slowly (downstream HTTP latency
 *       spikes, GC pause, whatever). Processing takes 10 seconds
 *       per batch instead of 1. Auto-commit fires at the 5-second
 *       mark, halfway through processing — committing offsets the
 *       application has not yet finished writing downstream.</li>
 *   <li>Consumer crashes mid-batch. Restart. Re-poll picks up where
 *       the auto-commit left off — skipping the records that were
 *       in-flight at crash time.</li>
 *   <li>Discovery: downstream reconciles, finds a gap, blames the
 *       producer or the broker. The consumer's auto-commit silently
 *       ate the records.</li>
 * </ol>
 *
 * <p>Recommendation: {@code enable.auto.commit=false} + explicit
 * {@code commitSync()} AFTER processing finishes (or
 * {@code commitAsync()} with a sync at shutdown / rebalance).
 * For at-least-once delivery, this is the only safe pattern.
 *
 * <p>Detection: bytecode rule (dedicated {@code ConsumerAutoCommitTrueRule}
 * predating the generic {@code ConfigKeyValueRule}). Matches a two-LDC
 * pair where the key is the literal {@code "enable.auto.commit"} and
 * the value is the literal {@code "true"}, followed by an INVOKE of
 * {@code put} or {@code setProperty} on {@link Properties}, {@link Map}
 * or {@link HashMap}. Literal-only by design — a value boxed via
 * {@code Boolean.toString(true)} will not fire.
 */
public final class BadConsumerAutoCommitTrue {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    /** Anti-pattern: Properties.setProperty("enable.auto.commit", "true") — FIRES. */
    public Properties propertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "orders-aggregator");
        props.setProperty("enable.auto.commit", "true"); // FIRES — risks loss / double-process
        return props;
    }

    /** Anti-pattern: Properties.put("enable.auto.commit", "true") — FIRES. */
    public Properties propertiesPut() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-aggregator");
        props.put("enable.auto.commit", "true"); // FIRES — risks loss / double-process
        return props;
    }

    /** Anti-pattern: HashMap.put("enable.auto.commit", "true") — FIRES. */
    public Map<String, Object> mapPut() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-aggregator");
        cfg.put("enable.auto.commit", "true"); // FIRES — risks loss / double-process
        return cfg;
    }

    /** Control: enable.auto.commit=false (explicit manual commit) — must NOT fire. */
    public Properties controlAutoCommitFalse() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "orders-aggregator");
        props.setProperty("enable.auto.commit", "false");
        return props;
    }
}
