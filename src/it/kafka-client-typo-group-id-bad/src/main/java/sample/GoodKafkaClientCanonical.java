package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Negative control for KAFKA_CLIENT_TYPO_GROUP_ID — every put / setProperty
 * in this class is silent because the key uses Kafka's canonical
 * dot-separated form ({@code group.id}, {@code bootstrap.servers},
 * {@code enable.auto.commit}, {@code auto.offset.reset},
 * {@code compression.type}). The TYPOS set is a CLOSED enumeration,
 * so anything not in that 10-entry set passes.
 *
 * <p>Two extra cases prove the boundary:
 * <ol>
 *   <li>{@code unrelatedConfig} — uses a totally unrelated key
 *       ({@code my.app.feature.flag}) on a Properties holder. The
 *       rule does NOT have a positive-list of valid Kafka keys, so
 *       any string outside the TYPOS set is silent. This is by
 *       design — the rule is narrow on purpose, only catching
 *       known typo shapes.</li>
 *   <li>{@code interpolatedKey} — a key built from string
 *       concatenation. The rule reads only LDC String constants in
 *       the slot immediately before the put/setProperty call; an
 *       interpolated key bypasses the lexical scan and is silent.
 *       This is a known limitation of the rule, but typos always
 *       appear as literal strings in real code so the gap is
 *       irrelevant in practice.</li>
 * </ol>
 */
public final class GoodKafkaClientCanonical {

    public Properties buildConsumerProps() {
        Properties props = new Properties();
        props.put("group.id", "orders-processor"); // silent — canonical dot form
        props.setProperty("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092"); // silent
        props.put("enable.auto.commit", "false"); // silent
        props.put("auto.offset.reset", "earliest"); // silent
        props.put("compression.type", "snappy"); // silent
        return props;
    }

    public HashMap<String, Object> buildProducerMap() {
        HashMap<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092"); // silent
        props.put("compression.type", "snappy"); // silent
        return props;
    }

    public Properties unrelatedConfig() {
        Properties props = new Properties();
        props.put("my.app.feature.flag", "true"); // silent — not in TYPOS set
        return props;
    }

    public Map<String, Object> interpolatedKey(String prefix) {
        Map<String, Object> props = new HashMap<>();
        props.put(prefix + ".group.id", "orders"); // silent — non-LDC key bypasses the lexical scan
        return props;
    }
}
