package sample;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named} pinning the foreach terminal-node name (and,
 * transitively, the JMX MBean name and every metric tag).
 */
public final class GoodStreamsForEachNoNamed {

    /** 2-arg SAM matching {@code (ForeachAction, Named)V}. */
    @FunctionalInterface
    interface ForEachFactory<K, V> {
        void apply(ForeachAction<? super K, ? super V> action, Named named);
    }

    public void chargePayments(KStream<String, String> stream) {
        stream.foreach((k, v) -> System.out.println("charge " + k + " " + v),
                Named.as("payment-charge-dispatch"));
    }

    public void auditEvents(KStream<String, String> stream) {
        stream.foreach((k, v) -> System.out.println("audit " + k),
                Named.as("audit-event-emit"));
    }

    public void dispatchAlerts(KStream<String, String> stream) {
        stream.foreach((k, v) -> { /* alert */ },
                Named.as("alert-dispatch"));
    }

    public BiConsumer<ForeachAction<String, String>, Named>
            buildChargeFactory(KStream<String, String> stream) {
        return stream::foreach;
    }

    public BiConsumer<ForeachAction<String, String>, Named>
            buildAuditFactory(KStream<String, String> stream) {
        return stream::foreach;
    }

    public <K, V> ForEachFactory<K, V> buildCustomFactory(KStream<K, V> stream) {
        return stream::foreach;
    }

    public void useLocalFactory(KStream<String, String> stream,
            ForeachAction<String, String> action) {
        BiConsumer<ForeachAction<String, String>, Named> factory = stream::foreach;
        factory.accept(action, Named.as("local-factory-foreach"));
    }

    public void dispatchAll(
            List<KStream<String, String>> streams,
            ForeachAction<String, String> action) {
        streams.forEach(s -> s.foreach(action, Named.as("dispatch-all")));
    }

    public static void main(String[] args) {
        new GoodStreamsForEachNoNamed();
    }
}
