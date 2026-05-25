package sample;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.function.Consumer;

/**
 * RULE: STREAMS_FOREACH_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptor on KStream
 * ({@code foreach(ForeachAction)}) via direct INVOKEINTERFACE
 * and via INVOKEDYNAMIC method-reference captures.
 *
 * <p>KStream.foreach is the TERMINAL side-effect sink — every
 * fire here is a latent at-least-once-amplification hazard on
 * rebalance or restore-from-changelog.
 */
public final class BadStreamsForEachNoNamed {

    /** 1-arg SAM matching {@code (ForeachAction)V}. */
    @FunctionalInterface
    interface ForEachFactory<K, V> {
        void apply(ForeachAction<? super K, ? super V> action);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.foreach(ForeachAction). */
    public void chargePayments(KStream<String, String> stream) {
        stream.foreach((k, v) -> System.out.println("charge " + k + " " + v));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload, second site. */
    public void auditEvents(KStream<String, String> stream) {
        stream.foreach((k, v) -> System.out.println("audit " + k));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload, distinct call site. */
    public void dispatchAlerts(KStream<String, String> stream) {
        stream.foreach((k, v) -> { /* alert */ });
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::foreach} bound to
     *  {@link Consumer Consumer&lt;ForeachAction&gt;}. */
    public Consumer<ForeachAction<String, String>>
            buildChargeFactory(KStream<String, String> stream) {
        return stream::foreach;
    }

    /** MUST FIRE — {@code stream::foreach} bound to a generic
     *  Consumer at a second site. */
    public Consumer<ForeachAction<String, String>>
            buildAuditFactory(KStream<String, String> stream) {
        return stream::foreach;
    }

    /** MUST FIRE — {@code stream::foreach} bound to a custom
     *  1-arg generic SAM. */
    public <K, V> ForEachFactory<K, V> buildCustomFactory(KStream<K, V> stream) {
        return stream::foreach;
    }

    /** MUST FIRE — local Consumer binding via method
     *  reference, applied inside the same method. */
    public void useLocalFactory(KStream<String, String> stream,
            ForeachAction<String, String> action) {
        Consumer<ForeachAction<String, String>> factory = stream::foreach;
        factory.accept(action);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code s.foreach(action)}. */
    public void dispatchAll(
            List<KStream<String, String>> streams,
            ForeachAction<String, String> action) {
        streams.forEach(s -> s.foreach(action));
    }

    public static void main(String[] args) {
        new BadStreamsForEachNoNamed();
    }
}
