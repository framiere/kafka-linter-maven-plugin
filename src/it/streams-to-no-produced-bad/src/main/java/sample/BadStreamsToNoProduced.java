package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.List;
import java.util.function.Consumer;

/**
 * RULE: STREAMS_TO_NO_PRODUCED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises both unsafe overload descriptors:
 *
 * <ul>
 *   <li>{@code to(String)} — UNSAFE</li>
 *   <li>{@code to(TopicNameExtractor)} — UNSAFE</li>
 * </ul>
 */
public final class BadStreamsToNoProduced {

    /** 1-arg String SAM matching {@code (String)V}. */
    @FunctionalInterface
    interface ToTopicFactory {
        void apply(String topic);
    }

    private static TopicNameExtractor<String, String> extractor() {
        return (key, value, recordContext) -> "extracted-topic";
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on to(String). */
    public void toOrders(KStream<String, String> stream) {
        stream.to("orders-out");
    }

    /** MUST FIRE — direct INVOKEINTERFACE on to(TopicNameExtractor). */
    public void toExtractor(KStream<String, String> stream) {
        stream.to(extractor());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on to(String) after a filter chain. */
    public void toAfterFilter(KStream<String, String> stream) {
        stream.filter((k, v) -> v != null).to("filtered-out");
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::to} bound to
     *  {@link Consumer Consumer&lt;String&gt;}. Indy bsm-arg
     *  points at {@code to(String)V}. */
    public Consumer<String> buildConsumerFactory(KStream<String, String> stream) {
        return stream::to;
    }

    /** MUST FIRE — {@code stream::to} bound to
     *  {@link Consumer Consumer&lt;TopicNameExtractor&gt;}. Indy
     *  bsm-arg points at {@code to(TopicNameExtractor)V}. */
    public Consumer<TopicNameExtractor<String, String>>
            buildExtractorConsumerFactory(KStream<String, String> stream) {
        return stream::to;
    }

    /** MUST FIRE — {@code stream::to} bound to a custom 1-arg
     *  String SAM. Indy implMethod handle descriptor is the
     *  String unsafe overload exactly. */
    public ToTopicFactory buildCustomFactory(KStream<String, String> stream) {
        return stream::to;
    }

    /** MUST FIRE — local SAM bound and applied immediately. The
     *  indy site is in this method's bytecode. */
    public void useLocalFactory(KStream<String, String> stream) {
        Consumer<String> factory = stream::to;
        factory.accept("local-out");
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code stream.to(topic)}. The synthetic lambda method's
     *  bytecode contains a direct INVOKEINTERFACE on the String
     *  unsafe overload — the rule walks all methods on the class
     *  including synthetic lambda bodies and fires there. */
    public void toAll(KStream<String, String> stream, List<String> topics) {
        topics.forEach(t -> stream.to(t));
    }

    public static void main(String[] args) {
        new BadStreamsToNoProduced();
    }
}
