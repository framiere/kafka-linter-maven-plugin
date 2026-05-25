package sample;

import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Map;
import java.util.function.Function;

/**
 * RULE: STREAMS_BRANCH_DEPRECATED — must NOT fire.
 *
 * <p>The three methods below exercise the supported migration
 * targets for {@code KStream.branch(...)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.split()} (zero-arg) followed by the fluent
 *       chain {@code .branch(Predicate, Branched).defaultBranch(Branched)}.
 *       The owner of each chained call is {@code BranchedKStream},
 *       not {@code KStream} — the rule's owner pin discriminates,
 *       so the chained {@code .branch(...)} calls are NOT
 *       flagged.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on the named-split overload
 *       {@code KStream.split(Named)}, followed by the same fluent
 *       chain. Same reasoning: the fluent {@code .branch(...)}
 *       calls are on {@code BranchedKStream}, not
 *       {@code KStream}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KStream::split} bound to a
 *       {@code Function<KStream<K,V>, BranchedKStream<K,V>>}
 *       supplier — javac resolves the method-ref by arity and
 *       return-erasure to {@code split()}. The bsm-arg handle's
 *       name is {@code split}, not {@code branch}, so the rule's
 *       name filter rejects the site.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>KIP-418 (Kafka Streams 2.8, April 2021) introduced
 * {@code KStream.split([Named])} returning {@code BranchedKStream}
 * with a fluent builder that solves the four documented correctness
 * traps of the legacy {@code branch(Predicate...)} API:
 * (1) {@code defaultBranch(...)} eliminates silent drops — records
 * matching no predicate go to the explicit default sink;
 * (2) {@code Branched.as(name)} makes branches addressable by string
 * key, so reordering the {@code .branch(...)} calls preserves
 * routing; (3) named branches show up in JMX with their declared
 * names; (4) the new API's method name is {@code split} on
 * {@code KStream} and {@code branch} on {@code BranchedKStream} —
 * the (owner, name) pair discriminates from the legacy
 * (KStream, branch) shape.
 */
public final class GoodBranch {

    public Map<String, KStream<String, String>> routeByPredicates(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.split() (zero-arg) is the
        // supported entry to the fluent BranchedKStream API. The
        // chained .branch(Predicate, Branched) calls below are on
        // BranchedKStream, NOT KStream — the rule's owner pin
        // (org/apache/kafka/streams/kstream/KStream) discriminates
        // and rejects these sites.
        BranchedKStream<String, String> split = stream.split();
        return split
                .branch((k, v) -> v.startsWith("hot:"), Branched.as("hot"))
                .branch((k, v) -> v.startsWith("warm:"), Branched.as("warm"))
                .defaultBranch(Branched.as("cold"));
    }

    public Map<String, KStream<String, String>> routeByNamedPredicates(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.split(Named) is the named entry
        // to the fluent BranchedKStream API. Same reasoning as
        // above for the chained .branch(...) calls.
        return stream.split(Named.as("router"))
                .branch((k, v) -> v.startsWith("hot:"), Branched.as("hot"))
                .branch((k, v) -> v.startsWith("warm:"), Branched.as("warm"))
                .defaultBranch(Branched.as("cold"));
    }

    public Function<KStream<String, String>, BranchedKStream<String, String>> capturedSplit() {
        // DOES NOT FIRE — INVOKEDYNAMIC unbound method-ref capture
        // resolving to KStream::split. The bsm-arg handle's name
        // is `split`, not `branch`, so the rule's name filter
        // rejects the site.
        return KStream::split;
    }
}
