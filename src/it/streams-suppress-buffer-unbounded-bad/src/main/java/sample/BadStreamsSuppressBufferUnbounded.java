package sample;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.Suppressed.StrictBufferConfig;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_SUPPRESS_BUFFER_UNBOUNDED — must fire EXACTLY
 * 8 times across this class (one per method below).
 *
 * <p>Exercises {@code Suppressed.BufferConfig.unbounded()} in
 * every shape that compiled Java emits: direct INVOKESTATIC on
 * the BufferConfig interface, INVOKEDYNAMIC bound to a captured
 * {@code BufferConfig::unbounded} as Supplier or a custom SAM,
 * and a synthetic lambda body that invokes the method.
 */
public final class BadStreamsSuppressBufferUnbounded {

    /** 0-arg SAM matching {@code ()StrictBufferConfig}. */
    @FunctionalInterface
    interface StrictBufferFactory {
        StrictBufferConfig apply();
    }

    // ===== Direct INVOKESTATIC on Suppressed$BufferConfig =====

    /** MUST FIRE — direct INVOKESTATIC used as the argument to
     *  Suppressed.untilWindowCloses(). */
    public Suppressed<Windowed> bufA() {
        return Suppressed.untilWindowCloses(BufferConfig.unbounded());
    }

    /** MUST FIRE — direct INVOKESTATIC assigned to a local
     *  variable, then handed to Suppressed.untilTimeLimit. */
    public Suppressed<Object> bufB() {
        StrictBufferConfig buf = BufferConfig.unbounded();
        return Suppressed.untilTimeLimit(Duration.ofSeconds(30), buf);
    }

    /** MUST FIRE — direct INVOKESTATIC returned from a helper
     *  that the caller will pass to a suppress operator. */
    public StrictBufferConfig bufC() {
        return BufferConfig.unbounded();
    }

    /** MUST FIRE — direct INVOKESTATIC inside a fluent chain
     *  that also calls a no-op further setter on the buffer. */
    public Suppressed<Object> bufD() {
        return Suppressed.untilTimeLimit(
                Duration.ofSeconds(60),
                BufferConfig.unbounded().withNoBound());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code BufferConfig::unbounded} bound to a
     *  {@link Supplier}{@code <StrictBufferConfig>}. Indy
     *  implMethod handle descriptor matches
     *  Suppressed$BufferConfig.unbounded()StrictBufferConfig. */
    public Supplier<StrictBufferConfig> buildSupplierFactory() {
        return BufferConfig::unbounded;
    }

    /** MUST FIRE — {@code BufferConfig::unbounded} bound to a
     *  custom SAM-typed factory. */
    public StrictBufferFactory buildCustomFactory() {
        return BufferConfig::unbounded;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public StrictBufferConfig useLocalFactory() {
        StrictBufferFactory factory = BufferConfig::unbounded;
        return factory.apply();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code BufferConfig.unbounded()}. The synthetic
     *  {@code lambda$buildAll$0} carries a direct INVOKESTATIC
     *  on Suppressed$BufferConfig.unbounded. */
    public Stream<StrictBufferConfig> buildAll(List<String> names) {
        Function<String, StrictBufferConfig> f = n -> BufferConfig.unbounded();
        return names.stream().map(f);
    }

    public static void main(String[] args) {
        new BadStreamsSuppressBufferUnbounded();
    }
}
