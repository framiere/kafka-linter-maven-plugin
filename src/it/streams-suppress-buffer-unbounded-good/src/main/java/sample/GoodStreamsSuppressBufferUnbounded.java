package sample;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.Suppressed.EagerBufferConfig;
import org.apache.kafka.streams.kstream.Suppressed.StrictBufferConfig;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every BufferConfig has an explicit bound,
 * either via {@link BufferConfig#maxBytes(long)} or
 * {@link BufferConfig#maxRecords(long)}, paired with a
 * shutdown-on-overflow policy.
 */
public final class GoodStreamsSuppressBufferUnbounded {

    private static final long MAX_BYTES = 256L * 1024L * 1024L;
    private static final long MAX_RECORDS = 1_000_000L;

    public Suppressed<Windowed> bufA() {
        return Suppressed.untilWindowCloses(
                BufferConfig.maxBytes(MAX_BYTES).shutDownWhenFull());
    }

    public Suppressed<Object> bufB() {
        StrictBufferConfig buf =
                BufferConfig.maxRecords(MAX_RECORDS).shutDownWhenFull();
        return Suppressed.untilTimeLimit(Duration.ofSeconds(30), buf);
    }

    public StrictBufferConfig bufC() {
        return BufferConfig.maxBytes(MAX_BYTES).shutDownWhenFull();
    }

    public Suppressed<Object> bufD() {
        return Suppressed.untilTimeLimit(
                Duration.ofSeconds(60),
                BufferConfig.maxRecords(MAX_RECORDS).shutDownWhenFull());
    }

    public Supplier<StrictBufferConfig> buildSupplierFactory() {
        return () -> BufferConfig.maxBytes(MAX_BYTES).shutDownWhenFull();
    }

    public Function<Long, EagerBufferConfig> buildBoundFactory() {
        return BufferConfig::maxBytes;
    }

    public StrictBufferConfig useLocalFactory() {
        Function<Long, EagerBufferConfig> factory = BufferConfig::maxBytes;
        return factory.apply(MAX_BYTES).shutDownWhenFull();
    }

    public Stream<StrictBufferConfig> buildAll(List<String> names) {
        Function<String, StrictBufferConfig> f =
                n -> BufferConfig.maxBytes(MAX_BYTES).shutDownWhenFull();
        return names.stream().map(f);
    }

    public static void main(String[] args) {
        new GoodStreamsSuppressBufferUnbounded();
    }
}
