package sample;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

/**
 * RULE: TOPOLOGY_ADD_PROCESSOR_LEGACY_SUPPLIER.
 *
 * Fires when {@code Topology.addProcessor(name, ProcessorSupplier, ...)} is called
 * with the LEGACY {@code org.apache.kafka.streams.processor.ProcessorSupplier}
 * (vs the KIP-820 {@code org.apache.kafka.streams.processor.api.ProcessorSupplier}).
 * The bytecode descriptor for the legacy overload contains
 * {@code Lorg/apache/kafka/streams/processor/ProcessorSupplier;} (no /api/ between
 * /processor/ and /ProcessorSupplier;); the new overload's descriptor contains
 * {@code Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;}.
 */
public final class BadTopologyAddProcessorLegacySupplier {

    /** Anti-pattern: legacy ProcessorSupplier passed by lambda — FIRES. */
    public Topology buildLegacyLambda(Topology t) {
        ProcessorSupplier<String, String> legacy = LegacyProcessor::new;
        return t.addProcessor("legacy-via-lambda", legacy, "source"); // FIRES — legacy ProcessorSupplier
    }

    /** Anti-pattern: legacy ProcessorSupplier passed by method reference — FIRES. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Topology buildLegacyMethodRef(Topology t) {
        return t.addProcessor("legacy-via-method-ref", (ProcessorSupplier) LegacyProcessor::new, "source"); // FIRES — legacy ProcessorSupplier
    }

    /** Anti-pattern: legacy ProcessorSupplier passed inline as anonymous class — FIRES. */
    public Topology buildLegacyAnonymous(Topology t) {
        return t.addProcessor("legacy-anonymous", new ProcessorSupplier<String, String>() {
            @Override
            public Processor<String, String> get() {
                return new LegacyProcessor();
            }
        }, "source"); // FIRES — legacy ProcessorSupplier
    }

    /** Control: new typed api.ProcessorSupplier — must NOT fire. */
    public Topology buildNewApi(Topology t) {
        org.apache.kafka.streams.processor.api.ProcessorSupplier<String, String, String, String> newApi =
                () -> new org.apache.kafka.streams.processor.api.Processor<>() {
                    @Override
                    public void process(Record<String, String> record) {
                        // process record
                    }
                };
        return t.addProcessor("new-api", newApi, "source");
    }

    /** Legacy processor implementation — uses the old AbstractProcessor + process(K, V) signature. */
    static final class LegacyProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(String key, String value) {
            ProcessorContext ctx = context();
            ctx.forward(key, value == null ? "" : value.toUpperCase());
        }
    }
}
