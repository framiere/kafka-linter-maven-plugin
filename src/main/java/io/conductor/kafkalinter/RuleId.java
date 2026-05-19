package io.conductor.kafkalinter;

public enum RuleId {
    PRODUCER_IN_LOOP(Severity.ERROR,
        "KafkaProducer instantiated inside a loop or iterating lambda — producers must be long-lived (singleton-style)."),
    CONSUMER_IN_LOOP(Severity.ERROR,
        "KafkaConsumer instantiated inside a loop or iterating lambda — consumers must be long-lived."),
    PRODUCER_NO_COMPRESSION(Severity.ERROR,
        "KafkaProducer constructed without setting 'compression.type' — uncompressed throughput is wasteful and often a footgun."),
    PRODUCER_SEND_BLOCKING_GET(Severity.ERROR,
        "producer.send(record).get() — defeats async batching. Use a Callback instead."),
    PRODUCER_SEND_NO_CALLBACK(Severity.WARNING,
        "producer.send(record) without a Callback and result discarded — send errors will be silently swallowed."),
    PRODUCER_FLUSH_IN_LOOP(Severity.ERROR,
        "producer.flush() called inside a loop — defeats batching."),
    CONSUMER_AUTO_COMMIT_TRUE(Severity.WARNING,
        "KafkaConsumer configured with enable.auto.commit=true — risks message loss or double-processing."),
    CONSUMER_COMMIT_PER_RECORD(Severity.ERROR,
        "commitSync() called inside the per-record loop of a poll() — kills consumer throughput."),
    CONSUMER_POLL_ZERO(Severity.ERROR,
        "consumer.poll(0) / poll(Duration.ZERO) — busy-loops the consumer thread.");

    private final Severity defaultSeverity;
    private final String message;

    RuleId(Severity defaultSeverity, String message) {
        this.defaultSeverity = defaultSeverity;
        this.message = message;
    }

    public Severity defaultSeverity() {
        return defaultSeverity;
    }

    public String message() {
        return message;
    }
}
