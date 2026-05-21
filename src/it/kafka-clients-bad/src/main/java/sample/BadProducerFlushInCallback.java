package sample;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * RULE: PRODUCER_FLUSH_IN_CALLBACK.
 *
 * Calling producer.flush() from inside a Callback.onCompletion body deadlocks
 * the producer:
 *   - Callbacks run on the producer's I/O / sender thread.
 *   - flush() blocks the calling thread until the accumulator drains.
 *   - The accumulator can only drain when the sender thread is free.
 *   - The sender thread is currently running this callback.
 *   ⇒ The sender thread is waiting for itself. Forever.
 *
 * Modern kafka-clients (KAFKA-10852, 2.8+) throws a runtime
 * KafkaException at the first such call site; older versions hang silently.
 * Either way, this is broken code that compiles cleanly — exactly the kind
 * of bug a build-time lint should kill.
 *
 * The rule fires once per flush() call site inside a callback body. This
 * file exercises both shapes the rule must catch:
 *   1) A named class implementing Callback.
 *   2) A lambda passed to producer.send(record, lambda).
 */
public final class BadProducerFlushInCallback {

    private static Properties props() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "broker:9092");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }

    public void namedClassCallback() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props());
        producer.send(new ProducerRecord<>("orders", "k", "v"), new DeadlockingCallback(producer));
        producer.close();
    }

    private static final class DeadlockingCallback implements Callback {
        private final KafkaProducer<String, String> producer;

        DeadlockingCallback(KafkaProducer<String, String> producer) {
            this.producer = producer;
        }

        @Override
        public void onCompletion(RecordMetadata md, Exception e) {
            if (e == null) {
                producer.flush();  // FIRES
            }
        }
    }

    public void lambdaCallback() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props());
        producer.send(new ProducerRecord<>("orders", "k", "v"), (md, e) -> {
            if (e == null) {
                producer.flush();  // FIRES
            }
        });
        producer.close();
    }

    public void cleanLambdaShouldNotFire() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props());
        producer.send(new ProducerRecord<>("orders", "k", "v"), (md, e) -> {
            if (e != null) {
                System.err.println("send failed: " + e);
            }
        });
        producer.flush();  // legitimate: flush before close, NOT in callback — must NOT fire
        producer.close();
    }
}
