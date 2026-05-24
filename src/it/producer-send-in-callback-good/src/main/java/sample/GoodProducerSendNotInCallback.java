package sample;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Good shapes — must NOT fire PRODUCER_SEND_IN_CALLBACK.
 */
public final class GoodProducerSendNotInCallback {

    /** Control #1: callback hands the next send off to an executor — Sender thread is freed
     *  immediately, the next send happens on a worker thread. */
    public static final class HandOffCallback implements Callback {
        private final ExecutorService executor;
        private final KafkaProducer<String, String> producer;
        private final ProducerRecord<String, String> derived;

        public HandOffCallback(ExecutorService executor,
                               KafkaProducer<String, String> producer,
                               ProducerRecord<String, String> derived) {
            this.executor = executor;
            this.producer = producer;
            this.derived = derived;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                executor.submit(() -> producer.send(derived)); // safe — worker thread
            }
        }
    }

    /** Control #2: plain user-thread send, no callback at all. */
    public void plainUserThreadSend(KafkaProducer<String, String> producer,
                                    ProducerRecord<String, String> record) {
        producer.send(record);
    }

    /** Control #3: callback that only logs / observes — no producer work at all. */
    public void sendWithLogOnlyLambda(KafkaProducer<String, String> producer,
                                      ProducerRecord<String, String> record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("send failed: " + exception.getMessage());
            }
        });
    }

    /** Control #4: lambda that targets a DIFFERENT SAM (Consumer) and calls send —
     *  the lambda's SAM-return is {@code Consumer}, not {@code Callback}, so the
     *  rule must not treat its body as a callback. */
    public void consumerLambdaThatSends(KafkaProducer<String, String> producer,
                                        ProducerRecord<String, String> record) {
        Consumer<ProducerRecord<String, String>> sender = r -> producer.send(r);
        sender.accept(record);
    }

    /** Control #5: callback hands off to a Runnable submitted to executor — same
     *  principle as #1 but expressed as a method reference dispatch. */
    public void runnableHandOff(KafkaProducer<String, String> producer,
                                ProducerRecord<String, String> record,
                                ProducerRecord<String, String> derived,
                                ExecutorService executor) {
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                Runnable task = () -> producer.send(derived); // body of Runnable, not Callback
                executor.submit(task);
            }
        });
    }
}
