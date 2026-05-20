package sample;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;

import java.util.HashMap;
import java.util.Map;

public final class BadListener {

    // RULE: SPRING_LISTENER_ASYNC_ANNOTATION.
    @KafkaListener(topics = "orders")
    @Async
    public void onMessage(String body) {
        System.out.println(body);
    }

    // RULE: SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES.
    public Map<String, Object> consumerConfig() {
        Map<String, Object> p = new HashMap<>();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("group.id", "g");
        p.put("value.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        // missing spring.deserializer.value.delegate.class
        return p;
    }
}
