package sample;

import org.springframework.kafka.annotation.KafkaListener;

import java.util.HashMap;
import java.util.Map;

public final class GoodListener {

    @KafkaListener(topics = "orders")
    public void onMessage(String body) {
        System.out.println(body);
    }

    public Map<String, Object> consumerConfig() {
        Map<String, Object> p = new HashMap<>();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("value.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        p.put("spring.deserializer.value.delegate.class",
                "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
