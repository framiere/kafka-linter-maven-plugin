package sample;

import java.util.Properties;

/**
 * Good shape #4 — &lt;clinit&gt; exists but constructs NO Kafka client.
 * Static configuration assembly is fine in &lt;clinit&gt; as long as no
 * network-resource-holding object is instantiated.
 */
public final class GoodStaticInitWithoutClient {

    static final Properties DEFAULT_PROPS;

    static {
        DEFAULT_PROPS = new Properties();
        DEFAULT_PROPS.put("bootstrap.servers", "localhost:9092");
        DEFAULT_PROPS.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        DEFAULT_PROPS.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    private GoodStaticInitWithoutClient() {}
}
