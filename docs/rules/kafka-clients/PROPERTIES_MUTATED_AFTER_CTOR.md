# PROPERTIES_MUTATED_AFTER_CTOR

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: KafkaProducer reads your Properties once. After that you're talking to yourself.

## TL;DR

The linter flags code that mutates the `Properties` object after passing it to `new KafkaProducer(props)` / `new KafkaConsumer(props)`. The mutation has no effect on the live client and confuses anyone reading the code.

## What's happening (the mechanism)

`KafkaProducer` and `KafkaConsumer` copy the Properties into an internal `ProducerConfig` / `ConsumerConfig` at construction time. The copy is immutable for the lifetime of the client. Mutating the original `Properties` after construction:

- Has no effect on the running client.
- May be a sign the team thought they were "updating the config live" — common when porting from frameworks that support hot reload.
- May indicate a shared `Properties` reused across multiple constructions — racy and order-dependent.

## Operational impact

- Silent: nothing fails. The code just doesn't do what the author thinks.
- "We bumped `linger.ms` and it didn't change anything" — incident triage time wasted.
- Worst case: two producers built from the same `Properties` reference with intervening mutations have surprising configs.

## How to fix

```java
// BAD
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "...");
KafkaProducer<K, V> p = new KafkaProducer<>(props);
props.put(ProducerConfig.LINGER_MS_CONFIG, "100"); // no effect on p

// GOOD — finish configuring before construction
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "...");
props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
KafkaProducer<K, V> p = new KafkaProducer<>(props);

// GOOD — if you need different configs, use separate Properties objects
```

## When this might be a false positive

- Mutating the Properties to construct a *second* client with different settings (legitimate, but should ideally use a fresh Properties).

## Detection strategy

- Bytecode: track `Properties` allocations; flag if a `Properties.put` / `Properties.setProperty` call is reachable AFTER a `new KafkaProducer(<that Properties>)` or `new KafkaConsumer(<that Properties>)` in the same method's control flow. HIGH.
- Same applies to `Map.put` on a `Map<String,Object>` passed to the constructor.

## References

- KafkaProducer constructor javadoc — "Properties": https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#%3Cinit%3E-java.util.Properties-
- AbstractConfig source — defensive copy: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/config/AbstractConfig.java
