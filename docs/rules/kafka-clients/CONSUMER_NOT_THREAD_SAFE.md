# CONSUMER_NOT_THREAD_SAFE

**Severity**: ERROR
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Kafka consumers are single-threaded. Share one and the JVM tells you so.

## TL;DR

The linter flags evidence that a single `KafkaConsumer` instance is touched from more than one thread (except for `wakeup()`, which is the only thread-safe method). Real-world fallout: `ConcurrentModificationException`.

## What's happening (the mechanism)

`KafkaConsumer` is documented as not thread-safe. The class detects concurrent use via a lock counter and throws:

```
java.util.ConcurrentModificationException:
  KafkaConsumer is not safe for multi-threaded access
```

The only thread-safe method is `wakeup()`, which is explicitly designed for cross-thread interruption.

Common ways to accidentally violate this:
1. Storing the consumer in a Spring-managed singleton bean and calling `poll()` from two threads.
2. Submitting `() -> consumer.poll(...)` to an `ExecutorService`.
3. Calling `consumer.commitSync()` from a worker thread that processes records in parallel (worker thread is not the poll thread).
4. Capturing the consumer in a lambda submitted to a parallel stream.

## Operational impact

- `ConcurrentModificationException` at runtime — typically a few seconds into the workload, not at startup.
- The consumer state is left inconsistent; the only safe recovery is `close()` + new instance.
- Subtle data loss if the exception fires after some records were processed but before commit.

## How to fix

```java
// BAD — sharing one consumer across worker threads
KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props);
ExecutorService pool = Executors.newFixedThreadPool(4);
while (true) {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<K, V> r : records) {
        pool.submit(() -> {
            // process...
            consumer.commitSync(); // BOOM: ConcurrentModificationException
        });
    }
}

// GOOD — poll thread owns the consumer; offload only the processing
while (running) {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
    List<Future<?>> work = new ArrayList<>();
    for (ConsumerRecord<K, V> r : records) {
        work.add(pool.submit(() -> handle(r)));
    }
    for (Future<?> f : work) f.get();   // wait for batch
    consumer.commitSync();              // commit on the poll thread
}

// GOOD — one consumer per thread (consumer-per-thread model)
```

## When this might be a false positive

- Multi-consumer-per-instance designs that use one consumer per thread (correct pattern). The static scan must distinguish "the consumer field is touched from N methods" (potentially same thread) from "the consumer is dispatched to a thread pool" (definite violation).
- Wrappers (`KafkaShareConsumer`, framework concurrency layers) where ownership is explicit.

## Detection strategy

- Bytecode: detect a `KafkaConsumer`-typed instance field whose value is captured inside a lambda or anonymous `Runnable`/`Callable` passed to:
  - `ExecutorService.submit`, `.execute`, `.invokeAll`.
  - `new Thread(...).start()`.
  - `CompletableFuture.runAsync`, `.supplyAsync`.
  - `Stream.parallel()`, `parallelStream()`.
- Exception: ignore calls to `wakeup()` (thread-safe).
- MEDIUM because intra-thread captures cannot always be ruled out.

## References

- KafkaConsumer javadoc — "Multi-threaded Processing" section: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
- Confluent — Consumer thread model: https://docs.confluent.io/platform/current/clients/consumer.html
