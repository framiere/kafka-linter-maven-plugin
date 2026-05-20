# LAMBDA_PRODUCER_CALLBACK_EMPTY

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `producer.send(record, (md, ex) -> {})` is the lambda spelling of "I don't want to know if it failed."

## TL;DR

The linter flags `KafkaProducer.send(record, callback)` invocations where the callback is a lambda whose body either (a) is empty, (b) does nothing with the `Exception` parameter, or (c) logs at DEBUG/INFO without rethrowing or reporting via a metric. Unlike `PRODUCER_SEND_NO_CALLBACK` (no callback at all), this rule catches the case where a callback *is* provided but functionally does nothing — the visual presence of the lambda creates false confidence that errors are handled.

## The setup

Engineer reads the docs: "send() is async, use a callback to handle errors." They add `(md, ex) -> {}` to silence the IDE warning about ignored return value. The producer now compiles, tests pass, the callback is "there" — but the exception parameter is never read, so any send failure is invisible.

## What's actually happening

`KafkaProducer.send(record, Callback callback)` returns a `Future<RecordMetadata>`. The producer batches the record, sends it to the broker async, then invokes `callback.onCompletion(metadata, exception)` on the producer's I/O thread:

- On success: `metadata` is the broker-assigned offset/timestamp, `exception` is `null`.
- On failure: `metadata` is `null` (or has `topic.partition` for partition-level info), `exception` is the failure reason.

If your callback ignores `exception`, the failure is dropped. The Future also captures it, but unless you call `Future.get()`, you never see it. So:

```java
// All of these are equivalent: error is silently dropped
producer.send(record, (md, ex) -> {});
producer.send(record, (md, ex) -> log.info("sent {}", md));
producer.send(record, (md, ex) -> meter.counter("sent").increment());
```

The bytecode pattern: `INVOKEDYNAMIC` to `LambdaMetafactory.metafactory` produces a synthetic method. The synthetic method's bytecode either has no `getfield` / `aload` on the exception parameter, or only an `aload` followed by `ALOAD/POP` (dead).

## Why this is subtle

- The lambda *is* a callback. It satisfies the type signature, IDE warnings disappear.
- Tests pass because the test broker delivers successfully.
- Code review approves: "good, the callback is there."
- Even careful reviewers can miss "the exception parameter is unused" — IDEs don't flag unused lambda parameters by default.
- Without metrics or logging on the exception path, the failure is invisible at runtime too.

## Operational impact

- Send failures invisible.
- Topic partition leader elections cause silent drops if `acks=0` or `acks=1` and `retries=0`.
- Producer-side metrics (record-error-rate) might still show the error, but only if metric reporters are wired (see `OBS_NO_METRIC_REPORTERS`).
- Audit chains break: the audit interceptor logs "sent" but the broker rejected it.

## Failure scenarios (walkthrough)

1. **The schema-registry outage.** Schema Registry is unreachable for 5 minutes. Every send → callback with `RestClientException`. Empty lambda → silent drop. Application logs show "produced 12,000 records" while the broker received zero.

2. **The wrong topic name.** Producer code has a typo: writes to `orders` instead of `order`. Broker rejects with `UnknownTopicOrPartitionException` (or auto-creates a 1-partition shadow). Empty lambda → silent. Operator finds nothing in the right topic, eventually finds the wrong one.

3. **The TLS handshake failure.** Cert rotation breaks producer TLS. Every send fails. Empty lambda → silent. Lag dashboard on the consumer side suddenly shows "lag = 0, no traffic" — the wrong reason.

## How to fix

```java
// BAD — empty lambda
producer.send(record, (md, ex) -> {});

// BAD — logs success but ignores ex
producer.send(record, (md, ex) -> log.info("sent {}", md));

// GOOD — handle the error
producer.send(record, (md, ex) -> {
    if (ex != null) {
        log.error("Failed to send to {}", record.topic(), ex);
        meter.counter("producer.error", "topic", record.topic(),
            "exception", ex.getClass().getSimpleName()).increment();
        // optional: rethrow / mark a future / re-enqueue
    }
});

// ALSO GOOD — let it throw if you want sync semantics
try {
    producer.send(record).get();  // blocks, throws on failure (but see PRODUCER_SEND_BLOCKING_GET)
} catch (ExecutionException e) { ... }
```

## When this might be a false positive

- The lambda body genuinely doesn't need to act on the exception because a `ProducerInterceptor` handles all error reporting via metrics — but verify the interceptor is wired and actually counts failures.
- Test fixture / one-shot CLI tool where send failure is fine to ignore.
- A `BiConsumer` reference like `Callback::ignore` (rare, but possible).

## Detection strategy

- bytecode: visit `KafkaProducer.send(ProducerRecord;Callback)` call sites.
- Resolve the second argument:
  - If it's a method reference, inspect the target.
  - If it's a lambda (`INVOKEDYNAMIC + LambdaMetafactory.metafactory`), follow to the synthetic method.
- In the synthetic method, count instructions. If body is `RETURN`/`ARETURN` only (empty), flag.
- Otherwise, check whether parameter index 1 (the `Exception`) is ever loaded (`ALOAD 1`). If not, flag.
- If loaded only by a `LOG.info`/`LOG.debug` call (look at method name on the receiver `Logger`), flag with lower confidence.
- Confidence HIGH for empty body, MEDIUM for logged-without-rethrow.

## References

- Apache Kafka — `Callback` interface: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/Callback.html
- Apache Kafka — `KafkaProducer.send` semantics: https://kafka.apache.org/documentation/#producerapi
- Related rule: `PRODUCER_SEND_NO_CALLBACK` (no callback at all)
- Related rule: `PRODUCER_SEND_BLOCKING_GET` (callback + .get())
