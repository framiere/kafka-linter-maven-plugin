# LAMBDA_SEND_GET_WITH_CALLBACK

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `producer.send(record, callback).get()` is one too many error-handling paths. Pick one.

## TL;DR

The linter flags `KafkaProducer.send(record, callback).get()` patterns — a callback AND a blocking `get()` on the same send. This expresses two contradictory intents: "I want async error handling via the callback" AND "I want to block until completion and throw on failure." The blocking `get()` makes the callback redundant (any exception is already raised) and the callback overhead is wasted. Worse, if both touch shared state, you can race.

## The setup

Team writes a producer wrapper. They add a callback "for observability" (counter increment, span close). They also call `.get()` "to make sure the send actually happens before the request returns" (typical inside an HTTP handler). The two combine: callback fires *and* `get()` throws. Now the same error is handled twice.

## What's actually happening

`send(record, callback)` returns `Future<RecordMetadata>`. The callback is invoked on the producer I/O thread when the broker responds. `Future.get()` blocks the caller until the producer I/O thread has either set the result or wrapped the exception in `ExecutionException`.

When both are used:

```java
producer.send(record, (md, ex) -> { meter.count("sent"); }).get();
```

Sequence on failure:
1. Broker rejects with `RecordTooLargeException`.
2. Producer I/O thread invokes callback: `meter.count("sent")` increments. (Probably wrong — the metric counted a non-sent record.)
3. Producer I/O thread sets the `Future` exception.
4. Calling thread's `get()` throws `ExecutionException(RecordTooLargeException)`.
5. Calling thread's try/catch handles the exception.

So the failure is handled by the calling thread (the right place) AND the callback ran (probably emitting a wrong metric or a no-op span).

On success the duplication is harmless but wasteful — every record incurs two `ALLOC`s (lambda + future result) for one logical send.

## Why this is subtle

- Both calls look reasonable in isolation. Combined they're redundant.
- The callback's "observability" intent collides with the sync semantics introduced by `get()`.
- If the callback closes a tracing span, the span is closed before the calling thread's catch block runs — span/exception association can be wrong.
- The pattern often arrives via incremental refactoring: someone adds `.get()` for one-shot ordering guarantees, forgets to remove the callback.

## Operational impact

- Metrics over-count or under-count depending on what the callback does.
- Tracing spans close at the wrong moment.
- Twice the per-send allocation cost.
- Confused error handling: which path "owns" the failure?

## Failure scenarios (walkthrough)

1. **The double-count metric.** Callback increments `kafka.produced.total`. `get()` throws on failure. The catch block also increments `kafka.failed.total`. Total = produced + failed = 2 × actual sends. Dashboard shows traffic at 2× reality.

2. **The closed span.** Callback closes a "producer-send" span. `get()` throws. Calling thread catches and logs — but the active span is now the *parent* span, so the log line is attributed to the parent operation, not the failed send.

## How to fix

Pick one path.

```java
// BAD
producer.send(record, (md, ex) -> { /* something */ }).get();

// GOOD — sync with .get(), no callback (let the try/catch own it)
try {
    producer.send(record).get();
    meter.count("sent");
} catch (ExecutionException e) {
    log.error("send failed", e.getCause());
    meter.count("failed");
    throw e;
}

// GOOD — async with callback, no .get()
producer.send(record, (md, ex) -> {
    if (ex == null) meter.count("sent");
    else { log.error("send failed", ex); meter.count("failed"); }
});

// BEST for most apps — async with callback, separate flush before close
producer.send(record, observabilityCallback);
// ... at shutdown:
producer.flush();
producer.close();
```

## When this might be a false positive

- The callback's purpose is to capture `metadata.offset()` into a downstream pipeline (legitimately needs the metadata), AND the `.get()` is for back-pressure. Even then, prefer `.get()` and use the returned metadata.

## Detection strategy

- bytecode: look for `send(record, callback)` followed by `.get()` on the returned `Future`.
- Pattern: `INVOKEVIRTUAL KafkaProducer.send (...;...)Ljava/util/concurrent/Future;` then immediate or DUP'd-then-`INVOKEINTERFACE Future.get`.
- Confidence HIGH — the pattern is exact in bytecode.

## References

- Apache Kafka — `KafkaProducer.send(record, callback)`: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
- Related: `PRODUCER_SEND_BLOCKING_GET` (no callback, with .get())
- Related: `PRODUCER_SEND_NO_CALLBACK` (no callback at all)
