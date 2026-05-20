# ASYNC_LISTENER_MONO_BLOCK

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `Mono.block()` inside a `@KafkaListener` defeats reactive — and the error path with it.

## TL;DR

The linter flags `@KafkaListener`-annotated methods (Spring) whose body contains `Mono.block()`, `Mono.blockOptional()`, `Flux.blockFirst()`, `Flux.blockLast()`, or equivalent. The listener thread is already a real OS thread managed by the container's `MessageListenerContainer` — blocking on a reactive type defeats the purpose of choosing reactive in the first place. More importantly: reactive error signals (`Mono.error`, `Flux.error`) become unchecked exceptions thrown out of `block()`, which is correct *only if* the listener method's try/catch correctly propagates them — and many such methods catch and log (see `SPRING_LISTENER_SWALLOWS_EXCEPTION`).

## The setup

Engineer wraps a reactive WebClient call inside their `@KafkaListener`. To make the listener wait for the response, they call `.block()`. The IDE doesn't warn (Reactor's APIs are fine to call from a non-reactive thread). The listener works. Performance is fine because the listener container can be configured for high concurrency. But the team now has a "reactive" downstream call that isn't doing them any reactive favors.

## What's actually happening

A `@KafkaListener` method is invoked on a thread provided by `ConcurrentMessageListenerContainer` (or the underlying `KafkaMessageListenerContainer`). These threads are pooled OS threads. They are not Reactor schedulers, not event loops, not coroutines.

When you do:

```java
Mono<Order> reactiveCall = webClient.get().uri(...).retrieve().bodyToMono(Order.class);
Order order = reactiveCall.block();  // blocking call
```

You're explicitly opting out of reactive semantics:
- The listener thread blocks for the duration of the call.
- `WebClient`'s connection pool is still reactive — but the thread that's waiting is not.
- Reactor schedulers see this as legitimate blocking on a non-elastic scheduler.
- Reactor's `BlockingOperationError` check (when enabled) will throw at runtime.

The right tools for the situation:

- **If you want async/non-blocking under high load:** make the listener return `Mono`/`CompletableFuture` (Spring Kafka 3.0+ supports async return types with the `AsyncListenerContainerFactory`).
- **If you want sync semantics:** call the blocking version of the API. Don't go reactive just to `.block()`.

## Why this is subtle

- `Mono.block()` is a documented public API. It's not deprecated. Using it is "allowed."
- Hybrid codebases (Spring MVC at the edge, WebFlux clients inside) make `WebClient` the path of least resistance, and `.block()` is the bridge.
- Performance often looks fine in tests because contention is low.
- The error-propagation issue (Mono.error → block throws → listener body decides) is harder to spot in code review.
- Spring's own `ReactiveKafkaConsumerTemplate` exists for properly-reactive Kafka consumption — but is a separate beast from `@KafkaListener`.

## Operational impact

- One blocked listener thread per in-flight reactive call. Container concurrency caps throughput.
- `block()` rethrows reactive errors as runtime exceptions. If the listener body catches `Exception` and logs (rule `SPRING_LISTENER_SWALLOWS_EXCEPTION`), the error vanishes.
- Reactor's `Schedulers.parallel()` threads (used by some operators internally) get blocked, triggering `BlockingOperationError` if enabled.
- Profilers attribute time to the reactive pipeline rather than the underlying I/O.

## Failure scenarios (walkthrough)

1. **The pool-starvation cascade.** Listener concurrency is 5 (default). Each listener call does `webClient.x().block()`. Downstream becomes slow. All 5 threads block on `block()`. Lag grows. New replicas added — they also block. The "reactive" downstream is single-purpose-blocking.

2. **The swallowed error.** `Mono.error(new TimeoutException())` from a WebClient call. `block()` rethrows `TimeoutException`. Listener body catches `Exception`, logs, returns. Record committed. Order lost.

## How to fix

Pick a model and stick to it.

```java
// BAD — reactive call, blocked
@KafkaListener(topics = "orders")
public void handle(Order order) {
    Order enriched = webClient.get().uri(...).bodyToMono(Order.class).block();
    processor.process(enriched);
}

// GOOD — sync client, sync listener
@KafkaListener(topics = "orders")
public void handle(Order order) {
    Order enriched = restTemplate.getForObject(uri, Order.class);  // or RestClient in Spring 6.1+
    processor.process(enriched);
}

// GOOD — async listener (Spring Kafka 3.0+)
@KafkaListener(topics = "orders")
public Mono<Void> handle(Order order) {
    return webClient.get().uri(...).bodyToMono(Order.class)
        .map(processor::process)
        .then();
}

// GOOD — ReactiveKafkaConsumerTemplate (different model entirely)
public Flux<ReceiverRecord<String, Order>> consume() {
    return reactiveKafkaConsumerTemplate.receive()
        .flatMap(rec -> webClient.get()...);
}
```

## When this might be a false positive

- One-shot `Mono.fromCallable(() -> ...).block()` wrapping a non-reactive call (mostly pointless, but not the listed anti-pattern).
- Initialization code that's not the listener body (`@PostConstruct`).
- A custom container factory that dispatches the listener on a different scheduler — verify before suppressing.

## Detection strategy

- bytecode: visit `@KafkaListener` methods.
- Walk the method body (and inlined lambdas) for INVOKE on `reactor.core.publisher.Mono.block`, `Mono.blockOptional`, `Flux.blockFirst`, `Flux.blockLast`, `Flux.toIterable`.
- Also flag `Mono.subscribe()` with no Disposable handling (fire-and-forget reactive in a sync method).
- Confidence HIGH — descriptors are exact.

## References

- Reactor — Blocking guidelines: https://projectreactor.io/docs/core/release/reference/#faq.wrap-blocking
- Spring Kafka — async listener return types: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html
- Spring `ReactiveKafkaConsumerTemplate`: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/core/reactive/ReactiveKafkaConsumerTemplate.html
- Related: `SPRING_LISTENER_SWALLOWS_EXCEPTION`
