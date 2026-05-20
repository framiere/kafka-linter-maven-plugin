# kafka-clients rules

Build-time anti-pattern checks for plain `org.apache.kafka:kafka-clients` producer / consumer code. Rules combine bytecode analysis (ASM) of compiled classes with parsing of property sources (`application.properties`, `application.yml`, JVM `Properties` literals). Targets Kafka 3.x and 4.x; defaults referenced are those of Kafka 3.0+ unless noted.

Severity:
- **ERROR** — almost always a bug. Will throw `ConfigException` / `IllegalStateException` at runtime, or cause silent data loss.
- **WARNING** — usually a bug; legitimate edge cases exist.

Confidence:
- **HIGH** — the linter can prove the antipattern from a single constant value.
- **MEDIUM** — heuristic / cross-key inference; some false positives expected.
- **CONTEXT** — depends on environment (managed cluster, deployment model) that cannot be known statically.

Detection:
- **bytecode** — ASM scan of `.class` files for API calls / lambdas / try-with-resources.
- **config-file** — text parsing of property files (`*.properties`, `*.yml`, framework-prefixed keys).
- **both** — applies to either source.

## Catalog

### Existing rules (already implemented)

| Rule ID | Severity | Conf. | Detection | Tagline |
|---|---|---|---|---|
| PRODUCER_IN_LOOP | ERROR | HIGH | bytecode | Don't put a producer constructor inside a tight loop. |
| CONSUMER_IN_LOOP | ERROR | HIGH | bytecode | Don't put a consumer constructor inside a tight loop. |
| PRODUCER_NO_COMPRESSION | WARNING | MEDIUM | both | No compression set leaves bandwidth on the floor. |
| PRODUCER_SEND_BLOCKING_GET | WARNING | HIGH | bytecode | send().get() turns async into the slowest synchronous loop you've ever written. |
| PRODUCER_SEND_NO_CALLBACK | WARNING | HIGH | bytecode | send() without a callback is "I don't want to know if it failed." |
| PRODUCER_FLUSH_IN_LOOP | WARNING | HIGH | bytecode | flush() in a loop is "I undid the batching myself." |
| CONSUMER_AUTO_COMMIT_TRUE | WARNING | HIGH | both | enable.auto.commit=true commits before you process. |
| CONSUMER_COMMIT_PER_RECORD | WARNING | HIGH | bytecode | Committing every record is one Kafka roundtrip per message. |
| CONSUMER_POLL_ZERO | ERROR | HIGH | bytecode | poll(0) is a no-op masquerading as work. |

### New rules

| Rule ID | Severity | Conf. | Detection | Tagline |
|---|---|---|---|---|
| [PRODUCER_ACKS_ZERO](./PRODUCER_ACKS_ZERO.md) | ERROR | HIGH | both | acks=0 is fire-and-pray. |
| [PRODUCER_ACKS_ONE](./PRODUCER_ACKS_ONE.md) | WARNING | MEDIUM | both | acks=1 means "the leader saw it" — and then the leader died. |
| [PRODUCER_IDEMPOTENCE_DISABLED](./PRODUCER_IDEMPOTENCE_DISABLED.md) | WARNING | HIGH | both | Turning idempotence off in 2026 is undoing five years of work the client did for you. |
| [PRODUCER_MAX_IN_FLIGHT_TOO_HIGH](./PRODUCER_MAX_IN_FLIGHT_TOO_HIGH.md) | ERROR | HIGH | both | The broker only remembers your last 5 sequences. Sixth one rewrites history. |
| [PRODUCER_RETRIES_ZERO](./PRODUCER_RETRIES_ZERO.md) | WARNING | MEDIUM | both | retries=0 means every transient network blip is a permanent failure. |
| [PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE](./PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE.md) | ERROR | HIGH | both | A transactional.id without idempotence is a transaction that isn't. |
| [PRODUCER_LINGER_ZERO_NO_BATCH](./PRODUCER_LINGER_ZERO_NO_BATCH.md) | WARNING | MEDIUM | config-file | linger.ms=0 with default batch.size means every record is a network round trip. |
| [PRODUCER_COMPRESSION_NONE_EXPLICIT](./PRODUCER_COMPRESSION_NONE_EXPLICIT.md) | WARNING | MEDIUM | both | compression.type=none isn't a default — it's a choice. Make sure you meant it. |
| [PRODUCER_DEPRECATED_PARTITIONER](./PRODUCER_DEPRECATED_PARTITIONER.md) | WARNING | HIGH | both | DefaultPartitioner is older than your phone. Stop naming it. |
| [CLIENT_ID_MISSING](./CLIENT_ID_MISSING.md) | WARNING | MEDIUM | both | No client.id is a faceless caller — broker logs see "producer-1" and so do you. |
| [PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL](./PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL.md) | ERROR | HIGH | config-file | delivery.timeout.ms < request.timeout.ms + linger.ms doesn't even start. |
| [PRODUCER_MAX_BLOCK_MS_ZERO](./PRODUCER_MAX_BLOCK_MS_ZERO.md) | ERROR | HIGH | both | max.block.ms=0 means "fail before you even ask the broker." |
| [BOOTSTRAP_SERVERS_SINGLE_BROKER](./BOOTSTRAP_SERVERS_SINGLE_BROKER.md) | WARNING | HIGH | config-file | Bootstrap with one host and you're one DNS blip from no Kafka at all. |
| [PRODUCER_BUFFER_MEMORY_MISCONFIG](./PRODUCER_BUFFER_MEMORY_MISCONFIG.md) | WARNING | MEDIUM | config-file | buffer.memory below batch.size is "I asked for back-pressure on every send." |
| [REQUEST_TIMEOUT_LT_REPLICA_LAG](./REQUEST_TIMEOUT_LT_REPLICA_LAG.md) | WARNING | MEDIUM | config-file | A request timeout shorter than replica lag is an SLA against physics. |
| [SECURITY_PROTOCOL_PLAINTEXT_REMOTE](./SECURITY_PROTOCOL_PLAINTEXT_REMOTE.md) | WARNING | CONTEXT | config-file | Plaintext to a hostname you don't own is a credential leak in a green box. |
| [CONSUMER_AUTO_OFFSET_RESET_LATEST](./CONSUMER_AUTO_OFFSET_RESET_LATEST.md) | WARNING | MEDIUM | both | auto.offset.reset=latest is "skip the backlog you didn't know you had." |
| [CONSUMER_AUTO_OFFSET_RESET_NONE_UNHANDLED](./CONSUMER_AUTO_OFFSET_RESET_NONE_UNHANDLED.md) | WARNING | MEDIUM | bytecode | auto.offset.reset=none is great — if you handle the exception. |
| [CONSUMER_MAX_POLL_RECORDS_TOO_HIGH](./CONSUMER_MAX_POLL_RECORDS_TOO_HIGH.md) | WARNING | MEDIUM | config-file | A batch you can't process in max.poll.interval.ms is a batch you'll process twice. |
| [CONSUMER_HEARTBEAT_SESSION_RATIO](./CONSUMER_HEARTBEAT_SESSION_RATIO.md) | WARNING | HIGH | config-file | Heartbeats should be 1/3 of the session — two misses, you're still alive. |
| [CONSUMER_MAX_POLL_INTERVAL_TOO_LOW](./CONSUMER_MAX_POLL_INTERVAL_TOO_LOW.md) | WARNING | MEDIUM | config-file | Lower max.poll.interval.ms than your slowest record and you've built a rebalance perpetual motion machine. |
| [CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN](./CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN.md) | WARNING | MEDIUM | config-file | read_uncommitted from a transactional topic is reading the draft, not the final. |
| [CONSUMER_GROUP_INSTANCE_ID_MISSING](./CONSUMER_GROUP_INSTANCE_ID_MISSING.md) | WARNING | CONTEXT | bytecode | Without group.instance.id, every rolling restart is a full rebalance you didn't need. |
| [ALLOW_AUTO_CREATE_TOPICS_TRUE](./ALLOW_AUTO_CREATE_TOPICS_TRUE.md) | WARNING | HIGH | config-file | Auto-creating topics from the consumer makes typos into infrastructure. |
| [PRODUCER_NOT_CLOSED](./PRODUCER_NOT_CLOSED.md) | ERROR | HIGH | bytecode | An unclosed producer is unflushed buffers waiting for JVM shutdown to lose them. |
| [CONSUMER_NOT_CLOSED](./CONSUMER_NOT_CLOSED.md) | WARNING | HIGH | bytecode | If you don't close the consumer, the group waits 30s+ to forget you. |
| [CONSUMER_NOT_THREAD_SAFE](./CONSUMER_NOT_THREAD_SAFE.md) | ERROR | MEDIUM | bytecode | Kafka consumers are single-threaded. Share one and the JVM tells you so. |
| [CONSUMER_NO_WAKEUP_SHUTDOWN](./CONSUMER_NO_WAKEUP_SHUTDOWN.md) | WARNING | MEDIUM | bytecode | Without wakeup(), poll() never gets the memo to stop. |
| [PROPERTIES_MUTATED_AFTER_CTOR](./PROPERTIES_MUTATED_AFTER_CTOR.md) | WARNING | HIGH | bytecode | KafkaProducer reads your Properties once. After that you're talking to yourself. |
| [PRODUCER_RECORD_PARTITION_AND_KEY](./PRODUCER_RECORD_PARTITION_AND_KEY.md) | WARNING | MEDIUM | bytecode | Specifying both partition and key means you don't trust the key. Pick one. |
| [POLL_IN_REBALANCE_CALLBACK](./POLL_IN_REBALANCE_CALLBACK.md) | ERROR | HIGH | bytecode | Calling poll() inside onPartitionsRevoked is asking the consumer to interrupt itself. |
| [COMMIT_ASYNC_NO_FINAL_SYNC](./COMMIT_ASYNC_NO_FINAL_SYNC.md) | WARNING | MEDIUM | bytecode | commitAsync is fast and forgetful — pair it with one commitSync on the way out. |
| [PRODUCER_USED_AFTER_CLOSE](./PRODUCER_USED_AFTER_CLOSE.md) | ERROR | HIGH | bytecode | A closed producer answers "no" to every send. Forever. |
| [PRODUCER_PER_RECORD_ALLOCATION](./PRODUCER_PER_RECORD_ALLOCATION.md) | ERROR | HIGH | bytecode | Producers are oxygen — share one, breathe easy. |
| [CONSUMER_SEEK_BEFORE_POLL](./CONSUMER_SEEK_BEFORE_POLL.md) | ERROR | MEDIUM | bytecode | seek() before poll() is asking to skip to a chapter of a book you haven't opened. |
| [CONSUMER_ASSIGN_AND_SUBSCRIBE](./CONSUMER_ASSIGN_AND_SUBSCRIBE.md) | ERROR | HIGH | bytecode | Pick one. assign() and subscribe() are mutually exclusive — Kafka will tell you. |
| [HEADERS_SENSITIVE_KEYS](./HEADERS_SENSITIVE_KEYS.md) | WARNING | MEDIUM | bytecode | Kafka headers are plaintext — don't put your password in them. |
| [AVRO_SPECIFIC_READER_MISSING](./AVRO_SPECIFIC_READER_MISSING.md) | WARNING | HIGH | both | Generic Avro on a specific topic gives you back GenericRecord — and a ClassCastException later. |
| [SCHEMA_REGISTRY_URL_MISSING](./SCHEMA_REGISTRY_URL_MISSING.md) | ERROR | HIGH | both | A Schema Registry serializer without a URL is just an exception generator. |
| [STRING_SERIALIZER_NON_STRING](./STRING_SERIALIZER_NON_STRING.md) | WARNING | MEDIUM | bytecode | StringSerializer with a non-String value is a UTF-8 encoder pretending to be a serializer. |

## Notes for the implementer

- Most ERROR rules either throw `ConfigException` at startup or cause silent data loss; the linter should fail the build by default for these and let users downgrade.
- WARNING rules with CONTEXT confidence (e.g. `SECURITY_PROTOCOL_PLAINTEXT_REMOTE`, `CONSUMER_GROUP_INSTANCE_ID_MISSING`) should default to print-only and be opt-in for build failure.
- Several rules need cross-key inference inside a single Properties source (`PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL`, `CONSUMER_HEARTBEAT_SESSION_RATIO`, `PRODUCER_BUFFER_MEMORY_MISCONFIG`); the existing `RuleContext` will need to expose a "Properties group" abstraction.
- Several bytecode rules need lifecycle analysis on the same field/local (`PRODUCER_NOT_CLOSED`, `CONSUMER_NOT_CLOSED`, `PRODUCER_USED_AFTER_CLOSE`, `PROPERTIES_MUTATED_AFTER_CTOR`); reuse the `LambdaTracker` / `LoopFinder` scaffolding and add a small `RefLifetimeTracker`.
- Framework-aware suppression (Spring `@KafkaListener`, Quarkus `@Incoming`, `KafkaTemplate`, Reactor-Kafka) should be a project-wide flag — many rules drop to MEDIUM or are suppressed entirely under those.
