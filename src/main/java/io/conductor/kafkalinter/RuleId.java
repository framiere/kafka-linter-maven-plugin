package io.conductor.kafkalinter;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Identity + didactic metadata for a single lint rule.
 *
 * <p>This is a value class, not an enum: the documented catalog (see
 * {@code docs/rules/_CATALOG.md}) has hundreds of rule IDs while the plugin
 * implements a curated, high-confidence subset. Each constant below registers
 * itself in a process-wide registry so that {@link #valueOf(String)} and
 * {@link #values()} keep working from the call sites that the previous enum
 * form had.
 *
 * <p>Equality is by {@link #id()} — two {@code RuleId} instances with the same
 * id compare equal regardless of metadata.
 *
 * <p>Each rule carries a four-paragraph didactic block that the verbose
 * reporter prints:
 * <ul>
 *   <li>{@link #tagline()} — the one-line takeaway.</li>
 *   <li>{@link #mechanism()} — what's actually happening at the protocol / runtime level.</li>
 *   <li>{@link #impact()} — the concrete operational consequence (broker load, message loss, etc).</li>
 *   <li>{@link #whyMatters()} — why this is easy to miss in code review and what makes the fix worth it.</li>
 * </ul>
 */
public final class RuleId {

    private static final Map<String, RuleId> REGISTRY = new LinkedHashMap<>();

    // ────────────────────────────────────────────────────────────────────────
    // kafka-clients — producer / consumer hot-path & lifecycle
    // ────────────────────────────────────────────────────────────────────────

    public static final RuleId PRODUCER_IN_LOOP = register(builder("PRODUCER_IN_LOOP")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_IN_LOOP.md")
            .message("KafkaProducer instantiated inside a loop or iterating lambda — producers must be long-lived.")
            .tagline("Producers are heavy objects. Build once, share across the app.")
            .mechanism("Constructing a KafkaProducer opens TCP connections to bootstrap servers, fetches cluster metadata, allocates the record-accumulator buffer (32 MB by default), and starts the sender thread.")
            .impact("Per-iteration creation thrashes the broker (one metadata-fetch and TCP handshake per loop iteration), exhausts file descriptors, and prevents any batching. End-to-end throughput collapses by 100–1000x.")
            .whyMatters("Looks innocent in code review (just `new KafkaProducer(...)`), but the cost shape is invisible from the call site. The right shape is a singleton-style producer owned by the application lifecycle.")
            .build());

    public static final RuleId CONSUMER_IN_LOOP = register(builder("CONSUMER_IN_LOOP")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_IN_LOOP.md")
            .message("KafkaConsumer instantiated inside a loop or iterating lambda — consumers must be long-lived.")
            .tagline("Consumers carry group membership and fetch state. Don't recreate them.")
            .mechanism("KafkaConsumer construction joins the consumer group (rebalance), establishes coordinator + fetcher connections, and seeks to the committed offset. None of this is cheap.")
            .impact("Looping the constructor triggers a group rebalance every iteration — every other group member pauses while the join completes. Throughput drops to zero for the whole group, not just this app.")
            .whyMatters("The rebalance storm shows up as broker-side CPU and group-coordinator overload, not as a clear app-side error. Singletons are the only shape that scales.")
            .build());

    public static final RuleId PRODUCER_NO_COMPRESSION = register(builder("PRODUCER_NO_COMPRESSION")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_NO_COMPRESSION.md")
            .message("KafkaProducer constructed without setting 'compression.type'.")
            .tagline("Uncompressed Kafka traffic is 3-5× more bandwidth, disk, and replication cost than it needs to be.")
            .mechanism("Without explicit `compression.type`, the producer defaults to `none`: every record-batch travels uncompressed to the broker and is replicated uncompressed across the ISR.")
            .impact("Bytes-on-wire and bytes-on-disk both grow 3-5× for typical JSON/Avro payloads. At scale this dominates the broker's storage cost and cross-AZ network bill.")
            .whyMatters("Compression is one config key (`zstd` is the modern default; `lz4` if CPU-bound). The producer absorbs the CPU cost in the background sender thread, not on the user's hot path.")
            .build());

    public static final RuleId PRODUCER_SEND_BLOCKING_GET = register(builder("PRODUCER_SEND_BLOCKING_GET")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_SEND_BLOCKING_GET.md")
            .message("producer.send(record).get() — defeats async batching. Use a Callback instead.")
            .tagline("Calling .get() collapses the producer to one record at a time. No batching, no pipelining.")
            .mechanism("The producer batches records in the accumulator and the sender thread drains them in async ProduceRequests. `.get()` on the returned Future blocks the caller until this specific record's ACK lands.")
            .impact("Throughput drops from tens-of-thousands of records/sec to roughly `1 / RTT`. On a 5 ms cross-AZ RTT, that's ~200 records/sec — three orders of magnitude worse.")
            .whyMatters("The async API is a contract: the Future is for completion notification, not for waiting. Use a `Callback` (or future composition) so the sender thread keeps batching while your code moves on.")
            .build());

    public static final RuleId PRODUCER_SEND_NO_CALLBACK = register(builder("PRODUCER_SEND_NO_CALLBACK")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_SEND_NO_CALLBACK.md")
            .message("producer.send(record) without a Callback and result discarded — send errors will be silently swallowed.")
            .tagline("Discarding the Future from send() drops broker-side errors on the floor.")
            .mechanism("send() returns a Future<RecordMetadata>. If you ignore it AND don't pass a Callback, exceptions raised in the sender thread (serialization, broker NACK, timeout) never reach your code.")
            .impact("Failures appear as missing records downstream — no log line, no metric, no exception. Debugging requires comparing producer-side counts against consumer-side counts.")
            .whyMatters("A `Callback` of two lines (log on `exception != null`, increment a metric) turns the silent-loss class of bugs into a noisy, observable one.")
            .build());

    public static final RuleId PRODUCER_FLUSH_IN_LOOP = register(builder("PRODUCER_FLUSH_IN_LOOP")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_FLUSH_IN_LOOP.md")
            .message("producer.flush() called inside a loop — defeats batching.")
            .tagline("flush() forces every accumulator drain. Inside a loop, that's one ProduceRequest per record.")
            .mechanism("flush() blocks until all queued records have been ACKed. The sender thread sends out partial batches immediately instead of waiting for `batch.size` or `linger.ms`.")
            .impact("Same shape as `.send().get()` in the limit: throughput pinned at `1/RTT`. You also pay an extra context switch per record.")
            .whyMatters("flush() has exactly one correct call site: just before close(), to make sure in-flight records get a chance to land. Anywhere else, it's a bug.")
            .build());

    public static final RuleId CONSUMER_AUTO_COMMIT_TRUE = register(builder("CONSUMER_AUTO_COMMIT_TRUE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_AUTO_COMMIT_TRUE.md")
            .message("KafkaConsumer configured with enable.auto.commit=true — risks message loss or double-processing.")
            .tagline("Auto-commit fires on a timer regardless of whether processing actually succeeded.")
            .mechanism("With `enable.auto.commit=true`, the consumer commits offsets every `auto.commit.interval.ms` (5 s default) from the last poll, independent of what your code did with those records.")
            .impact("Two failure shapes: (a) commit fires before processing finishes → records re-processed on restart (at-least-once is fine; but if your processing isn't idempotent it's double-billing). (b) commit fires after processing succeeded but the next batch fails → uncommitted records reprocessed.")
            .whyMatters("The right shape is `enable.auto.commit=false` + explicit `commitSync()` after the per-batch processing block. That ties commit to *completed work*, not to a wall-clock timer.")
            .build());

    public static final RuleId CONSUMER_COMMIT_PER_RECORD = register(builder("CONSUMER_COMMIT_PER_RECORD")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_COMMIT_PER_RECORD.md")
            .message("commitSync() called inside the per-record loop of a poll() — kills consumer throughput.")
            .tagline("commitSync per record means one synchronous round-trip per record.")
            .mechanism("commitSync() is a network call to the group coordinator. In the per-record loop, every record incurs a coordinator RTT before the next one is processed.")
            .impact("Effective throughput pinned at `1/RTT_to_coordinator` — same order of magnitude as `.send().get()`. A consumer that could do 50k records/sec ends up doing ~200.")
            .whyMatters("Commit per batch (after the for-each on `poll()`'s result), or commit asynchronously with `commitAsync(callback)` and reconcile with a final `commitSync()` on close. The cost-vs-correctness tradeoff is real but the per-record shape is the worst of both worlds.")
            .build());

    public static final RuleId CONSUMER_POLL_ZERO = register(builder("CONSUMER_POLL_ZERO")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_POLL_ZERO.md")
            .message("consumer.poll(0) / poll(Duration.ZERO) — busy-loops the consumer thread.")
            .tagline("poll(0) returns immediately whether records are available or not.")
            .mechanism("`poll(Duration)` is the consumer's wait-or-fetch primitive. With `Duration.ZERO`, the consumer returns immediately on an empty fetcher — no records, no wait.")
            .impact("The consumer thread spins at 100% CPU pulling on an empty fetcher. On idle topics this is invisible in functional tests and shows up only as 'why is the box hot?' in production.")
            .whyMatters("Use a real timeout (typically `Duration.ofMillis(100-500)`). The consumer needs the wait window to deliver records efficiently; instantaneous polls defeat the fetcher's prefetch.")
            .build());

    public static final RuleId PRODUCER_ACKS_ZERO = register(builder("PRODUCER_ACKS_ZERO")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_ACKS_ZERO.md")
            .message("Producer configured with acks=0 — fire-and-forget. Any broker failure during send is silent message loss.")
            .tagline("acks=0 means the producer does NOT wait for any broker acknowledgement. Records can be lost without any error.")
            .mechanism("With `acks=0`, the producer writes to its TCP socket and returns success immediately. The broker may never receive the record (network drop, broker GC, leader election in progress) and the producer never learns of it.")
            .impact("Silent data loss during any broker hiccup. The producer's success metrics lie — they report send-attempts, not durable writes.")
            .whyMatters("The default is `acks=all` (since Kafka 3.0) and that's almost always what you want. `acks=1` is a tunable middle ground; `acks=0` is for benchmarks and metrics shippers where loss is acceptable. If you're not certain that's you, don't use it.")
            .build());

    public static final RuleId PRODUCER_ACKS_ONE = register(builder("PRODUCER_ACKS_ONE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_ACKS_ONE.md")
            .message("Producer configured with acks=1 — record durability depends only on the partition leader.")
            .tagline("acks=1 means 'the leader saw it' — and then the leader died.")
            .mechanism("With `acks=1`, the producer waits for the leader to write to its local log but does NOT wait for replication to ISR followers. If the leader crashes before the follower catches up, the un-replicated record is gone.")
            .impact("Window of data loss equal to the leader's replication lag — typically tens of milliseconds, but unbounded during follower outages. Looks like working durability on the happy path, fails open during the failure modes that matter.")
            .whyMatters("`acks=all` (the default since Kafka 3.0) waits for `min.insync.replicas` followers — that's the threshold the operator already configured. Choosing `acks=1` is a deliberate trade of durability for ~1ms latency; only valid if you've measured both and accept the loss budget.")
            .build());

    public static final RuleId PRODUCER_RETRIES_ZERO = register(builder("PRODUCER_RETRIES_ZERO")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_RETRIES_ZERO.md")
            .message("Producer configured with retries=0 — every transient broker error becomes a permanent send failure.")
            .tagline("retries=0 means every transient network blip is a permanent failure.")
            .mechanism("Most broker-side errors during produce are transient: leader election in progress (`NOT_LEADER_FOR_PARTITION`), follower out of sync (`NOT_ENOUGH_REPLICAS`), broker GC (`REQUEST_TIMED_OUT`). The producer's retry loop handles all of them transparently when `retries > 0`.")
            .impact("With retries=0 every retryable error surfaces to the caller's Callback as a hard failure. Application code is forced to re-implement Kafka's own retry logic — usually badly, often with broken ordering guarantees.")
            .whyMatters("Default is `Integer.MAX_VALUE` for a reason. The relevant upper bound is `delivery.timeout.ms` (default 120s), not the retry count itself. If you're tempted to set retries=0 because of ordering, the right answer is `enable.idempotence=true` instead.")
            .build());

    public static final RuleId PRODUCER_COMPRESSION_NONE_EXPLICIT = register(builder("PRODUCER_COMPRESSION_NONE_EXPLICIT")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_COMPRESSION_NONE_EXPLICIT.md")
            .message("Producer explicitly sets compression.type=none — 3-5× more bytes on the wire than zstd/lz4.")
            .tagline("compression.type=none isn't a default — it's a choice. Make sure you meant it.")
            .mechanism("Setting `compression.type=none` opts out of the producer's per-batch compression. Every record-batch travels uncompressed to the broker, is replicated uncompressed across the ISR, and is stored uncompressed in the log segments.")
            .impact("Typical JSON/Avro payloads grow 3-5× compared to `zstd` or `lz4`. At scale this dominates the broker's storage cost, cross-AZ network bill, and consumer-side fetch latency.")
            .whyMatters("The CPU cost of compression lives in the producer's sender thread, not on the caller's hot path. The only sane reason to choose `none` is that your payload is already compressed (parquet, protobuf-binary with a snappy outer layer, etc.) — and you should add a comment saying so.")
            .build());

    public static final RuleId PRODUCER_LINGER_ZERO_NO_BATCH = register(builder("PRODUCER_LINGER_ZERO_NO_BATCH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_LINGER_ZERO_NO_BATCH.md")
            .message("Producer sets linger.ms=0 — sender thread sends every record immediately, no batching.")
            .tagline("linger.ms=0 with default batch.size means every record is a network round trip.")
            .mechanism("`linger.ms` is the maximum time the producer's accumulator waits before sending a partial batch. With `linger.ms=0` (the default!), partial batches are sent as soon as the sender thread sees them.")
            .impact("On a steady stream of small records this means one ProduceRequest per record — same throughput collapse as `send().get()`. Bandwidth efficiency drops by 10-100× because the per-batch overhead amortises across one record instead of hundreds.")
            .whyMatters("The fix is `linger.ms=5` (or 10, or 20 — pick the latency budget you can spare). Five milliseconds of added latency typically doubles throughput; 20 ms quadruples it for high-fanout workloads.")
            .build());

    public static final RuleId CONSUMER_AUTO_OFFSET_RESET_LATEST = register(builder("CONSUMER_AUTO_OFFSET_RESET_LATEST")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_AUTO_OFFSET_RESET_LATEST.md")
            .message("Consumer sets auto.offset.reset=latest — a fresh group will skip everything produced before it started.")
            .tagline("auto.offset.reset=latest is \"skip the backlog you didn't know you had.\"")
            .mechanism("`auto.offset.reset` only fires when the consumer's group has NO committed offset for a partition (fresh group, or new partition, or after retention pruning). `latest` jumps to the end; `earliest` reads from the beginning.")
            .impact("A new deployment of a fresh consumer group silently skips every record produced before deployment. Looks like data loss but is actually intentional in the config. Discovered by 'why didn't we get message X?' tickets.")
            .whyMatters("`earliest` is almost always the right default for analytics, replay, and any system that processes historical data. `latest` is right for monitoring/health/heartbeat consumers where stale records are useless. Pick deliberately, document the reason, and consider `none` (fail loud) if neither is acceptable.")
            .build());

    public static final RuleId CONSUMER_MAX_POLL_RECORDS_TOO_HIGH = register(builder("CONSUMER_MAX_POLL_RECORDS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_MAX_POLL_RECORDS_TOO_HIGH.md")
            .message("max.poll.records > 1000 — a batch you can't process inside max.poll.interval.ms turns into a rebalance storm.")
            .tagline("A batch you can't process in max.poll.interval.ms is a batch you'll process twice.")
            .mechanism("`max.poll.records` (default 500) is the cap on records returned by a single `poll()`. `max.poll.interval.ms` (default 300000) is the maximum time between polls before the broker assumes the consumer is dead and removes it from the group. If per-record processing time × batch size > poll-interval, the consumer gets evicted mid-batch.")
            .impact("Eviction mid-batch means the entire poll batch is re-delivered to the new owner, which takes even longer (cold cache, larger backlog) and times out itself. Rebalance loop. Symptoms: `last-rebalance-seconds-ago` constantly resetting; `CommitFailedException: ... group has already rebalanced` in logs; `records-lag-max` climbing.")
            .whyMatters("Pick `max.poll.records` so that `max.poll.records × per-record-cost < max.poll.interval.ms` with margin. If records really are bulk-processable, raise both together (e.g. records=5000, interval=900000). Setting one without thinking about the other is the most common shape of this bug.")
            .build());

    public static final RuleId PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL = register(builder("PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL.md")
            .message("delivery.timeout.ms < 30000 — too short for the producer's retry budget under any realistic broker outage.")
            .tagline("`delivery.timeout.ms` is the producer's whole retry budget. Set it small and you turn 'broker hiccups' into 'callback errors'.")
            .mechanism("`delivery.timeout.ms` (default 120000) is the wall-clock cap on `send()`-to-final-state for one record, covering `linger.ms` + `request.timeout.ms` + every retry. Setting it small forces the producer to give up early — before a typical leader-election or controller-failover can resolve.")
            .impact("Records that would have been delivered by retry get failed callbacks instead. Application either drops the record or retries at the application layer with worse delivery semantics than the client would have provided. Discovered as 'TimeoutException: ... has passed since batch creation' bursts during routine cluster operations.")
            .whyMatters("`delivery.timeout.ms` should be ≥ `request.timeout.ms` and large enough to absorb the longest expected broker availability dip (typically 60-120 s for a leader-election). The default is intentionally generous. Lower it only when you have a deliberate latency SLO and an upstream retry path that handles `TimeoutException`.")
            .build());

    public static final RuleId PRODUCER_IDEMPOTENCE_DISABLED = register(builder("PRODUCER_IDEMPOTENCE_DISABLED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_IDEMPOTENCE_DISABLED.md")
            .message("enable.idempotence=false — undoing five years of duplicate-and-reorder fixes. Default is true since Kafka 3.0.")
            .tagline("Turning idempotence off in 2026 is undoing five years of work the client did for you.")
            .mechanism("The idempotent producer attaches a producer ID (PID) and per-partition monotonic sequence numbers to each batch. The broker maintains a five-record sequence window per (PID, partition) and rejects duplicates or out-of-order arrivals. KIP-679 made `enable.idempotence=true` the default in Kafka 3.0 (fully fixed in 3.0.1 / 3.1.1 / 3.2.0). Setting `enable.idempotence=false` explicitly opts out of that machinery.")
            .impact("After any transient network blip, retries produce duplicates on the broker — observable as same-key, same-payload, sequential offsets downstream. With `max.in.flight.requests.per.connection > 1`, retries also reorder writes per partition: msg2 lands before msg1. Compaction does not save you for non-compacted topics. `record-retry-rate` going non-zero during an incident is the trigger window for the data damage.")
            .whyMatters("Rely on the default (`true`) and let the client do this for you. Legitimate exceptions: Kafka Connect (defaults to false for broker-version breadth, KAFKA-13759), and pre-2.8 brokers without the IDEMPOTENT_WRITE ACL — both rare in 2026. 🤝 Removing `false` is not a one-line fix if `acks`, `retries`, or `max.in.flight.requests.per.connection` are also explicitly set: KIP-679's silent-disable rules mean the producer's actual runtime config depends on those knobs too.")
            .build());

    public static final RuleId PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE = register(builder("PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE.md")
            .message("Producer sets transactional.id but enable.idempotence is false — transactional producers REQUIRE idempotence.")
            .tagline("transactional.id without enable.idempotence=true is a configuration contradiction the broker will reject.")
            .mechanism("A transactional producer relies on idempotent semantics for its EOS guarantee — the producer ID + sequence number that the broker dedupes on is part of the idempotence machinery.")
            .impact("Application fails at producer initialization with a `ConfigException` (Kafka 3.0+) or silently downgrades semantics (older versions). Either way, EOS is not what the code claims.")
            .whyMatters("Idempotence is the floor; transactions are built on top. Always set `enable.idempotence=true` explicitly when you set `transactional.id`, even though it's the default on 3.0+. Future-you reading the config will thank present-you.")
            .build());

    public static final RuleId PRODUCER_MAX_IN_FLIGHT_TOO_HIGH = register(builder("PRODUCER_MAX_IN_FLIGHT_TOO_HIGH")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_MAX_IN_FLIGHT_TOO_HIGH.md")
            .message("max.in.flight.requests.per.connection > 5 with idempotence enabled — broker rejects this combination.")
            .tagline("Idempotent producers cap in-flight at 5. Higher values are rejected at start-up.")
            .mechanism("The idempotent producer dedupes by (PID, sequence). To dedupe correctly the broker must be able to reorder up to N in-flight batches per partition; that bound is hard-coded at 5.")
            .impact("Producer construction throws `ConfigException` and the app never starts. Easy to miss in dev (default is fine) and explode in production where someone has tuned the config.")
            .whyMatters("If you need higher in-flight for throughput, the answer is `batch.size` and `linger.ms`, not `max.in.flight`. The default of 5 is a contract, not a tunable.")
            .build());

    public static final RuleId CONSUMER_ASSIGN_AND_SUBSCRIBE = register(builder("CONSUMER_ASSIGN_AND_SUBSCRIBE")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_ASSIGN_AND_SUBSCRIBE.md")
            .message("KafkaConsumer.assign() and .subscribe() called on the same consumer — these are mutually exclusive modes.")
            .tagline("A consumer is either in group mode (subscribe) or manual mode (assign). Mixing them throws at runtime.")
            .mechanism("subscribe() registers the consumer with the group coordinator (dynamic assignment, rebalance). assign() bypasses the coordinator and pins specific partitions (no group, no rebalance). The Java client throws `IllegalStateException` if you call the other after one is set.")
            .impact("Runtime crash on the second call, often after the consumer has already polled for a while — discovered in production, not in tests.")
            .whyMatters("Pick one model up front. Manual assignment is for stateful single-consumer cases (CDC, replays, debug tools). Everything else is `subscribe()`.")
            .build());

    public static final RuleId CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE = register(builder("CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE.md")
            .message("Consumer has allow.auto.create.topics=true — typos create real topics with default config.")
            .tagline("Auto-create-on-subscribe turns a typo into a permanent topic with one-partition, default-replication config.")
            .mechanism("With `allow.auto.create.topics=true` (the default!), subscribing to a nonexistent topic causes the broker to create it on the fly with cluster-default settings.")
            .impact("Production gets a 'shadow' topic with replication-factor=1 and partition-count=1 that you never intended to operate. Real traffic might land there and be invisible to the alerting on the canonical topic.")
            .whyMatters("Explicit topic provisioning (Terraform, AdminClient, ops platform) is the only shape that survives audits. Set `allow.auto.create.topics=false` on every consumer.")
            .build());

    public static final RuleId KAFKA_CLIENT_TYPO_GROUP_ID = register(builder("KAFKA_CLIENT_TYPO_GROUP_ID")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_CLIENT_TYPO_GROUP_ID.md")
            .message("Consumer config uses 'groupId' / 'group_id' instead of 'group.id' — Kafka silently ignores unknown keys.")
            .tagline("Kafka clients silently ignore unknown config keys. A typo on 'group.id' leaves the consumer in random-group-on-each-restart mode.")
            .mechanism("The Kafka client validates *known* keys but logs at WARN for unrecognized ones. With no `group.id` set, the consumer generates a random UUID-style group on each construction.")
            .impact("Each restart joins a new group with no committed offsets — the consumer re-reads from `auto.offset.reset` (latest by default) and skips everything in between. Looks like 'lost messages'.")
            .whyMatters("The dot-separated form is the contract. `group.id`, `bootstrap.servers`, `enable.auto.commit` — not camelCase, not snake_case. The Spring / Quarkus property bindings translate; raw config maps do not.")
            .build());

    // ────────────────────────────────────────────────────────────────────────
    // versions/ — pom-dependency rules (no bytecode needed)
    // ────────────────────────────────────────────────────────────────────────

    public static final RuleId KAFKA_CLIENTS_EOL = register(builder("KAFKA_CLIENTS_EOL")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("versions")
            .docPath("versions/KAFKA_CLIENTS_EOL.md")
            .message("kafka-clients dependency is end-of-life — upgrade is required for support and security.")
            .tagline("kafka-clients < 3.5 is past EOL. No more security fixes; broker compatibility erodes.")
            .mechanism("Apache Kafka follows a release-window policy: the latest two minor versions get patches. Older minors stop receiving fixes — including CVE backports.")
            .impact("Known CVEs (JNDI/LDAP, OAuthBearer, ConfigProvider) accumulate without patches. Broker upgrades on the cluster side eventually break wire compatibility.")
            .whyMatters("Bumping kafka-clients is usually a non-event — the wire protocol is stable across minors. The cost of staying current is a quarterly bump; the cost of falling behind is an emergency upgrade under CVE pressure.")
            .build());

    public static final RuleId KAFKA_CLIENTS_CVE_JNDI_LDAP = register(builder("KAFKA_CLIENTS_CVE_JNDI_LDAP")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("versions")
            .docPath("versions/KAFKA_CLIENTS_CVE_JNDI_LDAP.md")
            .message("kafka-clients version is vulnerable to a known JNDI/LDAP RCE — upgrade immediately.")
            .tagline("CVE-class: a malicious bootstrap URL or SASL handshake can trigger remote class loading.")
            .mechanism("Older kafka-clients SASL handlers / config providers performed unsanitized JNDI lookups (Log4Shell-shape). A crafted broker URL or auth response triggers a JNDI call out to an attacker-controlled LDAP server, which serves a malicious class.")
            .impact("Remote code execution in the client JVM with the privileges of the running app. Network egress to LDAP ports (389/636) is the only requirement.")
            .whyMatters("Upgrade kafka-clients to a patched version. This is not a defense-in-depth fix — it's a 'don't run this version in production' fix.")
            .build());

    public static final RuleId KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER = register(builder("KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("versions")
            .docPath("versions/KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER.md")
            .message("kafka-clients version has a known SASL/OAUTHBEARER token-validation flaw — upgrade.")
            .tagline("Older OAUTHBEARER login modules accept tokens without verifying scope/audience.")
            .mechanism("The SASL OAUTHBEARER login module historically did not validate the token's `aud` / `scope` claims by default. A token issued for a different audience could be accepted.")
            .impact("Auth bypass between services that share an IdP — a token for service A authenticates as service A's Kafka identity.")
            .whyMatters("Upgrade kafka-clients AND ensure the OAuthBearerValidatorCallbackHandler is configured with explicit audience checks. The fix is partly version, partly config — the lint catches the version half.")
            .build());

    public static final RuleId KAFKA_CLIENTS_CVE_CONFIG_PROVIDER = register(builder("KAFKA_CLIENTS_CVE_CONFIG_PROVIDER")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("versions")
            .docPath("versions/KAFKA_CLIENTS_CVE_CONFIG_PROVIDER.md")
            .message("kafka-clients version is vulnerable to CVE-2024-31141 — ConfigProvider silently reads arbitrary files from any user-supplied placeholder.")
            .tagline("CVE-2024-31141 — ConfigProvider was happy to read whatever file you named.")
            .mechanism("In affected versions the client treated any `${...}` in a config value as a directive to resolve through the registered `ConfigProvider` set. `FileConfigProvider` and `DirectoryConfigProvider` are shipped by default and happily read any path the JVM user can — so a connector config of `${file:/etc/passwd:root}` returned the root entry interpolated into the config value.")
            .impact("Privilege boundary crossed: a caller authorized to *create connector configs* gains read access to the JVM process's filesystem and environment. Auditable as 'why did our config include /etc/passwd?' — but typically discovered post-incident.")
            .whyMatters("Fixed in 3.6.3 and 3.7.1. The vulnerable range covers `2.3.0`–`3.5.x`, `3.6.0`–`3.6.2`, and `3.7.0`. Upgrade now — the fix removes implicit ConfigProvider resolution, so applications relying on the implicit form will need to opt in explicitly (`config.providers=...`), which is the correct posture anyway.")
            .build());

    public static final RuleId KAFKA_CLIENTS_CVE_BUFFER_POOL = register(builder("KAFKA_CLIENTS_CVE_BUFFER_POOL")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("versions")
            .docPath("versions/KAFKA_CLIENTS_CVE_BUFFER_POOL.md")
            .message("kafka-clients version is vulnerable to CVE-2026-35554 — BufferPool reuse can send records to the wrong topic with no callback error.")
            .tagline("CVE-2026-35554 — your producer ships messages to the wrong topic and never tells you.")
            .mechanism("A race in `KafkaProducer`'s `BufferPool`: when a batch's `delivery.timeout.ms` expires while the network request carrying it is still pending, the buffer is returned to the pool. If a new batch reuses that buffer before the original network response arrives, the second batch's contents can be written into the in-flight request — silently delivering its records to whichever (topic, partition) the first request was bound for.")
            .impact("Records intended for topic A appearing on topic B at low but non-zero rate, exactly during periods of broker slowness. No callback errors, no metric anomalies beyond `record-expiration-rate` ticking up. Discovered post-hoc as 'why are these records in this topic?' downstream.")
            .whyMatters("Fixed in `3.9.2`, `4.0.2`, `4.1.2`, and `4.2.x`. Older lines are not getting backports per the Confluent advisory. If the project is on `< 3.9.2`, in `[4.0.0, 4.0.2)`, or in `[4.1.0, 4.1.2)`, upgrade. The exposure scales with `delivery.timeout.ms` expirations under load, so high-throughput producers are the highest-risk profile.")
            .build());

    public static final RuleId KAFKA_CLIENTS_CVE_SCRAM_REPLAY = register(builder("KAFKA_CLIENTS_CVE_SCRAM_REPLAY")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("versions")
            .docPath("versions/KAFKA_CLIENTS_CVE_SCRAM_REPLAY.md")
            .message("kafka-clients version mishandles SCRAM nonces (CVE-2024-56128); use SASL_SSL — and upgrade.")
            .tagline("CVE-2024-56128 — SCRAM without TLS is a replay attack waiting to happen.")
            .mechanism("SCRAM (RFC 5802) relies on per-exchange nonces to prevent replay. In the affected `kafka-clients` range, client-side nonce handling allows two distinct authentication attempts in close succession to reuse derived material — letting a man-in-the-middle replay a captured exchange and impersonate the client. The CVE only matters on the wire, i.e. when the listener is `SASL_PLAINTEXT` rather than `SASL_SSL`.")
            .impact("Authentication impersonation: an attacker who captures one SCRAM exchange can establish an authenticated session as that user. Once authenticated, the attacker inherits the impersonated user's ACLs — service accounts often have broad rights, so this fans out cluster-wide.")
            .whyMatters("Fixed in 3.9.1+ and 4.x. The *real* mitigation is `SASL_SSL` — TLS prevents the replay regardless of the client version. If the project resolves `kafka-clients < 3.9.1`, upgrade as a defensive layer, but the priority is the transport: a deployment on `SASL_PLAINTEXT` with strong SCRAM credentials is the worst combination.")
            .build());

    public static final RuleId JAVA_VERSION_TOO_LOW = register(builder("JAVA_VERSION_TOO_LOW")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("versions")
            .docPath("versions/JAVA_VERSION_TOO_LOW.md")
            .message("Project targets Java < 11 — modern kafka-clients (3.x+) require Java 11 minimum.")
            .tagline("kafka-clients 3.x dropped Java 8 support. Targeting Java 8 will fail at class-load time.")
            .mechanism("kafka-clients 3.0+ is compiled to Java 11 bytecode. Loading those classes in a Java 8 JVM raises `UnsupportedClassVersionError` at first reference.")
            .impact("App fails to start. Discovered at deployment, not at compile (the consuming app may still target Java 8 in its own bytecode).")
            .whyMatters("Java 11 is the new floor for the JVM Kafka ecosystem. Many ancillary libraries (Avro, Spring Boot 3, Quarkus 3) also require 17+. Plan a single bump rather than chasing dependencies one at a time.")
            .build());

    public static final RuleId QUARKUS_KAFKA_EXTENSION_RENAMED = register(builder("QUARKUS_KAFKA_EXTENSION_RENAMED")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("versions")
            .docPath("versions/QUARKUS_KAFKA_EXTENSION_RENAMED.md")
            .message("Quarkus project uses deprecated 'quarkus-kafka' or 'quarkus-smallrye-reactive-messaging-kafka' artifact name — use 'quarkus-messaging-kafka'.")
            .tagline("Quarkus 3 renamed the Kafka extension. Old artifact names still resolve but silently miss config bindings.")
            .mechanism("Quarkus 3.x consolidated the messaging extensions under `io.quarkus:quarkus-messaging-*`. The old `quarkus-smallrye-reactive-messaging-kafka` artifact is a redirect for back-compat but does not pick up the new config paths.")
            .impact("Configs like `mp.messaging.outgoing.*` may not be processed; the app starts but no channels are wired. The failure is silent at start-up and shows as 'my channels don't fire' in tests.")
            .whyMatters("On a Quarkus 3 upgrade, the extension rename is a five-character pom edit. Easy to miss because the build still resolves; the regression shows only at runtime.")
            .build());

    public static final RuleId SPRING_KAFKA_BOOT_MISMATCH = register(builder("SPRING_KAFKA_BOOT_MISMATCH")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("versions")
            .docPath("versions/SPRING_KAFKA_BOOT_MISMATCH.md")
            .message("spring-kafka version doesn't match the Spring Boot BOM — auto-configuration may not wire correctly.")
            .tagline("spring-kafka and Spring Boot are tightly coupled. Mismatched versions produce silent auto-config gaps.")
            .mechanism("Spring Boot's BOM pins a specific spring-kafka version that its auto-configuration is tested against. Overriding spring-kafka with a different minor disconnects the `KafkaAutoConfiguration` from the version actually on the classpath.")
            .impact("`KafkaTemplate`, `ConcurrentKafkaListenerContainerFactory` and related beans may not be created, or are created with the wrong defaults. App starts but listeners silently don't fire.")
            .whyMatters("Either upgrade Spring Boot to the version whose BOM matches, or accept the BOM's pin. Hand-rolled version overrides are a maintenance trap.")
            .build());

    // ────────────────────────────────────────────────────────────────────────
    // kafka-streams/ — Streams-specific config rules
    // ────────────────────────────────────────────────────────────────────────

    public static final RuleId STREAMS_REPLICATION_FACTOR_ONE = register(builder("STREAMS_REPLICATION_FACTOR_ONE")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_REPLICATION_FACTOR_ONE.md")
            .message("Kafka Streams configured with replication.factor=1 — internal topics (changelog, repartition) become single-points-of-failure.")
            .tagline("Streams internal topics with replication-factor=1 mean one broker reboot loses your state stores.")
            .mechanism("Streams creates changelog topics (for materialized state) and repartition topics (for keyed re-grouping) under the hood. `replication.factor` applies to these internal topics, not to your input topic.")
            .impact("A single broker outage during processing drops a partition of the changelog. The state store can't recover; the next rebalance re-bootstraps from input — slow at best, data-loss at worst.")
            .whyMatters("Set `replication.factor=3` in production. The default of 1 is a dev-only convenience; if the topic is gone, the state machinery is gone with it. Streams gives no warning on its own.")
            .build());

    public static final RuleId STREAMS_STATE_DIR_TMP = register(builder("STREAMS_STATE_DIR_TMP")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_STATE_DIR_TMP.md")
            .message("Streams state.dir points at /tmp or a container ephemeral path — state is lost on restart.")
            .tagline("/tmp is wiped on reboot. RocksDB state stores live there at your peril.")
            .mechanism("Streams persists RocksDB state under `state.dir`. The directory survives only as long as its filesystem — `/tmp` is wiped by systemd-tmpfiles, containers wipe the writable layer on restart.")
            .impact("Every restart triggers a full state rebuild from the changelog topic. For a non-trivial store that's minutes of cold-start; until then, joins / aggregations return empty results.")
            .whyMatters("Mount a persistent volume (`/var/lib/<app>/streams`, a PVC on K8s). The few-megabytes-per-second of RocksDB writes are not the bottleneck; the cold-start cost of losing them is.")
            .build());

    public static final RuleId STREAMS_EOS_V1_DEPRECATED = register(builder("STREAMS_EOS_V1_DEPRECATED")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_EOS_V1_DEPRECATED.md")
            .message("Streams processing.guarantee uses deprecated 'exactly_once' / 'exactly_once_beta' — use 'exactly_once_v2'.")
            .tagline("EOS-v1 was deprecated by KIP-732 (Kafka 3.0) and removed in 4.0. Use exactly_once_v2.")
            .mechanism("Original EOS-v1 used one producer per task (per-partition transactional state). KIP-447 introduced a thread-producer that handles multiple tasks per transaction. KIP-732 deprecated the v1 names; 4.0 removes them.")
            .impact("On Kafka 4.x: the app refuses to start (`ConfigException: 'exactly_once' is not a valid value`). On 3.x: deprecation warning at start-up plus broker-side `(tasks × partitions)` transactional state growth instead of just `tasks`.")
            .whyMatters("One-line change: `processing.guarantee=exactly_once_v2`. Required broker minimum is 2.5+, which is almost certainly already true. Migrate before the next Kafka upgrade window.")
            .build());

    public static final RuleId STREAMS_CLEANUP_IN_PROD = register(builder("STREAMS_CLEANUP_IN_PROD")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_CLEANUP_IN_PROD.md")
            .message("KafkaStreams.cleanUp() called in non-test code — wipes the local state store, forcing full rebuild from changelog.")
            .tagline("cleanUp() deletes the local state directory. Run it in tests; never in production code paths.")
            .mechanism("cleanUp() removes everything under `state.dir` for this application.id. On the next `start()`, Streams must replay the entire changelog topic to rebuild RocksDB.")
            .impact("Cold-start time grows from seconds to minutes-or-hours, proportional to changelog size. During the rebuild, the topology is paused — no records consumed, no records produced.")
            .whyMatters("cleanUp() exists for the test pattern of 'fresh state for each test run'. In production, even a small bug that triggers it on a hot path is a multi-hour outage.")
            .build());

    public static final RuleId STREAMS_COMMIT_INTERVAL_TOO_LOW = register(builder("STREAMS_COMMIT_INTERVAL_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_COMMIT_INTERVAL_TOO_LOW.md")
            .message("Streams commit.interval.ms < 100 — pathological commit rate. Brokers and changelog topics will feel it.")
            .tagline("Committing every millisecond means the broker commits a million times a millisecond. 🤝")
            .mechanism("Streams uses `commit.interval.ms` (default 30000 under at-least-once, 100 under EOS-v2) to time-bound how often the runtime drains the producer, advances source positions, and flushes state stores. Each commit triggers a state-store flush, a changelog produce, and an offset commit — three brokers-side writes. Setting it below 100 ms compounds this into a continuous storm.")
            .impact("Broker CPU saturates on offset/changelog writes. Producer batching collapses (each commit forces a flush). State store I/O climbs to the point that the topology stops making forward progress on input. Discovered as 'topology is stuck but consumer-lag isn't growing fast enough' — the runtime is spending all its time committing.")
            .whyMatters("Default 30 s under AT_LEAST_ONCE is intentional — record-level delivery is already covered by the producer's idempotence. Under EOS-v2 the default 100 ms is the documented sweet spot; lower than that is almost always a misunderstanding (people think they're reducing latency; they're actually increasing it). 🤝 Coordinate this knob with the broker team — they'll see the load immediately.")
            .build());

    public static final RuleId STREAMS_CACHE_DISABLED = register(builder("STREAMS_CACHE_DISABLED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_CACHE_DISABLED.md")
            .message("Streams cache disabled (cache.max.bytes.buffering=0 or statestore.cache.max.bytes=0) — every state-store update becomes a changelog produce.")
            .tagline("Cache=0 makes brokers cry. Update rates explode without it.")
            .mechanism("Streams' record cache buffers state-store updates between commit intervals and emits *only the latest value per key* downstream — this collapses a hot key receiving 1000 updates/second into one downstream record per commit. Setting `cache.max.bytes.buffering=0` (or the new `statestore.cache.max.bytes=0`, since Kafka 3.4) disables the cache and forwards every update.")
            .impact("Changelog topic write rate explodes (every state mutation goes to the broker). Downstream operators receive every intermediate value instead of the converged one. Consumer apps reading from the output topic see a 100× message-rate increase on the same logical workload. Symptoms: broker IO climbing on the changelog topic; downstream `records-per-second` mismatching upstream `records-consumed-per-second`.")
            .whyMatters("Keep the cache on. Disabling it is sometimes deliberate (you want every intermediate state, e.g. for audit), but that case is rare and should be paired with downstream sizing for the burst. The Streams 3.4+ `statestore.cache.max.bytes` key replaces `cache.max.bytes.buffering` — make sure you don't accidentally set both to zero.")
            .build());

    public static final RuleId STREAMS_THROUGH_DEPRECATED = register(builder("STREAMS_THROUGH_DEPRECATED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_THROUGH_DEPRECATED.md")
            .message("KStream.through() is deprecated — use repartition() (or split it into to() + stream()) for clarity.")
            .tagline("through() is deprecated since Kafka 2.6. Use repartition() or an explicit to/stream pair.")
            .mechanism("`through(topic)` was a write-and-read primitive that quietly created an intermediate topic. KIP-221 split it into the explicit `repartition()` (with auto-managed internal topic) and the to/stream pair (with user-managed topic).")
            .impact("The deprecated method still works but will be removed. More importantly, the implicit intermediate topic is hard to discover during operations — it's not in your topic inventory.")
            .whyMatters("Migrate to `repartition()` if you want Streams to manage the internal topic; or to `.to(\"x\")` then `streamsBuilder.stream(\"x\")` if you want the topic in your inventory. The code becomes self-describing.")
            .build());

    // ────────────────────────────────────────────────────────────────────────
    // spring-kafka/ — Spring auto-magic detection
    // ────────────────────────────────────────────────────────────────────────

    public static final RuleId SPRING_LISTENER_ASYNC_ANNOTATION = register(builder("SPRING_LISTENER_ASYNC_ANNOTATION")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_LISTENER_ASYNC_ANNOTATION.md")
            .message("@KafkaListener method also annotated @Async — container thread returns immediately, offset committed before processing.")
            .tagline("@Async on a @KafkaListener method breaks the at-least-once contract. The container commits before processing completes.")
            .mechanism("The Kafka listener container invokes the listener method and commits the offset (manual or auto) based on whether the method returned successfully. `@Async` makes the method return immediately by dispatching to a TaskExecutor — Kafka thinks processing succeeded the moment the dispatch happened.")
            .impact("Offsets commit before the actual work runs. If the async task fails (exception, container shutdown), the record is silently dropped — no DLT, no retry, no log.")
            .whyMatters("The Spring 'one of these makes things async' magic is bound to bite somewhere. Listener methods must run on the container thread. If you need async work downstream of the listener, dispatch *after* doing the durable acknowledgement step yourself.")
            .build());

    public static final RuleId SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE = register(builder("SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.MEDIUM).category("spring-kafka")
            .docPath("spring-kafka/SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE.md")
            .message("@RetryableTopic in use but no KafkaTemplate bean is exposed for the retry topics — retry/DLT publication will silently fail.")
            .tagline("@RetryableTopic uses an injected KafkaTemplate to publish to retry/DLT topics. No template → no retries.")
            .mechanism("Spring's `@RetryableTopic` machinery routes failures to versioned retry topics (`-retry-0`, `-retry-1`, …) and finally to a DLT. The mechanism publishes via a `KafkaTemplate<?, ?>` from the application context. Without one, the publisher path can't initialize.")
            .impact("In some configurations the listener fails to start; in others, retries silently fall through to direct DLT publish or are dropped. Either way, the retry topology you wrote is not the topology that runs.")
            .whyMatters("If you're using `@RetryableTopic`, expose an explicit `@Bean public KafkaTemplate<String, Object> kafkaTemplate(...)`. Spring auto-config provides one in some setups but not all — make it explicit so the wiring is obvious.")
            .build());

    public static final RuleId SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES = register(builder("SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES.md")
            .message("ErrorHandlingDeserializer configured without spring.deserializer.value.delegate.class / key.delegate.class — every record fails to deserialize.")
            .tagline("ErrorHandlingDeserializer is a wrapper. Without a delegate.class, it has nothing to delegate TO.")
            .mechanism("`ErrorHandlingDeserializer` wraps a real deserializer and converts deserialization exceptions into a header the listener can inspect. It needs `spring.deserializer.value.delegate.class` (FQCN of the underlying deserializer) to know what to wrap.")
            .impact("Without the delegate property, every record raises `ConfigException` at construction time, or fails to deserialize at runtime. The 'no records consumed' shape is hard to root-cause.")
            .whyMatters("The delegate property is the second half of the configuration; missing it is a copy-paste accident from the docs. The error-handling deserializer pattern is foundational for Spring Kafka error-recovery — get it right.")
            .build());

    public static final RuleId SPRING_BOOT_PRODUCER_ACKS_NOT_ALL = register(builder("SPRING_BOOT_PRODUCER_ACKS_NOT_ALL")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_PRODUCER_ACKS_NOT_ALL.md")
            .message("spring.kafka.producer.acks set to 0 or 1 — durability is partial; prefer 'all'.")
            .tagline("`spring.kafka.producer.acks=1` ships durability away one record at a time.")
            .mechanism("Spring Boot's `spring.kafka.producer.acks` key feeds directly into the underlying `acks` producer config. With `1`, only the leader has the record at the time of ACK; with `0`, the producer doesn't wait at all.")
            .impact("Same failure shape as raw kafka-clients `acks=1`/`acks=0`: lost records on leader failover (acks=1) or on any broker hiccup (acks=0). Discovered as 'missing records' tickets.")
            .whyMatters("Production deployments should set `acks=all`. The framework default is `1` for Boot < 3.0 (Boot 3+ defers to the kafka-clients default which is `all` since Kafka 3.0). Set it explicitly so the property reads as a deliberate decision rather than an inherited default.")
            .build());

    public static final RuleId SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST = register(builder("SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST.md")
            .message("spring.kafka.bootstrap-servers points at localhost — bound to be wrong for any non-local deployment.")
            .tagline("`bootstrap-servers: localhost:9092` shipped to prod is a Friday-evening pager.")
            .mechanism("`spring.kafka.bootstrap-servers` is the comma-separated list of broker addresses the Kafka client uses to bootstrap metadata. A `localhost` value means the application will attempt to reach a broker on the same machine as itself.")
            .impact("On a deployed app this fails closed: `KafkaProducer` / consumer constructors block on metadata fetch and then time out, often only after the container starts taking traffic. Discovered as cold-start errors at deploy time.")
            .whyMatters("Externalize via `${KAFKA_BOOTSTRAP_SERVERS}` or a profile-conditional override. A literal `localhost` belongs only in profile-suffixed properties (`application-dev.properties`) — and even then it should be obvious that this is the local-dev value, not the deployed one.")
            .build());

    public static final RuleId SPRING_BOOT_AUTO_OFFSET_RESET_LATEST = register(builder("SPRING_BOOT_AUTO_OFFSET_RESET_LATEST")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_AUTO_OFFSET_RESET_LATEST.md")
            .message("spring.kafka.consumer.auto-offset-reset=latest — fresh consumer groups skip the existing backlog. Prefer 'earliest' for pipeline consumers.")
            .tagline("`auto-offset-reset: latest` means 'lose every record produced before the first deploy'.")
            .mechanism("`auto.offset.reset` controls what happens when a consumer group has no committed offset for an assigned partition. `latest` (the kafka-clients default) seeks to the high-water-mark; `earliest` seeks to the log start. Spring Boot's `spring.kafka.consumer.auto-offset-reset` writes straight into that config.")
            .impact("First deploy of a new group, an operator-driven offset reset, a topic recreated, or a group expired past `offsets.retention.minutes` (7 days) all hit the 'no committed offset' path. With `latest` the consumer reports caught-up while everything still on disk is silently skipped. Downstream sees gaps centred on the deploy timestamp.")
            .whyMatters("For event-sourcing, audit, or replay topics — almost always pipeline consumers — `earliest` is what you want. `latest` is correct for ephemeral metric streams or CDC replicas where catch-up is meaningless. The linter can't know your intent; make the choice explicit and align with the topic's purpose.")
            .build());

    public static final RuleId SPRING_BOOT_ENABLE_AUTO_COMMIT_TRUE = register(builder("SPRING_BOOT_ENABLE_AUTO_COMMIT_TRUE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_ENABLE_AUTO_COMMIT_VS_MANUAL_ACK.md")
            .message("spring.kafka.consumer.enable-auto-commit=true — same failure shape as kafka-clients auto-commit. Use Spring's manual-ack mode instead.")
            .tagline("Spring's manual-ack story only works when auto-commit is off. Turning it on quietly disables the listener's commit machinery.")
            .mechanism("Spring's listener container has three ack modes: RECORD, BATCH, MANUAL (and friends). All of them require `enable.auto.commit=false` so that the container — not the Kafka client's timer — controls offset commits.")
            .impact("With auto-commit on, the Kafka client commits every `auto.commit.interval.ms` (5s default) regardless of whether the listener has acknowledged. Same failure shapes as the raw kafka-clients rule: skip on crash, double-process on retry.")
            .whyMatters("Set `spring.kafka.consumer.enable-auto-commit=false` and rely on the listener container's ack-mode. The default in Spring Boot 3+ is already false, but legacy projects and explicit overrides do still set it to true — easy to miss in code review.")
            .build());

    // ────────────────────────────────────────────────────────────────────────
    // quarkus-kafka/ — Quarkus / SmallRye Reactive Messaging
    // ────────────────────────────────────────────────────────────────────────

    public static final RuleId SPRING_BOOT_PRODUCER_COMPRESSION_NONE = register(builder("SPRING_BOOT_PRODUCER_COMPRESSION_NONE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_PRODUCER_COMPRESSION_NONE.md")
            .message("spring.kafka.producer.compression-type=none — explicitly disables compression on the producer.")
            .tagline("Setting compression-type=none in Spring Boot config explicitly opts out of one of the cheapest broker-cost wins.")
            .mechanism("`spring.kafka.producer.compression-type` flows through KafkaProperties → DefaultKafkaProducerFactory → the underlying KafkaProducer's `compression.type`. Setting it to `none` produces wire records with no compression even though every modern client and broker supports zstd/lz4 cheaply.")
            .impact("Bytes-on-wire and bytes-on-disk for this app's traffic stay 3-5× larger than they need to be. Across a fleet of producers this dominates the broker's storage and cross-AZ network bill.")
            .whyMatters("`zstd` is the modern default and adds essentially no CPU to a busy app's hot path. `lz4` if you're CPU-bound. `none` is almost always a copy from a sample that nobody updated.")
            .build());

    public static final RuleId SPRING_BOOT_PRODUCER_RETRIES_ZERO = register(builder("SPRING_BOOT_PRODUCER_RETRIES_ZERO")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_PRODUCER_RETRIES_ZERO.md")
            .message("spring.kafka.producer.retries=0 — turns transient broker errors into permanent send failures.")
            .tagline("retries=0 on a Spring Boot producer makes every routine broker hiccup a record loss.")
            .mechanism("`spring.kafka.producer.retries` is forwarded to the KafkaProducer's `retries` config. The kafka-clients default is `Integer.MAX_VALUE`, bounded by `delivery.timeout.ms`. Setting to 0 disables retry entirely — the producer fails the first time the broker returns a retriable error (leader-election, brief I/O hiccup, rolling restart).")
            .impact("Any non-trivial broker maintenance window now produces a spike of record-loss in application metrics. The error surface in the app — typically a `Callback` that logs and drops — silently absorbs records that the producer-level retry chain would have handled.")
            .whyMatters("Defaults are correct. Retries cost nothing if the broker isn't asking for them, and they save records when it is. Cargo-culted `retries=0` is one of the most common Spring Boot anti-patterns — usually copied from a 'fail fast' guide that confused retries with timeouts.")
            .build());

    public static final RuleId SPRING_BOOT_LISTENER_CONCURRENCY_ZERO = register(builder("SPRING_BOOT_LISTENER_CONCURRENCY_ZERO")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_LISTENER_CONCURRENCY_ZERO.md")
            .message("spring.kafka.listener.concurrency=0 — no listener container threads will be created.")
            .tagline("Concurrency=0 produces a listener factory that creates zero consumers. The app starts without reading anything.")
            .mechanism("`spring.kafka.listener.concurrency` sets the number of consumer threads `ConcurrentMessageListenerContainer` will spawn per listener. With `0`, the container creates no threads — the listener bean is registered, the @KafkaListener annotation processed, but no consumer is ever subscribed.")
            .impact("The app starts cleanly, the broker shows no consumer-group members, lag grows on the topic, and there is no obvious error in the app logs. The bug is usually only spotted hours later when downstream metrics break or someone notices the topic.")
            .whyMatters("This is almost always a typo or env-substitution bug (e.g. `${KAFKA_CONCURRENCY:0}` with the env var unset). Default of 1 is fine; explicit 0 has no legitimate use.")
            .build());

    public static final RuleId QK_AUTO_COMMIT_ENABLED = register(builder("QK_AUTO_COMMIT_ENABLED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_AUTO_COMMIT_ENABLED.md")
            .message("mp.messaging.incoming.{channel}.enable.auto.commit=true — hands offset management to Kafka's background committer and breaks at-least-once.")
            .tagline("Let SmallRye commit. Don't hand the wheel to Kafka.")
            .mechanism("SmallRye's Kafka connector defaults `enable.auto.commit` to false and drives commits via the channel's `commit-strategy` (throttled, latest, ignore). Setting `enable.auto.commit=true` flips the strategy to `ignore` and Kafka's own thread commits the polled position every `auto.commit.interval.ms` (5 s default) — regardless of whether the reactive pipeline acked the record.")
            .impact("On crash, in-flight polled-but-not-yet-processed records are lost because their offsets were already committed by Kafka's timer. Looks healthy: `consumer_lag` marches forward on schedule. Data loss is invisible without a DLQ or nack signal.")
            .whyMatters("Use `commit-strategy=throttled` (the connector default) and let SmallRye drive offset commits on ack. If at-most-once is the design (telemetry, metrics), pair with `@Acknowledgment(Strategy.NONE)` so the choice is explicit in code, not buried in properties.")
            .build());

    public static final RuleId QK_TRACING_DISABLED = register(builder("QK_TRACING_DISABLED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_TRACING_DISABLED.md")
            .message("mp.messaging.{direction}.{channel}.tracing-enabled=false — distributed traces stop at this channel. Producer/consumer correlation is lost.")
            .tagline("tracing-enabled=false is \"I solemnly swear to debug from logs alone.\"")
            .mechanism("Quarkus + SmallRye + the OpenTelemetry/MicroProfile-Tracing integration automatically inject and propagate `traceparent` headers across Kafka producer→consumer hops. The `tracing-enabled` channel knob switches that header propagation off for the channel in question.")
            .impact("Trace propagation breaks at the disabled channel. A request that fans out producer→consumer→producer ends up as two disconnected traces, neither of which tells the operator where latency or errors came from. Discovered during incident response when 'why is this slow?' has no answer.")
            .whyMatters("Leave `tracing-enabled` on (the default). Legitimate disables exist — very-high-volume telemetry streams where the per-record cost matters — but they should be rare and documented in the same change.")
            .build());

    public static final RuleId QK_HEALTH_DISABLED = register(builder("QK_HEALTH_DISABLED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_HEALTH_DISABLED.md")
            .message("SmallRye channel health-enabled=false or health-readiness-enabled=false — k8s won't see this channel as unhealthy.")
            .tagline("Disabling channel health turns the readiness probe into a lie: pod stays Ready while Kafka is broken.")
            .mechanism("SmallRye Reactive Messaging contributes per-channel checks into Quarkus' MicroProfile Health (liveness + readiness). When `health-enabled=false` (or `health-readiness-enabled=false`) the channel is omitted from the aggregate check. The HTTP endpoint stays green even when the channel can't connect, can't deserialize, or has fallen permanently behind.")
            .impact("Kubernetes never restarts the pod, the upstream load balancer never routes around it, and the on-call gets paged hours later by a downstream symptom (queue depth growing, dependent service timing out). The fact that the channel is dead is invisible at the platform layer.")
            .whyMatters("Almost the only legitimate reason to disable channel health is when the channel is genuinely optional (e.g. an analytics tap whose unavailability shouldn't unschedule the pod). For business-critical channels, leave it enabled. If a startup race is the problem, fix that, don't silence the probe.")
            .build());

    public static final RuleId QK_GRACEFUL_SHUTDOWN_DISABLED = register(builder("QK_GRACEFUL_SHUTDOWN_DISABLED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_GRACEFUL_SHUTDOWN_DISABLED.md")
            .message("SmallRye channel graceful-shutdown=false — pod termination drops in-flight records.")
            .tagline("Without graceful shutdown the consumer cuts off mid-poll. Polled-but-unacked records are reprocessed on the next instance.")
            .mechanism("When `graceful-shutdown=true` (the default), the SmallRye consumer waits for in-flight records to be acked before unsubscribing — so the next instance starts cleanly from the committed offset. Setting it to `false` skips that wait: the consumer leaves the group immediately, and any records already returned from `poll()` but not yet acked are processed twice.")
            .impact("On rolling deploys you get a steady drip of duplicate records exactly equal in count to the in-flight window times the number of pod restarts. For idempotent consumers it's noise; for any side-effect with cost (HTTP call, DB write, downstream produce) it's a real-world bug.")
            .whyMatters("Set this to `false` only if you have an external way to recover (transactional EOS, idempotent processor with a dedupe store). Most apps don't, and the default is correct. Almost always this flag is flipped during a debugging session and never put back.")
            .build());

    public static final RuleId QK_RETRIES_ZERO = register(builder("QK_RETRIES_ZERO")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_RETRIES_ZERO.md")
            .message("SmallRye outgoing channel retries=0 — transient Kafka errors become permanent send failures.")
            .tagline("retries=0 on a SmallRye outgoing channel disables the underlying producer's retry chain. One blip = one lost record.")
            .mechanism("`retries` on a SmallRye outgoing channel is forwarded to the underlying KafkaProducer config. The default is `Integer.MAX_VALUE` (bounded by `delivery.timeout.ms`), which means routine errors — leader-election, brief network hiccup, rolling-broker-restart — get retried transparently. Setting it to 0 surfaces every such error to the application.")
            .impact("The channel's outgoing call returns a failure for every transient broker error. The downstream handler (which typically logs and drops or NACKs) sees the failure rate spike during any broker maintenance window. If the channel feeds a critical event stream, you lose events on every deploy.")
            .whyMatters("There's almost never a reason to set `retries=0` on an outgoing channel — the producer-level retry chain is one of the strongest correctness affordances in Kafka. The fix is the one-line removal of the override.")
            .build());

    public static final RuleId QK_DEVSERVICES_IN_PROD = register(builder("QK_DEVSERVICES_IN_PROD")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_DEVSERVICES_IN_PROD.md")
            .message("Quarkus DevServices for Kafka enabled in the %prod profile — production will try to start a container.")
            .tagline("DevServices spawns a Kafka container for dev. Leaving it on in %prod means production starts a container and ignores your bootstrap.servers.")
            .mechanism("Quarkus DevServices intercepts the absence of a `kafka.bootstrap.servers` setting in dev/test profiles and starts a Testcontainers-based broker. The `%prod.quarkus.kafka.devservices.enabled` knob exists to make absolutely sure this doesn't happen in production.")
            .impact("Production app boots a container, points itself at the container's broker, and runs disconnected from the real Kafka cluster. Discovered only when downstream wonders why no traffic appears.")
            .whyMatters("Set `%prod.quarkus.kafka.devservices.enabled=false` explicitly. The default is on-when-no-bootstrap-servers, and prod environments do sometimes start without their config injected. Belt and braces.")
            .build());

    public static final RuleId QK_COMMIT_STRATEGY_IGNORE = register(builder("QK_COMMIT_STRATEGY_IGNORE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_COMMIT_STRATEGY_IGNORE.md")
            .message("SmallRye incoming channel has commit-strategy=ignore — offsets are never committed for this channel.")
            .tagline("commit-strategy=ignore: you process everything, you remember nothing.")
            .mechanism("`mp.messaging.incoming.<channel>.commit-strategy` controls when the connector commits offsets. `ignore` disables commit entirely: SmallRye reads, dispatches, and forgets. On the next restart the consumer re-reads from `auto.offset.reset`.")
            .impact("After a restart the channel re-reads from the beginning (or the latest, depending on reset policy) — either way, recently-processed records are reprocessed or skipped. Looks like 'we process every message twice' or 'we missed everything overnight' depending on reset.")
            .whyMatters("The default is `throttled` and that's almost always what you want. Use `ignore` only for channels where reprocessing is free and explicit (CDC replays, debugging consumers). Document the reason next to the setting.")
            .build());

    public static final RuleId QK_FAILURE_STRATEGY_IGNORE = register(builder("QK_FAILURE_STRATEGY_IGNORE")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_FAILURE_STRATEGY_IGNORE.md")
            .message("SmallRye incoming channel has failure-strategy=ignore — processing exceptions are swallowed and offsets advance.")
            .tagline("failure-strategy=ignore is data loss with a smile.")
            .mechanism("`mp.messaging.incoming.<channel>.failure-strategy=ignore` instructs SmallRye to log a warning when the @Incoming method throws and then ACK the message anyway. The next record processes; nothing fails loud.")
            .impact("Any processing exception (deserialization failure, downstream timeout, business-logic crash) becomes a silent skip. The consumer log says 'WARN: message failed' once and moves on; metrics and downstream effects look fine until customers complain.")
            .whyMatters("Use `fail` (default) for processing errors you want to investigate, or `dead-letter-queue` if you have an ops process to drain the DLQ. `ignore` is only correct for channels where the record was never meant to be reliable — and that case is rare enough to need a comment.")
            .build());

    public static final RuleId QK_AUTO_OFFSET_RESET_LATEST = register(builder("QK_AUTO_OFFSET_RESET_LATEST")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_AUTO_OFFSET_RESET_LATEST.md")
            .message("SmallRye incoming channel sets auto.offset.reset=latest — fresh deployments skip the existing backlog.")
            .tagline("auto.offset.reset=latest on a SmallRye channel is the Quarkus version of the same backlog-skip bug.")
            .mechanism("`mp.messaging.incoming.<channel>.auto.offset.reset=latest` makes a brand-new consumer group jump to the topic's end. Backlog produced before the first deployment is silently passed over.")
            .impact("First deployment of a new service skips every record produced before it started. Easy to miss because the symptom — 'why didn't service X receive message Y from yesterday?' — surfaces only after the app has been running for a while.")
            .whyMatters("Default to `earliest` for most analytical / pipeline consumers. Choose `latest` deliberately for heartbeat / live-only / monitoring consumers, and write a comment saying why.")
            .build());

    public static final RuleId QK_BLOCKING_MISSING_ON_BLOCKING_LISTENER = register(builder("QK_BLOCKING_MISSING_ON_BLOCKING_LISTENER")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.MEDIUM).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_BLOCKING_MISSING_ON_BLOCKING_LISTENER.md")
            .message("@Incoming method does blocking I/O (JPA / RestClient / Thread.sleep) but is not annotated @Blocking — runs on the event loop.")
            .tagline("SmallRye Reactive Messaging @Incoming methods run on the event loop unless @Blocking moves them off.")
            .mechanism("Quarkus / SmallRye dispatches incoming records on Vert.x event-loop threads. A blocking call on that thread (DB query, HTTP, sleep) stalls the entire loop — every other consumer, HTTP endpoint, and timer goes silent until the call returns.")
            .impact("Tail-latency spikes, health-checks failing, sometimes outright deadlocks. The signal looks like 'the whole app got slow' rather than 'this listener is slow'.")
            .whyMatters("If the listener does any blocking work, mark the method `@Blocking` (or `@Blocking(\"my-pool\")` for isolation). SmallRye then dispatches it on a worker thread and the event loop stays responsive.")
            .build());

    public static final RuleId PRODUCER_TXN_TIMEOUT_TOO_LOW = register(builder("PRODUCER_TXN_TIMEOUT_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_TXN_TIMEOUT_TOO_LOW.md")
            .message("transaction.timeout.ms below 10 s — the broker aborts the transaction before the producer can commit on a normal trip.")
            .tagline("Setting transaction.timeout.ms shorter than 10 s turns routine processing into routine InvalidProducerEpoch.")
            .mechanism("`transaction.timeout.ms` (default 60 s) is how long the broker keeps a producer's transactional state alive between `beginTransaction()` and `commitTransaction()`. If the producer doesn't commit/abort within that window, the broker fences the transactional id — subsequent operations get ProducerFencedException.")
            .impact("On the consume-process-produce pattern (Streams' EOS, or hand-rolled exactly-once), any pause longer than the timeout aborts the transaction: a slow downstream call, a GC pause, a brief broker hiccup. The app sees a ProducerFencedException, has to recreate the producer, and the records since `beginTransaction()` are rolled back — even though the producer thought they were fine.")
            .whyMatters("Default 60 s is the right starting point. Cut it only with very tight latency requirements AND a known-fast processing budget, and bound the upstream call to roughly half the timeout. Below 10 s is almost always a cargo-culted 'fail fast' setting.")
            .build());

    public static final RuleId PRODUCER_TXN_TIMEOUT_TOO_HIGH = register(builder("PRODUCER_TXN_TIMEOUT_TOO_HIGH")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_TXN_TIMEOUT_TOO_HIGH.md")
            .message("transaction.timeout.ms above 900 s — exceeds the broker's transaction.max.timeout.ms default. InitProducerId will fail.")
            .tagline("Brokers cap transaction.timeout.ms at transaction.max.timeout.ms (default 15 min). Going above is a startup error.")
            .mechanism("On `initTransactions()`, the producer sends its requested `transaction.timeout.ms` to the transaction coordinator. The coordinator validates it against the broker-side `transaction.max.timeout.ms` (default 900000). If higher, the broker returns `INVALID_TRANSACTION_TIMEOUT` and the producer constructor throws.")
            .impact("Application fails at boot. The error message is clear once you read it, but the symptom — 'producer constructor failed' — looks like a connectivity issue and is often misdiagnosed for a long time.")
            .whyMatters("Either keep the request below the broker cap, or coordinate with cluster ops to raise `transaction.max.timeout.ms` (which has its own cost — long-running transactions tie up __transaction_state log). Going to 30 min just to 'be safe' is the most common cause.")
            .build());

    public static final RuleId CONSUMER_ISOLATION_LEVEL_READ_UNCOMMITTED_EXPLICIT = register(builder("CONSUMER_ISOLATION_LEVEL_READ_UNCOMMITTED_EXPLICIT")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_ISOLATION_LEVEL_READ_UNCOMMITTED_EXPLICIT.md")
            .message("isolation.level=read_uncommitted explicitly set — consumer reads aborted/in-flight transactional records.")
            .tagline("read_uncommitted on a transactional topic exposes records that were never committed.")
            .mechanism("The consumer's `isolation.level` controls whether it sees records from in-flight or aborted transactions. `read_uncommitted` (the default) returns all records as they're written, including those that the producer later rolled back. `read_committed` skips them.")
            .impact("On a topic produced transactionally, `read_uncommitted` causes downstream consumers to process records that don't exist from the producer's perspective. Counts diverge, duplicates appear, and the bug is invisible from the topic-level lag metric.")
            .whyMatters("On non-transactional topics the setting is irrelevant — both values do the same thing. But seeing `isolation.level=read_uncommitted` explicitly set in a transactional pipeline is almost always a copy-paste from a non-transactional config. Either remove the override (the default is fine for non-transactional flows) or set it to `read_committed`.")
            .build());

    public static final RuleId PRODUCER_BUFFER_MEMORY_TOO_SMALL = register(builder("PRODUCER_BUFFER_MEMORY_TOO_SMALL")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_BUFFER_MEMORY_TOO_SMALL.md")
            .message("buffer.memory below 16 MiB — the accumulator fills under any burst and producer.send() blocks.")
            .tagline("buffer.memory is the producer's total record-accumulator budget. Too small and the hot path blocks waiting for room.")
            .mechanism("The producer buffers records in memory before the sender thread drains them into ProduceRequests. `buffer.memory` is the global cap (default 32 MiB). When the accumulator is full, `send()` blocks for up to `max.block.ms` waiting for the sender to free space.")
            .impact("Under a traffic spike or a brief broker slow-down, the buffer fills, and every producer thread synchronously blocks on `send()`. Application-side latency p99 spikes from microseconds to seconds. With an even bigger spike, `send()` throws TimeoutException after `max.block.ms` — records lost.")
            .whyMatters("Defaults (32 MiB) are right for most apps. Setting `buffer.memory` below 16 MiB is almost always a copy from a sample for a constrained device, or a misguided 'memory tuning' that forgot the impact on the hot path. Either revert or raise `max.block.ms` deliberately.")
            .build());

    public static final RuleId PRODUCER_REQUEST_TIMEOUT_TOO_LOW = register(builder("PRODUCER_REQUEST_TIMEOUT_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_REQUEST_TIMEOUT_TOO_LOW.md")
            .message("request.timeout.ms below 10 s — the producer gives up before the broker has had a chance to ack a normal request.")
            .tagline("request.timeout.ms shorter than 10 s burns retries on routine broker latency, then fails the record.")
            .mechanism("`request.timeout.ms` is how long the producer waits for a ProduceResponse before retrying (counted against `retries`/`delivery.timeout.ms`). The default (30 s in 3.x) leaves headroom for leader-elections, transient broker pauses, and cross-region calls.")
            .impact("Setting it to 5 s or less means a normal cross-AZ produce — which routinely takes 1-3 s during a Kafka rebalance — runs out of time, the producer retries, then the retry runs out of time too. After `delivery.timeout.ms` (also small if you cut this) the record is failed back to the app.")
            .whyMatters("This tuning is usually applied to 'make the producer fail fast' for a circuit-breaker design. The right shape for that is to set a budget on the application call, not to cut the broker-protocol timeout. Leave `request.timeout.ms` at the default; bound the upstream call instead.")
            .build());

    public static final RuleId CONSUMER_SESSION_TIMEOUT_TOO_LOW = register(builder("CONSUMER_SESSION_TIMEOUT_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_SESSION_TIMEOUT_TOO_LOW.md")
            .message("session.timeout.ms below 10 s — routine GC pauses and broker hiccups will trigger spurious rebalances.")
            .tagline("session.timeout.ms is the patience the broker has for a missed heartbeat. Too small means every blip kicks the consumer out of the group.")
            .mechanism("The consumer sends a heartbeat to the group coordinator every `heartbeat.interval.ms` (default 3 s). If no heartbeat arrives within `session.timeout.ms` (default 45 s since 3.0), the broker considers the consumer dead and triggers a rebalance.")
            .impact("With `session.timeout.ms=5000` (a common copy from old guides), a 6-second G1 GC pause or a brief OS thread freeze drops the consumer. Every other group member pauses while the rebalance completes. Throughput drops to zero for the group, not just the affected consumer. The thrash often cascades — the next GC kicks another consumer out.")
            .whyMatters("Default 45 s is correct for most workloads. The justification for shrinking it is 'failover faster' — but the cost (more frequent and unnecessary rebalances) almost always outweighs the benefit. If failover speed matters, use static group membership instead (`group.instance.id`).")
            .build());

    public static final RuleId CONSUMER_MAX_POLL_INTERVAL_MS_TOO_LOW = register(builder("CONSUMER_MAX_POLL_INTERVAL_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_MAX_POLL_INTERVAL_MS_TOO_LOW.md")
            .message("max.poll.interval.ms below 60 s — the group coordinator will evict the consumer mid-batch if a poll cycle takes longer.")
            .tagline("max.poll.interval.ms is the floor on how long the consumer is allowed to spend processing one poll. Too small turns normal processing into a rebalance loop.")
            .mechanism("Between two calls to `poll()`, the consumer keeps sending heartbeats (so `session.timeout.ms` is satisfied), but the group coordinator also tracks `max.poll.interval.ms`. If the gap between polls exceeds this, the coordinator forces a rebalance and kicks the consumer out as 'live-locked'. Default is 300_000 ms (5 minutes).")
            .impact("Set to 30_000 ms by someone copying a 'fast failover' snippet, and any batch that takes more than 30 s — a database write under load, a downstream HTTP call timing out — triggers a rebalance. The consumer rejoins, re-polls the same batch (uncommitted), processes it again, gets evicted again. The group makes zero forward progress while looping on the same records.")
            .whyMatters("This is the classic 'why are my consumers in a rebalance loop' bug. Fix the underlying slow processing (smaller `max.poll.records`, async processing, threadpool with manual offset commit) — don't paper over it by shortening this timeout. If anything, raise it above the default for batch processors with known slow per-record work.")
            .build());

    public static final RuleId PRODUCER_BATCH_SIZE_TOO_SMALL = register(builder("PRODUCER_BATCH_SIZE_TOO_SMALL")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_BATCH_SIZE_TOO_SMALL.md")
            .message("batch.size below the 16384-byte default — partition-level batches stay tiny and the producer issues one ProduceRequest per few records.")
            .tagline("batch.size is the upper bound on how much the accumulator collects per partition before sending. Shrink it and you defeat the only batching the producer does.")
            .mechanism("The producer maintains a per-partition record-batch buffer. It sends the batch when it reaches `batch.size`, when `linger.ms` elapses, or when `flush()`/`close()` runs. The default `16384` (16 KiB) is small enough to keep latency tight and large enough to amortize the produce-RPC overhead.")
            .impact("With `batch.size=1024` (or worse, `0`), every two or three records trigger an immediate send. The broker sees N× more ProduceRequests for the same record volume — broker request-handler CPU, network round-trips, and the ISR-replication cost per batch all multiply. Throughput craters; the producer's record-send-rate metric drops 5-10×.")
            .whyMatters("This rarely improves latency — `linger.ms` already gives that knob. The motivation is almost always 'I read a Kafka guide that said tune batch.size' — applied in the wrong direction. The right tuning is *upwards* (32 KiB, 64 KiB) when throughput matters, never downwards.")
            .build());

    public static final RuleId PRODUCER_LINGER_MS_TOO_HIGH = register(builder("PRODUCER_LINGER_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_LINGER_MS_TOO_HIGH.md")
            .message("linger.ms above 60 s — every send waits up to a minute in the accumulator before going on the wire.")
            .tagline("linger.ms is end-to-end latency the application pays for each record. Above ~60 s, this is almost certainly a typo.")
            .mechanism("`linger.ms` is the maximum time the sender thread waits before dispatching a partial batch. A small value (5-50 ms) trades a touch of latency for tighter batches. A large value (60_000+) means the producer sits on records for that long even when the broker is healthy and idle.")
            .impact("A `linger.ms=300000` typo (intended `300` ms, but `_000` got pasted) means every produced record sits in the accumulator for 5 minutes before send. The application looks like it's working (send() returns instantly, callbacks just don't fire), and downstream consumers see a 5-minute lag. By the time someone notices, hours of records are sitting in the producer's RAM.")
            .whyMatters("This rule isn't about choosing between 5 ms and 50 ms — that's a real tuning decision. It's about catching the units-mistake / extra-zeros class of typo. If you genuinely want a minute of linger, the rule's threshold is wrong for your case and you should disable it; in every other case, you have a bug.")
            .build());

    public static final RuleId PRODUCER_PARTITIONER_CLASS_DEPRECATED = register(builder("PRODUCER_PARTITIONER_CLASS_DEPRECATED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_PARTITIONER_CLASS_DEPRECATED.md")
            .message("partitioner.class points at DefaultPartitioner or UniformStickyPartitioner — both deprecated since Kafka 3.3 (KIP-794).")
            .tagline("KIP-794 made the built-in 'no partitioner.class set' strategy strictly better than the old DefaultPartitioner. Explicitly naming one of the deprecated classes leaves you on the worse code path.")
            .mechanism("Before 3.3, the producer used `DefaultPartitioner` (hash on key, sticky round-robin on null key) or `UniformStickyPartitioner` (sticky regardless of key). Both produce uneven broker load because the 'sticky' strategy keeps writing to a slow partition even after it falls behind. The 3.3 built-in strategy (active when `partitioner.class` is *not* set) uses queue size + RTT feedback to steer records away from slow partitions.")
            .impact("Sticking with the deprecated classes manifests as broker-side imbalance under load: one partition's leader replicates twice as much as its peers, and that hotspot becomes the throughput bottleneck. With the modern strategy, the producer self-corrects and the imbalance disappears.")
            .whyMatters("The fix is to *delete* the `partitioner.class` line entirely — the new default is the right answer. The deprecated classes will be removed in a future major; code that names them will break on upgrade.")
            .build());

    public static final RuleId CONSUMER_FETCH_MAX_BYTES_TOO_LOW = register(builder("CONSUMER_FETCH_MAX_BYTES_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_FETCH_MAX_BYTES_TOO_LOW.md")
            .message("fetch.max.bytes below 1 MiB — consumer cannot pull large batches even when the broker has data ready.")
            .tagline("fetch.max.bytes caps the per-FetchRequest payload. Shrink it and you cap the consumer's throughput regardless of how fast the broker can serve.")
            .mechanism("On `poll()`, the consumer issues a FetchRequest with `fetch.max.bytes` as the upper bound on bytes the broker can return across all assigned partitions. The default is 52 MiB. The minimum useful value is roughly one max-sized record; below 1 MiB you cannot even pull a single batch that uses Kafka's default `message.max.bytes` (1 MiB).")
            .impact("With `fetch.max.bytes=65536` (a common copy-paste from a 'tiny consumer' tutorial), each FetchRequest returns at most 64 KiB. The consumer issues 800+ FetchRequests/sec to keep up with a moderate produce rate, broker request-handler CPU spikes, and the consumer's actual throughput is bounded at roughly `64 KiB / RTT`.")
            .whyMatters("There is no upside to shrinking this — broker memory is not the bottleneck. The right knobs for memory pressure are `max.partition.fetch.bytes` (per-partition) and `max.poll.records` (number of records returned per poll), not the byte cap on the FetchRequest itself.")
            .build());

    public static final RuleId CONSUMER_CHECK_CRCS_FALSE = register(builder("CONSUMER_CHECK_CRCS_FALSE")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_CHECK_CRCS_FALSE.md")
            .message("check.crcs=false — consumer accepts records without verifying the on-the-wire CRC.")
            .tagline("CRC checks are how the consumer detects bit-rot, broker bugs, and network corruption. Turning them off means corrupted records are processed as valid.")
            .mechanism("Every Kafka record-batch is stored with a CRC32C checksum. The consumer recomputes it after decompression and compares — a mismatch raises CorruptRecordException. With `check.crcs=false`, the comparison is skipped entirely and the (potentially corrupt) batch is forwarded to the application.")
            .impact("Bit-flips during broker storage, a faulty NIC on either end of the connection, or a broker-side compression bug all produce records that look valid to the application — wrong field values, truncated payloads, or random schema-validation failures downstream. The first 'evidence' is usually a strange business-logic error that nobody can reproduce.")
            .whyMatters("The CPU cost of CRC verification is in the single-digit percent on the consumer side; modern JVMs use the CRC32C hardware instruction. There is no defensible reason to turn this off in production. The setting exists for benchmarking; if you see it in code, it leaked.")
            .build());

    public static final RuleId PRODUCER_MAX_BLOCK_MS_TOO_LOW = register(builder("PRODUCER_MAX_BLOCK_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_MAX_BLOCK_MS_TOO_LOW.md")
            .message("max.block.ms below 10 s — producer.send() throws on transient metadata-fetch delays instead of waiting them out.")
            .tagline("max.block.ms is how long send() is allowed to wait for metadata or buffer space. Cut it too short and a routine bootstrap-server hiccup becomes a TimeoutException to the caller.")
            .mechanism("On `producer.send()`, the producer may block in two places: waiting for cluster metadata (first send to a topic, leader changes) and waiting for accumulator space (when records pile up). `max.block.ms` (default 60 s) caps the total wait. When it expires, `send()` throws TimeoutException synchronously — your application code sees the failure, not the sender thread.")
            .impact("With `max.block.ms=1000`, a single cross-region metadata fetch — routine after a broker restart or leader move — bubbles up as a TimeoutException from `send()`. The application typically logs and drops the record. A small broker hiccup that the producer would have absorbed in 1.5 s becomes a record-loss event.")
            .whyMatters("The motivation is usually 'send() should never block this thread' — which is reasonable, but the right shape is to keep `max.block.ms` at the default and put the call behind a circuit-breaker or worker pool that owns the latency budget. Treating `max.block.ms` as a per-call latency cap mistakes one knob for another.")
            .build());

    public static final RuleId CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_LOW = register(builder("CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_LOW.md")
            .message("default.api.timeout.ms below 30 s — routine commitSync / position / metadata calls bubble up TimeoutException on normal broker hiccups.")
            .tagline("default.api.timeout.ms backs every consumer/admin call that doesn't take an explicit duration. Cut it short and routine cluster events become application errors.")
            .mechanism("Consumer methods that don't take a `Duration` argument (`commitSync()`, `position(tp)`, `endOffsets(tps)`, `partitionsFor(topic)`) all use `default.api.timeout.ms` (default 60 s) as their wait budget. When a broker is mid-restart or a leader is moving, these calls retry internally; if the timeout is too short, the retry budget is exhausted before the cluster settles.")
            .impact("With `default.api.timeout.ms=5000`, a routine partition-leader move (broker rolling restart, autoscaling event) causes the next `commitSync()` to throw TimeoutException. The application catches it, treats it as fatal, and restarts — or worse, silently retries and ends up double-processing the offset window.")
            .whyMatters("60 s is the right floor; lower values do not 'fail fast' usefully, they just turn routine cluster operations into application errors. If a specific call needs a tighter timeout, use the overload that takes a `Duration` for that one call rather than tightening the default.")
            .build());

    public static final RuleId SPRING_BOOT_AUTO_COMMIT_INTERVAL_TOO_HIGH = register(builder("SPRING_BOOT_AUTO_COMMIT_INTERVAL_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_AUTO_COMMIT_INTERVAL_TOO_HIGH.md")
            .message("spring.kafka.consumer.auto-commit-interval > 60 s — combined with auto-commit=true, every crash loses that much processed work.")
            .tagline("auto.commit.interval.ms is the size of the window of records you can lose on a crash. Stretch it past a minute and the loss window becomes operationally painful.")
            .mechanism("With `enable.auto.commit=true`, the consumer commits the highest in-memory offset every `auto.commit.interval.ms` (default 5 s). Between commits, the consumer has fetched + handed-to-application records whose offsets are not yet on the broker. A crash or rebalance during that window re-processes (at-least-once) those records.")
            .impact("Setting `spring.kafka.consumer.auto-commit-interval=300000` (5 min) means up to 5 minutes of records are reprocessed after each crash. For idempotent consumers that's annoying duplication; for non-idempotent consumers (counters, side-effects), it's a correctness bug.")
            .whyMatters("This setting is almost never the right knob to tune. The right shape is manual ack-mode + `commitSync()` after the unit-of-work completes — Spring's `AckMode.MANUAL_IMMEDIATE` is the well-trodden path. Raising the auto-commit interval just trades a smaller knob (commit RPC rate) against a much larger one (reprocessing window).")
            .build());

    public static final RuleId STREAMS_KSTREAM_PRINT = register(builder("STREAMS_KSTREAM_PRINT")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_KSTREAM_PRINT.md")
            .message("KStream.print(Printed.toSysOut()) — debugging operator left in production topology.")
            .tagline("KStream.print() pipes every record through System.out (or a writer). It is a debugging helper that has no business in a deployed topology.")
            .mechanism("`KStream.print(Printed.toSysOut())` inserts a `PrintForeachAction` processor that calls `System.out.println(record)` on every key/value. Even when redirected to a file, this is a synchronous I/O step inserted in the middle of the processing graph.")
            .impact("On a topology that handles thousands of records per second, this becomes the bottleneck — `System.out` is line-synchronized, so the print step serialises every Streams thread through one lock. In production, you also lose the records to a place nobody reads (the container stdout, often capped at a few MB).")
            .whyMatters("This is a 'forgot to remove' bug — easy to write in a notebook or local test, easy to leave in code if not checked. If you genuinely need to inspect records, use `peek()` with a metrics counter, or write to a dedicated debug topic. Never wire `System.out` into a stream.")
            .build());

    public static final RuleId STREAMS_TASK_TIMEOUT_MS_ZERO = register(builder("STREAMS_TASK_TIMEOUT_MS_ZERO")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_TASK_TIMEOUT_MS_ZERO.md")
            .message("task.timeout.ms=0 — the first transient broker error kills the task instead of retrying.")
            .tagline("task.timeout.ms is the retry-on-transient-error budget per Streams task. Setting it to 0 means 'crash on the first hiccup' — the opposite of what production wants.")
            .mechanism("When a Streams task hits a transient error (TimeoutException, broker disconnect, NotLeaderForPartition), the runtime retries internally for up to `task.timeout.ms` before raising the error to the global handler. The default is 5 minutes; the floor 0 means 'no retries'.")
            .impact("With `task.timeout.ms=0`, a routine leader-election (which the broker resolves in 1-3 s) shows up as a task failure, which (depending on the StreamsUncaughtExceptionHandler) either restarts the thread or kills the whole app. The application looks unstable and flaps under normal cluster operation.")
            .whyMatters("This is almost never what users want — they usually set it during testing to 'fail fast' and forget to remove it. The default is the right answer in production. If you genuinely need faster failure surfacing, do it at the handler level, not by zeroing the retry budget.")
            .build());

    public static final RuleId STREAMS_APPLICATION_ID_GENERIC = register(builder("STREAMS_APPLICATION_ID_GENERIC")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_APPLICATION_ID_GENERIC.md")
            .message("application.id is a generic placeholder (streams-app, test, demo, ...). Two apps with the same id share a consumer group, changelog topics, and state-store names.")
            .tagline("application.id is the unique identity of a Streams application across the cluster. A generic value collides with the next person who also leaves it at the default.")
            .mechanism("Kafka Streams uses `application.id` as the consumer group, as the prefix for internal changelog/repartition topics (e.g. `<appId>-KSTREAM-...-changelog`), as the embedded admin-client `client.id`, and as part of the local RocksDB path. The same id from two different apps means both will join the same consumer group, both will try to claim the same changelog topics, and the topology layouts must match — or the apps fight over the same state.")
            .impact("Two services both shipping with `application.id=streams-app` will rebalance against each other every time either restarts, will try to write each other's changelog records, and will fail with InvalidTopicException once their topologies diverge. The 'fix' often looks like flaky CI or 'random' rebalances and takes days to diagnose.")
            .whyMatters("Pick a value that includes the service name and ideally the topology version: `payments-fraud-screening-v3`. Treat changing it like a database name change — it is one. This rule catches the copy-paste-tutorial class of bug, where the placeholder ships to staging without being changed.")
            .build());

    public static final RuleId STREAMS_NUM_STANDBY_REPLICAS_ZERO = register(builder("STREAMS_NUM_STANDBY_REPLICAS_ZERO")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_NUM_STANDBY_REPLICAS_ZERO.md")
            .message("num.standby.replicas=0 — any instance failure forces a full changelog restore on a peer.")
            .tagline("With zero standbys, every Streams instance death costs you a from-scratch state rebuild.")
            .mechanism("`num.standby.replicas` controls how many warm copies of each state-store partition are maintained on other instances. With 0, when an instance dies the new owner must replay the entire `changelog` topic from earliest before resuming processing.")
            .impact("Recovery time scales linearly with state size — minutes-to-hours for medium-sized RocksDB stores. During recovery the new owner processes no input on its assigned tasks; downstream consumers see a stall. Throughput also drops on the surviving instance that's now doing the restore I/O on top of its own work.")
            .whyMatters("Set `num.standby.replicas=1` (or more) so a warm copy is ready. The trade is broker storage + replication bandwidth for changelog data — usually a much smaller cost than the operational disruption. Default is 0, which is rarely what you want in production.")
            .build());

    public static final RuleId STREAMS_DESER_HANDLER_LOG_AND_CONTINUE = register(builder("STREAMS_DESER_HANDLER_LOG_AND_CONTINUE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_DESER_HANDLER_LOG_AND_CONTINUE.md")
            .message("default.deserialization.exception.handler=LogAndContinueExceptionHandler — poison-pill records silently skipped.")
            .tagline("LogAndContinue swallows undeserializable records. Data loss looks like a log line, not an error.")
            .mechanism("`LogAndContinueExceptionHandler` catches deserialization exceptions, logs them at WARN, and tells the runtime to skip the record. The Streams app keeps running as if nothing happened. The default — `LogAndFailExceptionHandler` — would have stopped the app and forced a fix.")
            .impact("A schema change, a malformed producer, or a serializer-version mismatch produces a steady drip of skipped records. The topic looks healthy from a lag-metrics perspective (offsets advance) but you're losing data. Nobody notices until a downstream report shows a hole that no integrator can explain.")
            .whyMatters("If you genuinely want to skip poison-pills, route them to a Dead-Letter Queue instead — `SendToDeadLetterQueueExceptionHandler` or a custom handler. Pure 'log and continue' is almost never the right operational stance in a system you care about.")
            .build());

    public static final RuleId STREAMS_CACHE_KEY_DEPRECATED = register(builder("STREAMS_CACHE_KEY_DEPRECATED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_CACHE_KEY_DEPRECATED.md")
            .message("cache.max.bytes.buffering is deprecated since Kafka 3.4 — use statestore.cache.max.bytes.")
            .tagline("The Streams cache config was renamed in 3.4. The old key still works but emits a warning at every startup.")
            .mechanism("`cache.max.bytes.buffering` was the original key for the Streams record-cache size (used for KTable update suppression and aggregator hot-batching). KIP-770 renamed it to `statestore.cache.max.bytes` in 3.4 to clarify scope. The old key is still honored but will be removed in a future major.")
            .impact("Nothing breaks today, but each app startup logs a deprecation warning, and a future upgrade will silently ignore the old setting — at which point the cache reverts to its 10MB-per-thread default and you may see unexpected changelog write amplification.")
            .whyMatters("This is a 30-second fix: rename the property. Doing it now removes the deprecation noise and protects against a silent regression on the next major upgrade.")
            .build());

    // ────────────────────────────────────────────────────────────────────────
    // observability/ — security & deserialization
    // ────────────────────────────────────────────────────────────────────────

    public static final RuleId DESER_JSON_TYPE_INFO_NO_ALLOWLIST = register(builder("DESER_JSON_TYPE_INFO_NO_ALLOWLIST")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("observability")
            .docPath("observability/DESER_JSON_TYPE_INFO_NO_ALLOWLIST.md")
            .message("Jackson polymorphic deserialization (@JsonTypeInfo / default typing) without an allowlist — RCE-class footgun.")
            .tagline("Polymorphic JSON deserialization with no allow-list lets a malicious record instantiate arbitrary classes on the classpath.")
            .mechanism("Jackson's `@JsonTypeInfo` and `ObjectMapper.activateDefaultTyping()` embed a type-name in the JSON. The deserializer reflects on the type-name to pick a class to instantiate. With no allow-list, the type-name can name any class — and Jackson will call its no-arg constructor or setters.")
            .impact("Same shape as the Java deserialization CVE family: a crafted Kafka record triggers loading of a 'gadget' class (e.g. one whose setter executes shell). RCE in the consumer's JVM with its full privileges.")
            .whyMatters("Either set `PolymorphicTypeValidator` explicitly with an allow-list, or — strongly preferred — don't use default typing at all. Use explicit `@JsonSubTypes` enumerations. Kafka topics are an untrusted input from a security perspective.")
            .build());

    public static final RuleId SCHEMA_REGISTRY_URL_MISSING = register(builder("SCHEMA_REGISTRY_URL_MISSING")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.MEDIUM).category("observability")
            .docPath("observability/SCHEMA_REGISTRY_URL_MISSING.md")
            .message("Avro/Protobuf/JSON Schema serializer in use but schema.registry.url is not configured — serializer cannot register / lookup schemas.")
            .tagline("Confluent SR serializers fail at construction without schema.registry.url. The error is a deep ClassCastException, not an obvious 'missing config'.")
            .mechanism("Confluent's `KafkaAvroSerializer` / `KafkaProtobufSerializer` / `KafkaJsonSchemaSerializer` look up schemas at the URL in `schema.registry.url`. Without it, the serializer either fails at config-time or attempts to use a null URL with confusing downstream errors.")
            .impact("Producer / consumer fails to construct, or fails on the first record. In Spring/Quarkus the failure cascades through auto-config and the root cause is buried in the stack trace.")
            .whyMatters("Set `schema.registry.url` (and `basic.auth.user.info` if your registry is auth'd). The lint catches the missing key; the real source of bugs is the URL being right but the credentials wrong.")
            .build());

    // ────────────────────────────────────────────────────────────────────────
    // security/ — transport & authentication hygiene
    // ────────────────────────────────────────────────────────────────────────

    public static final RuleId SECURITY_PROTOCOL_PLAINTEXT = register(builder("SECURITY_PROTOCOL_PLAINTEXT")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("security")
            .docPath("security/SECURITY_PROTOCOL_PLAINTEXT.md")
            .message("security.protocol=PLAINTEXT — Kafka traffic (records + offsets + metadata) travels in clear over the wire.")
            .tagline("PLAINTEXT means no TLS and no authentication. Anyone on the path can read and inject records.")
            .mechanism("`security.protocol=PLAINTEXT` (the default when no security is configured) disables both TLS encryption and any SASL handshake. Every produce/fetch request, every offset commit, every consumer-group heartbeat is wire-readable.")
            .impact("On any network that isn't a fully-isolated VPC: payloads (including PII), headers (including auth tokens passed via headers), and credentials in record values are visible to anyone running tcpdump. The connection is also unauthenticated — a rogue producer can write to any topic.")
            .whyMatters("PLAINTEXT is fine for `docker compose up` and laptop tests, never for shared/staging/prod brokers. Use `SASL_SSL` (the realistic default for managed brokers like Confluent Cloud or MSK) or at minimum `SSL`. Setting this once in shared config is one line; auditing a leak after the fact is not.")
            .build());

    public static final RuleId SECURITY_PROTOCOL_SASL_PLAINTEXT = register(builder("SECURITY_PROTOCOL_SASL_PLAINTEXT")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("security")
            .docPath("security/SECURITY_PROTOCOL_SASL_PLAINTEXT.md")
            .message("security.protocol=SASL_PLAINTEXT — SASL credentials (and all record bytes) travel in clear.")
            .tagline("SASL_PLAINTEXT authenticates but doesn't encrypt. The username/password are visible on the wire.")
            .mechanism("With `SASL_PLAINTEXT`, the client does a SASL handshake (PLAIN, SCRAM, GSSAPI…) over an un-encrypted TCP socket. PLAIN puts the password on the wire literally; SCRAM puts a salted hash, but record bytes are clear regardless.")
            .impact("Anyone capturing traffic between the client and the broker — a misconfigured load balancer, a side-car proxy, a malicious node on the path — can read records and, with PLAIN, lift credentials directly. With SCRAM they can still see all messages, just not the password.")
            .whyMatters("The fix is one character: `SASL_SSL`. The TLS layer wraps the SASL handshake and the record stream. SASL_PLAINTEXT is almost always a mis-copy from a lab guide that nobody fixed before going to prod.")
            .build());

    public static final RuleId CRED_SASL_JAAS_LITERAL = register(builder("CRED_SASL_JAAS_LITERAL")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("security")
            .docPath("security/CRED_SASL_JAAS_LITERAL.md")
            .message("sasl.jaas.config set to a literal containing a password — credentials shipped in source/properties.")
            .tagline("A password baked into source or application.properties travels in every CI artifact, every container image, and every git history page.")
            .mechanism("The Kafka client reads `sasl.jaas.config` as the literal value of the JAAS login-module configuration string, e.g. `org.apache.kafka.common.security.plain.PlainLoginModule required username=\"app\" password=\"hunter2\";`. The value is consumed verbatim — there is no built-in env-resolution.")
            .impact("Once a password is in a git commit, it has to be considered compromised regardless of subsequent deletion. Container registries and CI logs preserve the artifact indefinitely. Rotating the password requires coordination across every consumer of that secret.")
            .whyMatters("Inject the secret at runtime: env-var substitution (`password=\"${KAFKA_PASSWORD}\"`), Spring's `${...}` placeholders, Kubernetes secret mounts, Vault Agent. The lint accepts placeholder-style values (containing `${`); anything else with `password=` is flagged.")
            .build());

    public static final RuleId CRED_BASIC_AUTH_USER_INFO_LITERAL = register(builder("CRED_BASIC_AUTH_USER_INFO_LITERAL")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("security")
            .docPath("security/CRED_BASIC_AUTH_USER_INFO_LITERAL.md")
            .message("basic.auth.user.info set to a literal user:password — Schema Registry credentials shipped in source/properties.")
            .tagline("Schema Registry basic-auth credentials baked into config are the easiest secrets to leak — they're in plain text and look like a normal URL.")
            .mechanism("Confluent's Schema Registry client reads `basic.auth.user.info` as `<user>:<password>` and base64-encodes it as the Authorization header. There is no env-resolution; the literal value is forwarded as configured.")
            .impact("Same shape as any committed secret — git history, CI logs, container images keep it forever. Worse, schema-registry credentials often grant read AND write to subjects, so a leak lets an attacker corrupt schema evolution for the entire org.")
            .whyMatters("Use a runtime placeholder (`${SR_AUTH}`) and inject the value from env/Vault/k8s secret. Lint accepts placeholder-style values; anything else with a `:` separator is flagged.")
            .build());

    public static final RuleId CRED_SSL_KEYSTORE_PASSWORD_LITERAL = register(builder("CRED_SSL_KEYSTORE_PASSWORD_LITERAL")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("security")
            .docPath("security/CRED_SSL_KEYSTORE_PASSWORD_LITERAL.md")
            .message("ssl.keystore.password (or truststore password) set to a literal — TLS material credential shipped in config.")
            .tagline("Keystore passwords in config files are credentials. The fact that they unlock a file instead of a service doesn't change the leak shape.")
            .mechanism("The Kafka client reads `ssl.keystore.password` / `ssl.truststore.password` / `ssl.key.password` as plain string values used to unlock the JKS/PKCS12 store at startup. The store typically lives in the same repo or container layer; if the password is also there, the bundle is unlocked by anyone with the artifact.")
            .impact("A leaked keystore password lets an attacker present the broker's mTLS client identity. On a cluster that relies on mTLS for authentication, that's full impersonation of the application. The blast radius is the topics that identity is authorized for.")
            .whyMatters("Inject at runtime (`${KAFKA_KEYSTORE_PASS}`) and pair it with secret-store-mounted keystore files. Lint accepts placeholder-style values; non-empty literals are flagged.")
            .build());

    public static final RuleId SR_AUTO_REGISTER_SCHEMAS_TRUE = register(builder("SR_AUTO_REGISTER_SCHEMAS_TRUE")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("security")
            .docPath("security/SR_AUTO_REGISTER_SCHEMAS_TRUE.md")
            .message("auto.register.schemas=true on a producer — the app writes new schemas to the registry on its own. Governance bypass.")
            .tagline("Producers should not be able to register schemas in production. That's a CI/CD step, not an app step.")
            .mechanism("With `auto.register.schemas=true` (the default for Confluent serializers), the first record with a new or modified schema triggers a POST to the registry's `/subjects/<topic>-value/versions` endpoint. The new version is created on the fly under whatever compatibility rule is set on the subject.")
            .impact("A producer rolled out with a buggy schema silently creates a new version that all downstream consumers then have to deal with. If compatibility is set to NONE (or no consumers exist yet), incompatible breaking changes ship without review. Schema evolution stops being a deliberate process.")
            .whyMatters("Set `auto.register.schemas=false` in non-development environments and register schemas through CI (e.g. Gradle/Maven Schema Registry plugins, or a dedicated job). The lint catches the explicit-true; the bigger fix is making sure the property is set in the deployed config, not just absent.")
            .build());

    public static final RuleId SR_USE_LATEST_VERSION_TRUE = register(builder("SR_USE_LATEST_VERSION_TRUE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("security")
            .docPath("security/SR_USE_LATEST_VERSION_TRUE.md")
            .message("use.latest.version=true without latest.compatibility.strict=true — producers may write records with an incompatible latest schema.")
            .tagline("Pinning to 'latest' without strict compatibility means schema rollbacks can corrupt the topic.")
            .mechanism("`use.latest.version=true` tells the serializer to always serialize against the latest registered version of the subject, rather than the schema embedded in the producer's POJO. Without `latest.compatibility.strict=true`, the serializer doesn't verify that the runtime schema is compatible with that latest — it just uses it.")
            .impact("If someone registers an incompatible schema (manually, or via a bad CI run), every producer immediately starts writing records that consumers can't deserialize. The breakage is global and immediate, not isolated to the producer that changed.")
            .whyMatters("Either set both `use.latest.version=true` AND `latest.compatibility.strict=true`, or leave both unset and let each producer's embedded schema drive registration. Half-configured is the dangerous state.")
            .build());

    public static final RuleId SCHEMA_REGISTRY_URL_HTTP = register(builder("SCHEMA_REGISTRY_URL_HTTP")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("security")
            .docPath("security/SCHEMA_REGISTRY_URL_HTTP.md")
            .message("schema.registry.url uses http:// — credentials and schemas travel in clear.")
            .tagline("HTTP to the Schema Registry leaks subject definitions and any basic-auth user/password on every request.")
            .mechanism("`schema.registry.url=http://...` makes every schema lookup, registration, and authentication request go over a plain TCP socket. If `basic.auth.credentials.source=USER_INFO` is set, the user:password is base64-encoded in an Authorization header — readable to anyone on the path.")
            .impact("On any non-isolated network, basic-auth credentials are recoverable from a single packet capture. Even without auth, subject schemas (which often encode business-domain detail like field names) are visible to passive observers.")
            .whyMatters("Switch to `https://` and put the Schema Registry behind TLS. Confluent Cloud and managed registries are HTTPS by default — this lint catches the case where someone copied an example URL or set up a local dev registry that survived into the deployed config.")
            .build());

    public static final RuleId SSL_ENDPOINT_IDENTIFICATION_DISABLED = register(builder("SSL_ENDPOINT_IDENTIFICATION_DISABLED")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("security")
            .docPath("security/SSL_ENDPOINT_IDENTIFICATION_DISABLED.md")
            .message("ssl.endpoint.identification.algorithm set to empty — disables hostname verification (MITM vector).")
            .tagline("Empty endpoint-identification algorithm turns off hostname check. TLS becomes 'encrypted to anyone who has any cert'.")
            .mechanism("By default the Kafka client requires the broker's certificate CN/SAN to match the hostname it connected to (`https`-style verification). Setting `ssl.endpoint.identification.algorithm=` (empty) skips that check — the client accepts any cert signed by a trusted CA, regardless of which host presents it.")
            .impact("A MITM with a cert valid for some other host on the same CA chain (or a compromised internal CA) can transparently intercept all Kafka traffic. The TLS handshake succeeds, the client is happy, every record flows through the attacker.")
            .whyMatters("Almost always set to empty as a workaround for self-signed certs or hostname mismatches during testing — then never reverted. The correct fix is to issue a cert with the right SAN, or to use the broker's internal hostname. Setting this to empty is equivalent to disabling TLS for any security purpose.")
            .build());

    // ────────────────────────────────────────────────────────────────────────
    // Plumbing
    // ────────────────────────────────────────────────────────────────────────

    private final String id;
    private final Severity defaultSeverity;
    private final Confidence confidence;
    private final String category;
    private final String docPath;
    private final String message;
    private final String tagline;
    private final String mechanism;
    private final String impact;
    private final String whyMatters;

    private RuleId(Builder b) {
        this.id = Objects.requireNonNull(b.id, "id");
        this.defaultSeverity = Objects.requireNonNull(b.defaultSeverity, "defaultSeverity");
        this.confidence = Objects.requireNonNull(b.confidence, "confidence");
        this.category = Objects.requireNonNull(b.category, "category");
        this.docPath = Objects.requireNonNull(b.docPath, "docPath");
        this.message = Objects.requireNonNull(b.message, "message");
        this.tagline = Objects.requireNonNull(b.tagline, "tagline");
        this.mechanism = Objects.requireNonNull(b.mechanism, "mechanism");
        this.impact = Objects.requireNonNull(b.impact, "impact");
        this.whyMatters = Objects.requireNonNull(b.whyMatters, "whyMatters");
    }

    private static Builder builder(String id) {
        return new Builder().id(id);
    }

    private static RuleId register(RuleId rule) {
        RuleId prev = REGISTRY.putIfAbsent(rule.id, rule);
        if (prev != null) {
            throw new IllegalStateException("Duplicate RuleId registration: " + rule.id);
        }
        return rule;
    }

    public String id() { return id; }
    public String name() { return id; }
    public Severity defaultSeverity() { return defaultSeverity; }
    public Confidence confidence() { return confidence; }
    public String category() { return category; }
    public String docPath() { return docPath; }
    public String message() { return message; }
    public String tagline() { return tagline; }
    public String mechanism() { return mechanism; }
    public String impact() { return impact; }
    public String whyMatters() { return whyMatters; }

    public static Collection<RuleId> values() {
        return Collections.unmodifiableCollection(REGISTRY.values());
    }

    public static RuleId valueOf(String id) {
        RuleId r = REGISTRY.get(id);
        if (r == null) {
            throw new IllegalArgumentException("Unknown rule id: " + id);
        }
        return r;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RuleId other && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id;
    }

    private static final class Builder {
        private String id;
        private Severity defaultSeverity;
        private Confidence confidence;
        private String category;
        private String docPath;
        private String message;
        private String tagline;
        private String mechanism;
        private String impact;
        private String whyMatters;

        Builder id(String v) { this.id = v; return this; }
        Builder defaultSeverity(Severity v) { this.defaultSeverity = v; return this; }
        Builder confidence(Confidence v) { this.confidence = v; return this; }
        Builder category(String v) { this.category = v; return this; }
        Builder docPath(String v) { this.docPath = v; return this; }
        Builder message(String v) { this.message = v; return this; }
        Builder tagline(String v) { this.tagline = v; return this; }
        Builder mechanism(String v) { this.mechanism = v; return this; }
        Builder impact(String v) { this.impact = v; return this; }
        Builder whyMatters(String v) { this.whyMatters = v; return this; }
        RuleId build() { return new RuleId(this); }
    }
}
