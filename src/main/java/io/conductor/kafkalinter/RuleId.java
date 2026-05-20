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

    public static final RuleId CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_LOW = register(builder("CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_LOW.md")
            .message("heartbeat.interval.ms below 1 s — consumer floods the group coordinator and adds broker-side CPU for no upside.")
            .tagline("heartbeat.interval.ms is how often the consumer pings the group coordinator. Under 1 s, you pay the broker every cycle for no failure-detection benefit.")
            .mechanism("The consumer's heartbeat thread sends a HeartbeatRequest every `heartbeat.interval.ms` (default 3 s) to keep its group membership alive. The coordinator declares the consumer dead only after `session.timeout.ms` elapses without one. Heartbeats are cheap individually but the broker pays per request — request-handler thread time, log-segment IO if there's group state to update.")
            .impact("With `heartbeat.interval.ms=100`, a 50-consumer group sends 500 heartbeats/sec to one broker (the group coordinator) just to maintain liveness. Broker request-handler queues build up, the coordinator broker's metrics blame 'unfair traffic share', and the failure-detection latency you gained (faster eviction) is measured in milliseconds — far below GC pause times anyway.")
            .whyMatters("3 s default is the right answer. The standard advice is `heartbeat.interval.ms ≈ session.timeout.ms / 3`. If someone has shrunk this 'to fail faster', they have misunderstood the design — `session.timeout.ms` is the actual failure-detection knob; the heartbeat interval just controls how often we check.")
            .build());

    public static final RuleId PRODUCER_RECONNECT_BACKOFF_MS_TOO_LOW = register(builder("PRODUCER_RECONNECT_BACKOFF_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_RECONNECT_BACKOFF_MS_TOO_LOW.md")
            .message("reconnect.backoff.ms below 100 ms — broker outage turns into a tight reconnect loop from this client.")
            .tagline("reconnect.backoff.ms is the floor on how fast the client retries a TCP connect after a failure. Below 100 ms it becomes a synthetic DDoS on the broker the moment it tries to come back.")
            .mechanism("On TCP connect failure the client waits `reconnect.backoff.ms`, then retries (with exponential growth up to `reconnect.backoff.max.ms`). The default is 50 ms — already aggressive. Shrinking it further turns the connect loop into a CPU-bound spin until the broker accepts.")
            .impact("During a broker-side outage (a network split, a rolling restart, a CrashLoopBackOff), every client in your fleet hammers the recovering broker's listener thread before it has finished binding. The broker accepts, immediately gets overloaded by the next 500 reconnect attempts, drops the next connection, and the loop reseeds. Recovery from a one-second outage stretches to minutes.")
            .whyMatters("Default 50 ms (with the max growing to 1 s) is the right shape. Cutting it further is almost always copy-paste of someone else's misunderstanding. If you want fast first-attempt reconnect, that's already the default; if you want quiet behavior during outage, *raise* this knob, don't lower it.")
            .build());

    public static final RuleId QK_OUTGOING_ACKS_NOT_ALL = register(builder("QK_OUTGOING_ACKS_NOT_ALL")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_OUTGOING_ACKS_NOT_ALL.md")
            .message("mp.messaging.outgoing.<channel>.acks not 'all' — partial durability on this outgoing channel.")
            .tagline("acks decides how many replicas must persist a record before the broker says OK. Anything but 'all' opens a window where 'sent successfully' means 'gone after a leader crash'.")
            .mechanism("On the Quarkus / SmallRye outgoing channel, the underlying producer's `acks` setting controls how many in-sync replicas must write before the broker ACKs. `acks=0` ACKs immediately (no broker confirmation), `acks=1` ACKs as soon as the leader writes (lost if leader dies before replication), `acks=all` (or `-1`) waits for all in-sync replicas — the only durable setting.")
            .impact("With `acks=1` on a critical pipeline (payment events, audit logs), a single broker death during the few-hundred-ms replication gap causes silent message loss. The producer's `send()` callback fires success, downstream consumers never see the record, and there is no record-loss metric — because from the producer's point of view, nothing failed.")
            .whyMatters("The kafka-clients default has been `acks=all` since 3.0, but the Quarkus channel still honors an explicit override. Treat any explicit non-'all' value as something that needs justification — usually the answer is 'this was copied from a tutorial' and it should be removed.")
            .build());

    public static final RuleId CONSUMER_GROUP_ID_GENERIC = register(builder("CONSUMER_GROUP_ID_GENERIC")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_GROUP_ID_GENERIC.md")
            .message("group.id is a generic placeholder (group, my-group, test, demo, ...). Two apps with the same group.id share the same partition assignment.")
            .tagline("group.id is how Kafka decides which consumers split work. A generic value means colliding with whoever else also forgot to change it.")
            .mechanism("The consumer group coordinator uses `group.id` as the identity for the partition-assignment protocol. All consumers sharing a `group.id` form one group: partitions are split across them, offsets are committed against this id, and a join triggers a rebalance for the whole set.")
            .impact("Two unrelated services both shipping with `group.id=my-group` end up in the same group. Each thinks it owns the topic; the assignor splits partitions arbitrarily; each commits offsets that the other will never see. The consequence is silent data loss for one service and silent duplicate-processing for the other — and a rebalance every time either restarts.")
            .whyMatters("Treat `group.id` like a database name. Use a service-qualified value: `payments-fraud-screening`, `audit-log-writer-v2`. This rule catches the leftover-from-tutorial class of bug, where the placeholder ships to staging without being changed.")
            .build());

    public static final RuleId PRODUCER_DELIVERY_TIMEOUT_MS_TOO_HIGH = register(builder("PRODUCER_DELIVERY_TIMEOUT_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_DELIVERY_TIMEOUT_MS_TOO_HIGH.md")
            .message("delivery.timeout.ms above 10 minutes — the producer will hold and retry a record for that long before surfacing failure.")
            .tagline("delivery.timeout.ms is the upper bound on a record's lifetime inside the producer. Set it too high and stuck records pile up in RAM while the application looks healthy.")
            .mechanism("`delivery.timeout.ms` (default 120 s) bounds the total time from `send()` enqueue to either ACK or final failure callback. While this clock runs, the producer keeps retrying — through leader-elections, network blips, broker rolling restarts.")
            .impact("Setting it to 3_600_000 ms (1 h) means a record that gets stuck because of an ACL change, a topic-rename, or a misconfigured client keeps occupying buffer.memory for an hour before the callback fires. By then, many other records have been blocked behind it (head-of-line in the accumulator), the producer's in-memory queue is full, and `send()` starts throwing BufferExhaustedException without ever telling you why.")
            .whyMatters("The right ceiling is on the order of minutes, not hours. If you genuinely need very long retry tolerance, build it at the application layer (durable outbox + retry job), not inside the producer's RAM. This rule catches the units-confusion class of typo — someone meant 300 s, typed 300000 thinking it was seconds.")
            .build());

    public static final RuleId CONSUMER_FETCH_MIN_BYTES_TOO_HIGH = register(builder("CONSUMER_FETCH_MIN_BYTES_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_FETCH_MIN_BYTES_TOO_HIGH.md")
            .message("fetch.min.bytes above 10 MiB — consumer parks the fetch until that much data accumulates or fetch.max.wait.ms elapses. Latency cliff.")
            .tagline("fetch.min.bytes is a 'don't bother responding until you have at least this much' hint to the broker. Set it large and you trade latency for batch size on every poll.")
            .mechanism("On a FetchRequest, the broker waits until either (a) the available data for the consumer's assigned partitions exceeds `fetch.min.bytes`, or (b) `fetch.max.wait.ms` (default 500 ms) elapses. The default `fetch.min.bytes=1` returns whatever is available. Raising it lets the broker batch — but the cost is the wait.")
            .impact("With `fetch.min.bytes=52428800` (50 MiB), every poll on a low-traffic topic waits the full `fetch.max.wait.ms` (default 500 ms) every time. End-to-end latency jumps from a few ms to 500+ ms per poll. On a streaming app, this stalls the topology; on a request/response consumer (rare but real), it adds half a second per request.")
            .whyMatters("This knob has a legitimate use (high-throughput batch consumer that prefers latency for throughput) but the threshold for 'too high' is much lower than the value users tend to pick. 10 MiB is already aggressive; above that almost always reflects a misunderstanding (someone thought it was a buffer size, not a wait condition).")
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

    public static final RuleId PRODUCER_RETRY_BACKOFF_MS_TOO_LOW = register(builder("PRODUCER_RETRY_BACKOFF_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_RETRY_BACKOFF_MS_TOO_LOW.md")
            .message("retry.backoff.ms below 50 ms — retry loop pounds the broker before it has time to recover.")
            .tagline("retry.backoff.ms is the wait between produce retries. Under 50 ms it stops being a backoff and becomes a tight loop that prevents the broker from recovering.")
            .mechanism("When a `produce` request fails with a retriable error (NOT_LEADER_FOR_PARTITION, REQUEST_TIMED_OUT, NETWORK_EXCEPTION), the producer waits `retry.backoff.ms` (default 100 ms, growing exponentially up to `retry.backoff.max.ms`) before retrying. The default exists to give the broker time to elect a new leader, finish a restart, or unblock its request queue.")
            .impact("With `retry.backoff.ms=10`, a leader election (typically 200-1000 ms broker-side) gets hit by 20-100 produce retries from every producer in the fleet. The broker's request-handler threads spend their first second post-election processing retries from clients that already gave up — and the next election starts before they have caught up. A routine rolling restart turns into a partial outage.")
            .whyMatters("The motivation is usually 'we want faster recovery from a single network blip'. But the bottleneck on recovery isn't producer wait time, it's broker readiness — and producers spamming a recovering broker make recovery *slower*. The default 100 ms is right; if anything, raise it under heavy load, never lower it.")
            .build());

    public static final RuleId CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_HIGH = register(builder("CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_HIGH.md")
            .message("auto.commit.interval.ms above 60 s — combined with auto-commit, a crash or rebalance reprocesses that whole window.")
            .tagline("auto.commit.interval.ms is the size of your at-least-once duplicate window. Stretch it past a minute and every restart re-runs a minute's worth of records.")
            .mechanism("With `enable.auto.commit=true` (Kafka's default), the consumer commits the highest offset it has *seen via poll()* — not necessarily processed — every `auto.commit.interval.ms` (default 5 s). Between commits, records have been delivered to the application but not yet acknowledged broker-side. A crash, kill, or rebalance during that interval causes the new owner to replay every record back to the last committed offset.")
            .impact("Setting `auto.commit.interval.ms=300000` (5 min) on a 10k-records-per-second consumer means a kill or rolling restart replays up to 3 million records. For idempotent consumers it is annoying duplication; for non-idempotent code (counters, side-effects, money), it is a correctness bug that surfaces as 'why are some numbers off after each deploy?'.")
            .whyMatters("This knob almost never deserves to be tuned upward. The right fix when commit RPC overhead is a problem is to move to manual commits (`enable.auto.commit=false` + `commitSync()` after processing), not to widen the loss window. The default 5 s is already the cheapest the broker cares about.")
            .build());

    public static final RuleId PRODUCER_MAX_REQUEST_SIZE_TOO_HIGH = register(builder("PRODUCER_MAX_REQUEST_SIZE_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_MAX_REQUEST_SIZE_TOO_HIGH.md")
            .message("max.request.size above 10 MiB — single-record cliff that brokers reject and consumers cannot read by default.")
            .tagline("max.request.size lets a producer assemble huge batches in memory, but the broker still has its own per-message cap. Set it too high and the request is fine on the wire and rejected by the server.")
            .mechanism("`max.request.size` (default 1 MiB) is the producer-side cap on the size of a single produce request, which in practice is the cap on the largest record the producer will accept from `send()`. The broker has an independent cap (`message.max.bytes` topic-level, default 1 MiB) and the consumer has another (`fetch.max.bytes` / `max.partition.fetch.bytes`). Raising one without raising the others creates a silent asymmetry.")
            .impact("Setting `max.request.size=104857600` (100 MiB) without touching broker/topic `message.max.bytes` means every record larger than 1 MiB makes it through `send()`, gets buffered in the accumulator, batches with friends, then dies broker-side with RecordTooLargeException — but on the producer it looks like 'some random sends fail with no obvious pattern'. The client's `delivery.timeout.ms` retries make it worse: each retry holds the same too-big record in memory until the timeout fires.")
            .whyMatters("Large Kafka records are almost always the wrong tool — the right shape for blobs is to store them externally (S3, blob store) and put a reference on the topic. If you genuinely need bigger records, the broker `message.max.bytes` and consumer `fetch.max.bytes` must be raised in lockstep, with awareness of replica replication buffer and disk IO. This rule catches the half-done version of that work.")
            .build());

    public static final RuleId KAFKA_METADATA_MAX_AGE_MS_TOO_LOW = register(builder("KAFKA_METADATA_MAX_AGE_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_METADATA_MAX_AGE_MS_TOO_LOW.md")
            .message("metadata.max.age.ms below 30 s — forces a MetadataRequest every few seconds per client; broker-CPU drain at scale.")
            .tagline("metadata.max.age.ms is the upper bound on cluster-metadata staleness inside the client. Cut it short and every client perpetually re-fetches the same data.")
            .mechanism("Each producer/consumer holds a cached view of cluster metadata (which broker leads which partition, where the controller is). When the cached entry is older than `metadata.max.age.ms` (default 300_000 ms = 5 min), the client issues a MetadataRequest to refresh. The default is already aggressive enough to catch leader moves; lower values do not improve correctness — leader changes also push a stale-metadata error, which triggers an immediate refresh independently of this knob.")
            .impact("With `metadata.max.age.ms=10000`, a fleet of 500 clients hits a single controller broker with one MetadataRequest every ~20 ms in steady state. The controller's request-handler thread spends meaningful CPU answering 'still the same as last time'. The cost is invisible from any single client; it shows up as 'why is the controller broker hot?' in the broker dashboards.")
            .whyMatters("The default 5 minutes is the right value. People shorten it after a leader-move incident, thinking 'we want to react faster' — but the client *already* refreshes on leader-move errors, so this knob only affects how often it polls when nothing has happened. Anything below 30 s is almost certainly a misunderstanding.")
            .build());

    public static final RuleId CONSUMER_FETCH_MAX_WAIT_MS_TOO_HIGH = register(builder("CONSUMER_FETCH_MAX_WAIT_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_FETCH_MAX_WAIT_MS_TOO_HIGH.md")
            .message("fetch.max.wait.ms above 5 s — every empty poll on a quiet topic blocks the consumer for that long.")
            .tagline("fetch.max.wait.ms is how long the broker holds an empty FetchRequest before responding. Stretch it past a few seconds and idle topics turn into latency cliffs.")
            .mechanism("On a FetchRequest, if the broker has less data than `fetch.min.bytes` for the consumer's partitions, it waits up to `fetch.max.wait.ms` (default 500 ms) for more data to arrive before responding. The wait blocks the consumer's poll() until either threshold trips.")
            .impact("With `fetch.max.wait.ms=30000` on a periodically-busy topic, the consumer is idle for up to 30 s after each burst. End-to-end latency on the next record arrival can hit the full wait. Worse, if the consumer's `max.poll.interval.ms` is shorter than `fetch.max.wait.ms`, the consumer can blow its poll deadline and get evicted from the group without ever calling user code.")
            .whyMatters("The default 500 ms is well-tuned for the throughput/latency trade-off. Raising this knob almost always reflects confusion with another setting (`request.timeout.ms`, `session.timeout.ms`). If you legitimately need huge batches, raise `fetch.min.bytes` and accept the default wait — never the other way round.")
            .build());

    public static final RuleId PRODUCER_TRANSACTIONAL_ID_GENERIC = register(builder("PRODUCER_TRANSACTIONAL_ID_GENERIC")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_TRANSACTIONAL_ID_GENERIC.md")
            .message("transactional.id is a generic placeholder (tx, txn, my-tx, test, ...). Two producers with the same id will fence each other.")
            .tagline("transactional.id is the unique identity of a transactional producer across the cluster. Two producers with the same id assume one of them is a zombie and fence the other.")
            .mechanism("Kafka uses `transactional.id` to fence zombie producers across restarts: when a producer calls `initTransactions()`, the broker bumps the producer's epoch and rejects (`PRODUCER_FENCED`) any older producer using the same id. The model assumes the id is *unique per producer instance* — usually a tuple of service-name + partition-shard or service-name + instance-id.")
            .impact("Two unrelated services both shipping with `transactional.id=tx` (or `my-tx`, `transaction`) end up fencing each other on every restart. Each restart bumps the epoch; the other service's next send fails with `ProducerFencedException`, the application restarts, fences the first one back, and the cluster oscillates. Symptoms look like 'random transactional producer crashes' that nobody can correlate.")
            .whyMatters("Pick a service-and-instance qualified value: `payments-fraud-pipeline-shard-3`, `audit-writer-v2-i07`. Treat the id like a database lease — it identifies a single producer instance, not a service. This rule catches the copy-paste-tutorial class of bug, where the placeholder ships to staging unchanged.")
            .build());

    public static final RuleId SECURITY_SSL_PROTOCOL_LEGACY = register(builder("SECURITY_SSL_PROTOCOL_LEGACY")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("security")
            .docPath("security/SECURITY_SSL_PROTOCOL_LEGACY.md")
            .message("ssl.protocol pinned to TLSv1 / TLSv1.1 / SSLv2 / SSLv3 — legacy/broken transport.")
            .tagline("ssl.protocol selects the TLS handshake version. The legacy versions (TLS 1.0, 1.1, SSL 3.0/2.0) are known-broken and have been removed from JDK defaults — pinning them either ships a vulnerable client or fails the handshake entirely.")
            .mechanism("The Kafka client passes `ssl.protocol` to the JSSE SSLContext factory. JDK 8u291+, JDK 11.0.11+ and JDK 17+ disable TLSv1 and TLSv1.1 by default; some JDK builds disable them via `jdk.tls.disabledAlgorithms` in `java.security`. SSLv2/v3 are removed entirely. Explicitly setting `ssl.protocol=TLSv1.1` either bypasses the disabled list (a security incident) or the SSL context refuses to initialise (a runtime failure).")
            .impact("Best case: the application fails to start with `NoSuchAlgorithmException` or `SSLException: No appropriate protocol` — caught in QA but expensive to diagnose. Worst case (older JDK or an unwise `security.properties` override): the client connects with a cipher suite vulnerable to BEAST, POODLE, or the truncation attacks that retired TLS 1.0/1.1 in the first place. Compliance audits (PCI-DSS 3.2.1+, FedRAMP) explicitly forbid both.")
            .whyMatters("There is no reason to pin a TLS version below 1.2 in 2026. If a broker only accepts TLS 1.0 or 1.1, the broker is the bug — upgrade it. Remove this setting entirely; the JDK will negotiate TLS 1.2 (or 1.3 on JDK 11+) as the floor, which is what every modern broker accepts.")
            .build());

    public static final RuleId CONSUMER_EXCLUDE_INTERNAL_TOPICS_FALSE = register(builder("CONSUMER_EXCLUDE_INTERNAL_TOPICS_FALSE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_EXCLUDE_INTERNAL_TOPICS_FALSE.md")
            .message("exclude.internal.topics=false — consumer can subscribe to __consumer_offsets / __transaction_state via regex.")
            .tagline("exclude.internal.topics keeps Kafka's own bookkeeping topics out of regex-based subscriptions. Turning it off opens a regex match to __consumer_offsets and the rest of the internal namespace.")
            .mechanism("When a consumer calls `subscribe(Pattern)` with a regex that would match an internal topic (any topic starting with `__`, e.g. `__consumer_offsets`, `__transaction_state`, `__schema_registry`), the consumer normally excludes them. Setting `exclude.internal.topics=false` removes that filter — the consumer joins those topics like any other.")
            .impact("Reading `__consumer_offsets` directly leaks every consumer group's offset commits across the cluster — group names, topic names, position cursors. Reading `__transaction_state` leaks every transactional producer's in-flight transactions. Beyond the information leak, reading these topics under load adds non-trivial broker load and can interfere with the GroupCoordinator's normal compaction pace.")
            .whyMatters("There is essentially no legitimate reason for an application to set this to `false`. The legitimate consumers of internal topics (the controller, schema registry, MirrorMaker's offset translation) all have dedicated code paths. If this is set in your config, either someone misread the docs while debugging or a debugging hack survived into production.")
            .build());

    public static final RuleId PRODUCER_COMPRESSION_GZIP = register(builder("PRODUCER_COMPRESSION_GZIP")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_COMPRESSION_GZIP.md")
            .message("compression.type=gzip — outdated codec choice; lz4 (default), zstd (best ratio), or snappy (cheapest CPU) dominate the trade-off curve.")
            .tagline("gzip is the slow corner of every modern compression benchmark. Today's three real choices are lz4, zstd, and snappy — gzip survives only in legacy configs.")
            .mechanism("`compression.type` (default `none` on the producer, but Kafka chose `lz4` as the recommended setting since 2.1) controls how the producer compresses each record batch before sending. The codecs available are `none`, `gzip`, `snappy`, `lz4`, and (since 2.1) `zstd`. gzip is the oldest option and the slowest by a wide margin on every benchmark Confluent or Apache has published — typically 2-5x slower CPU per byte than lz4 for the same compression ratio, and worse ratio than zstd at every speed setting.")
            .impact("On a producer at 100 MB/s, gzip can saturate a core on compression alone while lz4 or snappy stay under 20% CPU; zstd matches gzip's ratio at half the CPU. The cost is mostly invisible until traffic grows — you pay it in producer p99 latency (compression is on the send path), in container CPU bills, and in fewer records per producer instance.")
            .whyMatters("Almost every gzip in a Kafka config today is a copy from a 2017-era tutorial. The reasonable replacements are: `lz4` for the throughput-leaning default, `zstd` if you want size and have kafka-clients ≥ 2.1 + broker ≥ 2.1, `snappy` if you want the cheapest CPU. This rule is INFO severity — it's a quality-of-life signal, not a correctness bug.")
            .build());

    public static final RuleId CONSUMER_PARTITION_ASSIGNMENT_LEGACY = register(builder("CONSUMER_PARTITION_ASSIGNMENT_LEGACY")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_PARTITION_ASSIGNMENT_LEGACY.md")
            .message("partition.assignment.strategy pinned to legacy assignor (Range/RoundRobin/Sticky) without CooperativeStickyAssignor — eager rebalance hurts every restart.")
            .tagline("partition.assignment.strategy picks the protocol the group uses to share work. The legacy assignors stop the world for every rebalance; CooperativeStickyAssignor is strictly better since 2.4.")
            .mechanism("On every group event (join, leave, topic-metadata change), the assignor decides which partitions go to which consumer. Legacy assignors (`RangeAssignor`, `RoundRobinAssignor`, `StickyAssignor`) follow the *eager* protocol: every consumer revokes *all* its partitions, the group rebalances, then partitions are re-assigned. During the revoke→assign window, no record is being processed. `CooperativeStickyAssignor` (≥ 2.4) follows the *cooperative* protocol: only the partitions that actually need to move are revoked, the rest keep flowing.")
            .impact("On a 20-consumer group reading from a 200-partition topic, a single restart pauses *every* consumer for the duration of the rebalance under an eager assignor — typically 1-3 seconds, longer if any consumer is slow to finish its current poll. With CooperativeStickyAssignor, 19 of the 20 consumers keep working through the restart and only the moved partitions stall. The throughput difference under deploy/autoscale is large enough to show up as a SLO violation on busy services.")
            .whyMatters("The 3.x default of `RangeAssignor,CooperativeStickyAssignor` allows the cooperative protocol when all members can speak it. Pinning to just `RangeAssignor` (or `RoundRobinAssignor`, `StickyAssignor`) downgrades the whole group to eager rebalance. The fix is to either remove the override (let the default kick in) or to use `org.apache.kafka.clients.consumer.CooperativeStickyAssignor` alone if you can guarantee a homogeneous fleet.")
            .build());

    public static final RuleId PRODUCER_BUFFER_MEMORY_TOO_HIGH = register(builder("PRODUCER_BUFFER_MEMORY_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_BUFFER_MEMORY_TOO_HIGH.md")
            .message("buffer.memory above 256 MiB — producer can pin that much off-heap accumulator memory; almost always a misunderstanding.")
            .tagline("buffer.memory is the cap on the producer accumulator's in-memory queue. Set it to hundreds of megabytes and you have promised the JVM that much memory just for unsent records.")
            .mechanism("`buffer.memory` (default 33_554_432 = 32 MiB) bounds the total bytes the producer can hold for not-yet-sent records across all partitions. Records are queued here from `send()` and drained by the sender thread. When the buffer fills, `send()` blocks for up to `max.block.ms` before throwing `BufferExhaustedException`. The bytes are real heap allocations, used immediately on producer construction (the accumulator pre-allocates segments).")
            .impact("Setting `buffer.memory=536870912` (512 MiB) on a service that ships with `-Xmx768m` is a recipe for OOM. Even if the JVM has the room, half a gigabyte of unsent records means a single-broker outage can stack up 5-10 seconds of traffic in producer RAM before the back-pressure trips — multiplied by every producer instance. When the broker comes back, all those records get re-sent at once, causing a thundering-herd on the broker's request queue.")
            .whyMatters("The default 32 MiB is the right starting point for any service that is not a high-throughput log-collector. Raising this knob is almost always a mistake masquerading as a fix for an unrelated symptom (slow sends, BufferExhaustedException). The real fix is usually one of: increase `max.in.flight.requests.per.connection`, tune `batch.size`, or add another producer instance — not enlarge the queue.")
            .build());

    public static final RuleId SECURITY_SASL_MECHANISM_PLAIN = register(builder("SECURITY_SASL_MECHANISM_PLAIN")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("security")
            .docPath("security/SECURITY_SASL_MECHANISM_PLAIN.md")
            .message("sasl.mechanism=PLAIN — sends username/password in cleartext during the SASL handshake; only safe over TLS.")
            .tagline("PLAIN is the SASL mechanism that ships the password as-is. Without TLS underneath, anyone with a packet capture has the credentials.")
            .mechanism("`sasl.mechanism=PLAIN` (RFC 4616) wraps the username and password into a single SASL token, base64-encoded, sent on the client's first authentication frame. There is no challenge, no salt, no hash — the broker receives the password and compares it. With `security.protocol=SASL_SSL` this is all inside a TLS tunnel and is fine. With `security.protocol=SASL_PLAINTEXT` (or no TLS at all), the password is on the wire in cleartext.")
            .impact("On a cluster that uses SASL_PLAINTEXT (already flagged separately, but combined with PLAIN it is acute), a single tcpdump from a side-car, a Kubernetes-network plugin in promiscuous mode, or a man-in-the-middle on the broker DNS yields the service account password. The credential is then valid for every other service using the same account — and PLAIN-with-Kafka is usually paired with a single shared service-account pattern, so the blast radius is wide.")
            .whyMatters("Even when TLS is correctly configured, PLAIN puts the broker in possession of every client's plaintext password — a key-management headache that the SCRAM family removes. The right shape is `SCRAM-SHA-256` or `SCRAM-SHA-512`: the broker only stores a derived value, the handshake uses a challenge, and a compromised broker disk does not leak the original passwords. Reserve PLAIN for cases where SCRAM is unavailable on the broker — and then only over SASL_SSL.")
            .build());

    public static final RuleId STREAMS_TOPOLOGY_OPTIMIZATION_NONE = register(builder("STREAMS_TOPOLOGY_OPTIMIZATION_NONE")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_TOPOLOGY_OPTIMIZATION_NONE.md")
            .message("topology.optimization=none — extra repartition / source-KTable changelog topics that 'all' would eliminate.")
            .tagline("Streams' topology optimizer fuses redundant repartitions and lets source KTables reuse the input topic as a changelog. Disabling it ships more internal topics than necessary.")
            .mechanism("`topology.optimization` (default `none` for backward compatibility, but the modern recommendation is `all` or `reuse.ktable.source.topic`) tells Streams to rewrite the topology before runtime. Two optimisations matter most: (1) reusing the input topic as a KTable's changelog instead of creating a `-changelog` internal topic, and (2) merging consecutive `selectKey()→groupByKey()` operations into a single repartition. Both reductions are correctness-preserving and broker-load-reducing.")
            .impact("With `topology.optimization=none`, a topology that uses two consecutive `groupBy()` operations creates two internal repartition topics; with `all`, it creates one. A KTable read from a compacted input topic carries a duplicate `-changelog` topic — twice the storage, twice the replication bandwidth — under `none`. On a heavy topology, the optimizer cuts internal topic count by 30-50%, with proportional savings on broker disk, network, and the GroupCoordinator's commit traffic.")
            .whyMatters("The reason the default is `none` is that switching it on can re-shape an existing application's internal topics — which is a breaking change for live state. For *new* applications there is no reason to leave it off; for *existing* applications the documented migration path lets you switch deliberately. This rule is INFO severity because it is improvement headroom, not a correctness bug.")
            .build());

    public static final RuleId STREAMS_REPLICATION_FACTOR_TWO = register(builder("STREAMS_REPLICATION_FACTOR_TWO")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_REPLICATION_FACTOR_TWO.md")
            .message("Streams replication.factor=2 — losing one broker leaves only one replica, no fault-tolerance budget for a second loss.")
            .tagline("Streams' internal topics carry the application state. With replication.factor=2, the first broker loss leaves you single-replicated; the second loss is data loss.")
            .mechanism("`replication.factor` (Streams-side, applied to changelog and repartition topics) controls how many in-sync replicas each internal topic gets. A value of 2 means 'one leader + one follower'. When the follower is down (rolling restart, hardware fault), `min.insync.replicas≥2` blocks all writes; when the leader is also down (rolling restart of a second broker, or rack failure), there is no second copy of the changelog.")
            .impact("On a routine rolling restart of a 3-node cluster, each broker is unavailable for a few minutes at a time. With `replication.factor=2`, every changelog has a window where only one ISR exists — producers stall (acks=all needs `min.insync.replicas` met), state-store updates pause, and Streams tasks may evict. Worse, an unexpected fault during the restart window (a coincident hardware failure) means full data loss for the topics that happened to have both replicas on the affected pair.")
            .whyMatters("The standard production recommendation is `replication.factor=3` with `min.insync.replicas=2`. That gives you one tolerated failure with continued availability. Streams's default of 1 is for local testing; 2 is sometimes set as a 'compromise' that costs almost as much as 3 (still two cross-broker replicas to write) but gives no real fault-tolerance. The right answer in production is 3.")
            .build());

    public static final RuleId SECURITY_SSL_KEYSTORE_TYPE_JKS = register(builder("SECURITY_SSL_KEYSTORE_TYPE_JKS")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("security")
            .docPath("security/SECURITY_SSL_KEYSTORE_TYPE_JKS.md")
            .message("ssl.keystore.type=JKS — proprietary keystore format; PKCS12 is the modern, cross-tool standard.")
            .tagline("JKS is the Java-specific keystore format from the 1990s. PKCS12 is the cross-platform, modern alternative — JDK 9+ uses it as the default and the tooling around it is much better.")
            .mechanism("`ssl.keystore.type` selects the JCA KeyStore SPI implementation that the Kafka client uses to load `ssl.keystore.location`. `JKS` is the proprietary Java format; `PKCS12` is the cross-tool RFC-7292 format that OpenSSL, Windows, macOS Keychain, and every modern certificate tool produce natively. JDK 9+ defaults to `PKCS12`; the `JKS` value is supported but discouraged. Apache Kafka 3.x will keep accepting JKS but the JSSE deprecation notes flag it.")
            .impact("This is not a runtime bug — JKS still works — but it is a maintainability tax. Rotating certificates requires `keytool` round-trips that the rest of the certificate-management toolchain (cert-manager, OpenShift's serving certs, AWS ACM exports, Vault's PKI engine) cannot produce directly. Every cert rotation becomes a manual JKS conversion step, which is exactly where production teams introduce bugs.")
            .whyMatters("Switching to PKCS12 is a one-line config change plus a `keytool -importkeystore -srcstoretype JKS -deststoretype PKCS12` conversion. Once done, the certificate-management pipeline is straight-through. This rule is INFO severity: it's improvement headroom, not a vulnerability, and a JKS keystore is fine until the next rotation.")
            .build());

    public static final RuleId SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_GENERIC = register(builder("SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_GENERIC")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_PRODUCER_TRANSACTION_ID_PREFIX_GENERIC.md")
            .message("spring.kafka.producer.transaction-id-prefix is a generic placeholder — two apps with the same prefix will fence each other across restarts.")
            .tagline("Spring's transaction-id-prefix becomes the per-producer transactional.id at runtime. A generic value collides with whoever else also forgot to set theirs.")
            .mechanism("When `spring.kafka.producer.transaction-id-prefix` is set, Spring's DefaultKafkaProducerFactory builds the underlying producer's `transactional.id` as `<prefix><suffix>` — the suffix is the listener-container's group-id and a sequence number, or a synthesised id for non-listener producers. The full id is what the broker uses for the fencing/epoch protocol. If the prefix collides between two apps, the full ids end up colliding too — and the broker enforces 'one live producer per transactional.id' by fencing all but the latest.")
            .impact("Two services both shipping with `spring.kafka.producer.transaction-id-prefix=tx-` end up fencing each other: every Spring app restart bumps the producer epoch, the other service's next send fails with `ProducerFencedException`, the Spring listener container restarts, fences the first one back, and the cluster oscillates. Symptoms look like 'random ProducerFencedExceptions in the listener container logs that nobody can correlate'.")
            .whyMatters("Pick a service-and-environment qualified prefix: `payments-fraud-pipeline-staging-`. Treat the prefix the way you treat a database lease — it identifies a single application instance group, not a service. This rule catches the copy-paste class of bug, where the placeholder ships unchanged.")
            .build());

    public static final RuleId CONSUMER_AUTO_OFFSET_RESET_NONE_EXPLICIT = register(builder("CONSUMER_AUTO_OFFSET_RESET_NONE_EXPLICIT")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_AUTO_OFFSET_RESET_NONE_EXPLICIT.md")
            .message("auto.offset.reset=none — consumer throws NoOffsetForPartitionException at startup if no committed offset exists.")
            .tagline("auto.offset.reset=none means 'if there is no committed offset, refuse to start'. Sometimes that is what you want; more often it ships a service that won't start on a fresh group.")
            .mechanism("When a consumer joins a group for a partition that has no committed offset yet (new group, expired group, never-committed-offset), `auto.offset.reset` decides whether to start at `earliest`, `latest`, or `none`. `none` raises `NoOffsetForPartitionException` and the consumer refuses to start. The exception bubbles to `poll()`, the listener container restarts, and the app stays in a crash loop until either offsets are seeded manually or the setting is changed.")
            .impact("On a service whose first deployment uses a fresh `group.id`, `auto.offset.reset=none` makes the deploy fail in a way that looks like a Kafka outage — the service is up, the broker is up, but the consumer cannot start. In incident reviews this confuses people: the error message names the partition, not the policy. The intended use case (operator demands explicit offset seeding before consumption starts) is real but rare.")
            .whyMatters("This is INFO severity because `none` can be the right choice for offset-sensitive pipelines (you'd rather fail loudly than silently skip or replay). But it should be a deliberate choice with operational runbook attached, not a copy-paste survival from a tutorial. The rule prompts a review; it does not assert wrongness.")
            .build());

    public static final RuleId KAFKA_CLIENT_ID_GENERIC = register(builder("KAFKA_CLIENT_ID_GENERIC")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_CLIENT_ID_GENERIC.md")
            .message("client.id is a generic placeholder — broker logs and metrics cannot tell instances apart.")
            .tagline("client.id is the label brokers attach to every request from this client. A generic value turns every broker dashboard and audit log into anonymous noise.")
            .mechanism("`client.id` is included in every produce/fetch/metadata request and is the dimension brokers use to label client metrics (`kafka.server:type=BrokerTopicMetrics,name=*` per-client-id), JMX rate limits, ACL audit logs, and quota tracking. The default is a generated `producer-<n>` / `consumer-<n>` — already not great for ops. A placeholder like `client`, `my-client`, `test` is strictly worse: every service in the fleet shows up as the same identity in broker metrics, and quota limits get applied unfairly because two unrelated apps share an id.")
            .impact("On a multi-tenant cluster, an unattributed producer flooding the cluster cannot be pinned by `client.id` alone — the broker's metrics dashboard shows a hot client called 'producer' with no further information. Quotas configured per-client-id (the common pattern) apply collectively across every service that shares the placeholder. Audit logs (`org.apache.kafka.server.authorizer.AuthorizerAuditLogPattern`) become useless for forensics.")
            .whyMatters("Pick a service-and-instance qualified value: `payments-fraud-screening-i07`, `audit-writer-v3-c0`. The cost is essentially zero (one config line), and the value at debug-time is enormous. INFO severity because it is operational hygiene rather than a correctness bug.")
            .build());

    public static final RuleId STREAMS_MAX_TASK_IDLE_MS_HIGH = register(builder("STREAMS_MAX_TASK_IDLE_MS_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_MAX_TASK_IDLE_MS_HIGH.md")
            .message("max.task.idle.ms is set very high — the topology will stall waiting on idle partitions, inflating event-time latency.")
            .tagline("`max.task.idle.ms` is how long a Streams task is willing to wait for records on an empty partition before declaring itself caught up and advancing stream-time. Crank it up and the whole topology pauses every time one input goes quiet.")
            .mechanism("Streams uses `max.task.idle.ms` (default 0 in 3.x, was -1 in older versions) to decide what to do when a task has data on some partitions and none on others. With idle.ms=0, the task processes whatever is available and advances stream-time using the data it has — at the cost of potentially out-of-order joins when a slow partition catches up. With idle.ms > 0, the task buffers ready records and waits up to that many ms for the empty partition to deliver data; if nothing arrives, it gives up and proceeds. The intent is to give slower upstream producers a chance to deliver their record before a co-partitioned join uses a stale value.")
            .impact("With `max.task.idle.ms=60000` (60 s), a join where one input topic goes briefly idle introduces a fixed 60-second stall before any downstream record emits. In a low-traffic dev cluster this is invisible; in production where one partition is genuinely quieter (cold key, geographic skew), every task hits the timeout every poll. End-to-end latency for a streaming app that should be sub-second becomes a minute. The metric `task-idle-ratio` will sit near 1.0 — the topology is almost always idling.")
            .whyMatters("Most teams that set this knob do so because they read about out-of-order joins and overcorrected. The 3.x default (0) is the right starting point: process what you have, accept some out-of-order in exchange for low latency, and use windowed joins with grace if event-time correctness matters. Values above 10 seconds are almost always a misconfiguration. If you genuinely need synchronized cross-partition ordering, use a windowed join with grace and revisit your partitioning, not `max.task.idle.ms`.")
            .build());

    public static final RuleId QK_INCOMING_AUTO_OFFSET_RESET_NONE = register(builder("QK_INCOMING_AUTO_OFFSET_RESET_NONE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_INCOMING_AUTO_OFFSET_RESET_NONE.md")
            .message("mp.messaging.incoming.{channel}.auto.offset.reset=none — fresh consumer group fails to start instead of resetting.")
            .tagline("`auto.offset.reset=none` makes the consumer throw `NoOffsetForPartitionException` when there is no committed offset, rather than picking `earliest` or `latest`. For a brand-new Quarkus deployment this means the pod crashloops on startup.")
            .mechanism("When a Kafka consumer subscribes to a partition it has never committed an offset for, it must decide where to begin. `auto.offset.reset` is that policy: `earliest` rewinds to the start of the log, `latest` jumps to the tail, and `none` refuses — the broker raises `NoOffsetForPartitionException` and the consumer thread dies. SmallRye Reactive Messaging surfaces this as a failed channel; in a Quarkus app with health-readiness-enabled (the default), the pod stays Unready and the deployment never rolls.")
            .impact("On the first deploy of a new consumer group (new app, new environment, after a group rename, after a manual `kafka-consumer-groups --delete`), the channel fails to start. Quarkus' health endpoint reports the channel down; k8s holds traffic; the operator spends the next hour debugging why a 'clean' deployment cannot consume anything. The intended use case for `none` — strictly forbidding accidental data skip — is real but rare, and almost always wants explicit offset management (`seek`) rather than this knob.")
            .whyMatters("Pick `earliest` for batch-style pipelines that need to process the full history, or `latest` for live-tail consumers (metrics, real-time dashboards). Reach for `none` only when you have a custom offset-management layer that guarantees an offset is committed before the consumer ever starts. If you are setting it because someone said it was 'safer', they were thinking of `latest`.")
            .build());

    public static final RuleId KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_LOW = register(builder("KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_LOW.md")
            .message("connections.max.idle.ms is set very low — the client tears down and re-handshakes broker connections on a tight cycle.")
            .tagline("`connections.max.idle.ms` is how long a TCP connection to a broker can sit idle before the client itself closes it. Default is 9 minutes; lowering it to seconds means every brief lull rebuilds the connection from scratch — TCP, TLS, SASL all over again.")
            .mechanism("The Kafka client tracks per-broker connection idle time. Once the timer crosses `connections.max.idle.ms` (default 540000 ms = 9 min), the client closes the socket on its side; the next produce/fetch transparently re-establishes it. The reconnect cost is non-trivial: TCP handshake, TLS handshake (4-5 round trips for TLS 1.2, 2-3 for TLS 1.3), SASL auth exchange (1-2 round trips for PLAIN, more for SCRAM/Kerberos), and finally a fresh metadata refresh. On a SASL_SSL cluster that is 6-10 round trips of unproductive latency every time the timer fires.")
            .impact("With `connections.max.idle.ms=5000` on a low-traffic producer (e.g. a job-completion event channel), every burst of 1-2 records pays a full reconnect penalty: client traces show 50-200 ms of pure setup latency before the first send. The broker side sees a flood of `ChannelClose` and `Authentication` log lines — security/SRE dashboards light up with fake 'auth churn' alerts. Connection pools never warm up because every connection is torn down before TCP slow-start completes.")
            .whyMatters("Almost every team that sets this low is mimicking an HTTP `keep-alive` setting they saw elsewhere. Kafka is not HTTP — the broker side has its own idle-killer (`connections.max.idle.ms` on the broker, default 10 min) and the client default already matches it. Either remove the override entirely, or set it to a value greater than 30 s. Sub-second values are always wrong.")
            .build());

    public static final RuleId CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_HIGH = register(builder("CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_HIGH.md")
            .message("max.partition.fetch.bytes is set very high — the consumer commits to allocating that much memory per partition per fetch.")
            .tagline("`max.partition.fetch.bytes` is the per-partition slice the consumer reserves on every FetchRequest. With 50 assigned partitions and the knob at 50 MiB, a single poll can pull 2.5 GiB into heap before any user code runs.")
            .mechanism("On a FetchRequest, the consumer tells the broker the maximum bytes it is willing to receive **per partition** (`max.partition.fetch.bytes`, default 1 MiB) and the maximum total across all partitions (`fetch.max.bytes`, default 50 MiB). The broker fills each assigned partition up to the per-partition cap, then truncates to the total cap. Crucially, the consumer pre-allocates per-partition buffers in network code paths; the per-partition cap also controls the maximum single record size the consumer can decode (a record larger than this cap will block the partition forever — `RecordTooLargeException` on the consumer side).")
            .impact("With `max.partition.fetch.bytes=104857600` (100 MiB) and a consumer assigned 32 partitions during a rebalance peak, a single poll can ask the broker for up to 3.2 GiB. Heap pressure spikes, GC pauses lengthen, the consumer misses its `max.poll.interval.ms` deadline and the group rebalances — which assigns more partitions to surviving members, amplifying the problem. Conversely, the same knob set higher than the actual broker-side `message.max.bytes` is a no-op that creates the illusion of headroom.")
            .whyMatters("This knob is almost always raised to 'work around RecordTooLargeException' without realising it then becomes a memory-and-rebalance hazard. The right fix for that exception is to lower producer-side payload size, raise broker `message.max.bytes`, and align all three. Setting `max.partition.fetch.bytes` above 16 MiB is a strong signal of cargo-cult tuning.")
            .build());

    public static final RuleId STREAMS_NUM_STREAM_THREADS_ONE = register(builder("STREAMS_NUM_STREAM_THREADS_ONE")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_NUM_STREAM_THREADS_ONE.md")
            .message("num.stream.threads=1 — the topology runs on a single thread; no intra-instance parallelism.")
            .tagline("Streams runs `num.stream.threads` StreamThreads inside each application instance, each thread owning a slice of the assigned partitions. With 1 thread, even a multi-CPU pod processes one partition at a time and any blocking I/O stalls the whole topology.")
            .mechanism("A Streams instance allocates `num.stream.threads` (default 1) StreamThread workers. Tasks (groups of partitions for the same sub-topology) are spread across these threads; each thread polls, processes, and produces sequentially. Adding threads is the cheapest way to scale a Streams instance vertically — up to the lesser of (a) the number of input partitions, and (b) the number of available CPU cores. A single-thread instance running on a 4-vCPU pod leaves 75% of the compute idle.")
            .impact("In production it usually shows up as 'why is my Streams pod at 25% CPU and 3 s end-to-end latency'. Lag accumulates linearly with traffic because one thread cannot stay ahead of the producers; the operator scales the deployment horizontally instead of vertically, paying for empty pods. A single slow `transform()` (DB lookup, external HTTP call) blocks all assigned partitions in that thread.")
            .whyMatters("The right value is roughly min(input-partitions / instances, cpu-cores). Leaving the default 1 is usually accidental — someone copy-pasted a dev config into prod. INFO severity because there are cases where 1 is intentional (resource-constrained dev/test instance, single-partition pipeline), but flagging it forces a conscious choice. MEDIUM confidence because the right value is workload-dependent — the linter cannot know cpu-cores at build time.")
            .build());

    public static final RuleId STREAMS_PRODUCTION_EXCEPTION_HANDLER_ALWAYS_CONTINUE = register(builder("STREAMS_PRODUCTION_EXCEPTION_HANDLER_ALWAYS_CONTINUE")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_PRODUCTION_EXCEPTION_HANDLER_ALWAYS_CONTINUE.md")
            .message("default.production.exception.handler is AlwaysContinueProductionExceptionHandler — every failed produce is silently dropped and the topology keeps running.")
            .tagline("Streams' production exception handler decides what happens when the embedded producer cannot deliver a record (RecordTooLarge, UnknownTopicOrPartition, network errors with retries exhausted). `AlwaysContinueProductionExceptionHandler` answers 'drop it and move on' — silently — for every kind of failure.")
            .mechanism("When a Streams topology produces to a downstream topic or to an internal repartition/changelog topic, the embedded `RecordCollector` invokes `ProductionExceptionHandler.handle(...)` on every async send failure. The handler returns either `FAIL` (default — re-throw and crash the StreamThread, surfacing the problem) or `CONTINUE` (drop the record and proceed). `AlwaysContinueProductionExceptionHandler` returns `CONTINUE` for every exception, regardless of cause. The topology keeps processing the next record as if nothing happened — but the downstream topic is now missing data, and any changelog-backed state store has an inconsistent on-disk snapshot vs broker state.")
            .impact("A `RecordTooLarge` against your changelog topic silently corrupts the state store: the in-memory KTable has the value, the changelog topic does not, and on the next restart the store reloads from the changelog and the value vanishes. A produce error against an output topic produces gaps that are invisible at runtime — no warning, no metric increment beyond `dropped-records-rate`, and the downstream consumer just sees fewer records. Data loss with no audit trail.")
            .whyMatters("Use the default (`DefaultProductionExceptionHandler`, which fails fast) or write a handler that selectively retries/quarantines specific exception classes (e.g. continue on `RecordTooLargeException` after sending to a DLQ, fail on everything else). 'Always continue' is the worst possible answer because it treats every kind of broker failure — transient, configuration, data-shape — identically as 'drop it'. If you reach for this handler because the topology keeps crashing, the crash is the symptom, not the disease.")
            .build());

    public static final RuleId SPRING_BOOT_JSON_TRUSTED_PACKAGES_WILDCARD = register(builder("SPRING_BOOT_JSON_TRUSTED_PACKAGES_WILDCARD")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_JSON_TRUSTED_PACKAGES_WILDCARD.md")
            .message("spring.json.trusted.packages=* — Spring Kafka's JsonDeserializer will instantiate any FQCN sent in the __TypeId__ header. Remote class loading vector.")
            .tagline("`spring.json.trusted.packages` is an allow-list of package prefixes that Spring's `JsonDeserializer` is allowed to instantiate based on the producer-supplied `__TypeId__` header. Setting it to `*` bypasses the allow-list and trusts the wire.")
            .mechanism("Spring Kafka's `JsonDeserializer` resolves the target type for each record by reading the `__TypeId__` (or configured) header — typically a FQCN like `com.example.OrderEvent`. Before instantiating, it checks `spring.json.trusted.packages`: only packages whose prefix appears in this list are loaded. The default in modern Spring Boot is `*` for the trust list ONLY if you opt in; the secure pattern is an explicit allow-list like `com.example.events,com.example.dto`. Configuring `*` instructs Spring to skip the allow-list entirely — any FQCN the producer puts in the header is loaded via the JVM classloader.")
            .impact("A producer (or anyone who can write to the topic) can put `__TypeId__: org.example.AttackerBean` in the headers; on the next consumer poll, Spring loads that class. If the class has a no-arg constructor with side effects (network call, file write, command execution), the side effect runs in your consumer JVM. Variations of this pattern have CVEs filed against Jackson polymorphic typing (CVE-2017-7525 family) and were exploited via Spring Kafka.")
            .whyMatters("Use an explicit allow-list scoped to your own packages: `spring.json.trusted.packages=com.acme.orders,com.acme.shared`. The wildcard exists mostly for migration convenience during framework upgrades and should never reach production. ERROR severity because it is an RCE-class issue, not a tuning miss.")
            .build());

    public static final RuleId STREAMS_NUM_STREAM_THREADS_TOO_HIGH = register(builder("STREAMS_NUM_STREAM_THREADS_TOO_HIGH")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_NUM_STREAM_THREADS_TOO_HIGH.md")
            .message("num.stream.threads set very high — threads beyond input-partitions/instances sit idle while still claiming heap and metric overhead.")
            .tagline("Streams threads are a one-to-one map onto assignable tasks. Setting `num.stream.threads` higher than the number of tasks the instance can ever own means most threads spend the entire process lifetime parked.")
            .mechanism("Streams' rebalance protocol assigns tasks (one task per sub-topology per partition) to the StreamThreads available in the consumer group. A topology consuming a 12-partition topic has 12 tasks; if you run 2 instances each with `num.stream.threads=64`, the assignor hands each instance 6 tasks at most — 58 threads on each pod simply idle. Each idle StreamThread still allocates its consumer/producer buffers, registers JMX MBeans, and shows up in `kafka.streams:type=stream-thread-metrics` with zero traffic.")
            .impact("Memory floor and metric cardinality scale with `num.stream.threads`, not with usage. On a 4 vCPU pod, going from `num.stream.threads=4` to `num.stream.threads=128` adds ~2 GB of heap commitment for accumulator buffers and ~30k unused JMX metric points — without adding any throughput because the input topic does not have enough partitions to feed them. Pods OOM at startup or during heap warm-up for no benefit.")
            .whyMatters("The right value is roughly min(input-partitions / instances, cpu-cores). Setting `num.stream.threads` above 64 is almost always cargo-cult tuning ('more threads = more speed'). INFO severity because high values are *sometimes* correct (very-high-partition pipelines on large CPU pods); MEDIUM confidence because the linter cannot know your partition count at build time.")
            .build());

    public static final RuleId SPRING_BOOT_PRODUCER_LINGER_MS_ZERO = register(builder("SPRING_BOOT_PRODUCER_LINGER_MS_ZERO")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_PRODUCER_LINGER_MS_ZERO.md")
            .message("spring.kafka.producer.properties.linger.ms=0 — explicit no-batching defeats the producer accumulator.")
            .tagline("`linger.ms=0` flushes every record as its own ProduceRequest. Set this way from Spring Boot's properties fall-through means the user actively chose 'no batching', usually based on a misunderstanding of latency vs throughput.")
            .mechanism("The Kafka producer accumulator holds outgoing records per partition until either `batch.size` bytes are queued or `linger.ms` elapses, then ships a single ProduceRequest with the whole batch. The kafka-clients default of `linger.ms=0` means 'flush immediately once any record is queued', which is the right answer for a strict request/response synchronous client — but the wrong answer for any bulk producer. Spring Boot autoconfiguration forwards `spring.kafka.producer.properties.linger.ms` verbatim. Writing `=0` explicitly in the YAML is the same as 'I want no batching' — but `=0` is also the default if you omit the key, so any explicit `=0` is a strong signal of intent: the team chose this on purpose, almost always for the wrong reason.")
            .impact("Under steady traffic of 1k records/s on a single partition, `linger.ms=0` produces ~1000 single-record ProduceRequests/s instead of ~30 batched requests of 30 records each. Broker request rate metric spikes; producer's `record-send-rate` plateaus because each thread is blocked on more round trips; compression goes from ~3x to ~1.2x because per-batch dictionary cannot warm up. Net cost is mid-double-digit percent throughput, 2-4x broker CPU per record, and a lot of TCP noise on the wire.")
            .whyMatters("If the application genuinely needs sub-ms latency for produce (rare; usually the round-trip-to-broker latency dominates anyway), `linger.ms=5` is a far better starting point — it still batches whatever queues up within 5 ms and barely budges p99 latency. `0` is almost always a copy-paste from a stale 'low-latency' tutorial.")
            .build());

    public static final RuleId KAFKA_BOOTSTRAP_SERVERS_LOCALHOST = register(builder("KAFKA_BOOTSTRAP_SERVERS_LOCALHOST")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_BOOTSTRAP_SERVERS_LOCALHOST.md")
            .message("bootstrap.servers points at localhost — production deployment will connect to itself instead of the broker cluster.")
            .tagline("`bootstrap.servers` is the initial broker list the client contacts to discover the cluster. A hard-coded localhost (or 127.0.0.1 or 0.0.0.0) means the pod will try to reach a broker on its own loopback interface — which only ever works on a developer laptop.")
            .mechanism("`bootstrap.servers` accepts one or more `host:port` pairs. The client opens a TCP connection to the first responsive entry, sends a MetadataRequest, and receives the full broker list — at which point `bootstrap.servers` is only used again on reconnect. Setting it to `localhost:9092` is fine in `application-local.properties` for `mvn quarkus:dev` or `spring-boot:run`; it is wrong everywhere else. The reason these literals get committed is that someone followed a 'Hello Kafka' tutorial, copy-pasted the snippet into the production configuration class, and never came back to externalize it.")
            .impact("On the first prod deploy the application starts cleanly (KafkaProducer construction is async, no immediate failure) but every send times out 60 seconds later with `org.apache.kafka.common.errors.TimeoutException: Topic X not present in metadata after 60000 ms`. The dashboards show 100% produce errors, the broker dashboard shows zero connections from this client (it never tried), and the on-call engineer goes hunting for a broker that does not exist. With 0.0.0.0 the failure mode is worse: the OS may route the connection to the wrong interface and the client appears to connect, then fails authentication or metadata exchange with a less helpful error.")
            .whyMatters("Externalize as `${KAFKA_BOOTSTRAP_SERVERS}` or read from an injected `@Value`. A localhost literal in a class file or a non-profile-suffixed properties file is always wrong — there is no production environment in which it is the correct answer. The detector treats localhost, 127.0.0.1, and 0.0.0.0 identically.")
            .build());

    public static final RuleId QK_BOOTSTRAP_SERVERS_LOCALHOST = register(builder("QK_BOOTSTRAP_SERVERS_LOCALHOST")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_BOOTSTRAP_SERVERS_LOCALHOST.md")
            .message("kafka.bootstrap.servers in application.properties points at localhost — production will not be able to reach a broker.")
            .tagline("Quarkus' SmallRye Kafka connector reads `kafka.bootstrap.servers` at startup. A literal localhost value shipped to production means the pod tries to connect to itself.")
            .mechanism("In a Quarkus application, `kafka.bootstrap.servers` (or the per-channel `mp.messaging.{direction}.{channel}.bootstrap.servers`) is the connector's initial cluster contact point. The connector hands this value to kafka-clients verbatim. If left as `localhost:9092`, every channel — incoming and outgoing — tries to reach `localhost:9092` from inside the pod, which is exactly nothing.")
            .impact("In a containerized prod deploy the channels fail health checks and the pod stays Unready, so traffic never flows. In a `quarkus:dev` setup the value works because DevServices spawns a Kafka container on localhost — and that hides the bug right until staging or prod. The fix is one line (`%prod.kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS}`) but only after the bug has shipped.")
            .whyMatters("Externalize via `${KAFKA_BOOTSTRAP_SERVERS}` or profile-conditional overrides (`%prod.kafka.bootstrap.servers=...`). A localhost literal belongs only in profile-prefixed entries (`%dev.kafka.bootstrap.servers=localhost:9092`), and even then it should be obviously a dev-only convenience.")
            .build());

    public static final RuleId STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_WALL_CLOCK = register(builder("STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_WALL_CLOCK")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_WALL_CLOCK.md")
            .message("default.timestamp.extractor=WallclockTimestampExtractor — windowed/joined aggregations use ingestion time, not event time. Replays, retries and out-of-order arrivals all silently bucket into the wrong window.")
            .tagline("Streams uses the timestamp extractor to assign every record a stream-time value. The default (`FailOnInvalidTimestamp`) uses the record's broker-stamped event time; `WallclockTimestampExtractor` ignores it and substitutes `System.currentTimeMillis()` on the consuming machine.")
            .mechanism("Windowed operations (`groupByKey().windowedBy(...).aggregate()`), joins (`KStream.join(KStream, ...)` with a JoinWindows), and any time-based logic all depend on the StreamTime advancing in lockstep with each record's `timestamp`. The default extractor pulls `ConsumerRecord.timestamp()` (set by the producer or by the broker depending on `message.timestamp.type`). Switching to `WallclockTimestampExtractor` replaces that with the consumer's local wall clock — so a record produced 2 hours ago at 10:00, replayed today at noon, arrives at the topology with timestamp 'now', bucketing it into today's noon window instead of the original 10:00 window.")
            .impact("Replays after an outage (or after a consumer group reset) silently corrupt aggregated results: the 10:00–11:00 window from yesterday is now empty in storage but full of fresh records labelled with today's time. Joins between two streams produce 'late' matches that should have happened hours ago. Worst of all, the topology emits no warnings — the metrics dashboards keep showing healthy throughput while the data is wrong.")
            .whyMatters("The only legitimate uses of `WallclockTimestampExtractor` are (a) a strict ingestion-time pipeline where event time genuinely does not matter (very rare in practice), or (b) inside a `KafkaStreams` test fixture. Setting it as `default.timestamp.extractor` in production is almost always a copy-paste from a tutorial that wanted to demo time semantics without setting up event-time producers. If you need to handle records with invalid timestamps, use `LogAndSkipOnInvalidTimestamp` or a custom extractor — not wall clock.")
            .build());

    public static final RuleId SPRING_BOOT_TEMPLATE_OBSERVATION_DISABLED = register(builder("SPRING_BOOT_TEMPLATE_OBSERVATION_DISABLED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_TEMPLATE_OBSERVATION_DISABLED.md")
            .message("spring.kafka.template.observation-enabled=false — disables Micrometer observation on KafkaTemplate; outgoing produces drop out of distributed traces.")
            .tagline("`spring.kafka.template.observation-enabled` turns Micrometer Observation API on for every KafkaTemplate.send() in Spring Boot 3.x. Setting it to false strips W3C `traceparent` propagation and removes the producer hop from your trace graph.")
            .mechanism("Spring Boot 3.0+ wires KafkaTemplate to the Micrometer Observation API when `spring.kafka.template.observation-enabled=true` (the framework default in 3.2+). The template wraps each send in an `Observation`, which produces (a) a Brave/OpenTelemetry span around the producer call, (b) a `traceparent` Kafka header so downstream consumers can stitch the trace, and (c) a `spring.kafka.template` meter for produce timing/error rate. Setting the flag to false skips all three: no span, no header, no meter.")
            .impact("Distributed traces show the upstream HTTP request, then a gap, then whatever consumer eventually picks up the record. SREs lose the ability to attribute end-to-end latency. The downstream consumer's `traceparent`-based observation also goes blank — the trace ends at the producer boundary even on the consumer side, because the propagation header is missing. Bug triage that depends on 'follow the trace' becomes 'reproduce locally with debug logging'.")
            .whyMatters("Almost every team that explicitly sets this to false is doing so as a 'workaround' for a noisy metric or a startup-time concern that no longer applies in current Spring Boot versions. The defaults are correct. If a specific span is too noisy, configure the sampler; do not disable observation wholesale.")
            .build());

    public static final RuleId SPRING_BOOT_LISTENER_OBSERVATION_DISABLED = register(builder("SPRING_BOOT_LISTENER_OBSERVATION_DISABLED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_LISTENER_OBSERVATION_DISABLED.md")
            .message("spring.kafka.listener.observation-enabled=false — disables Micrometer observation on @KafkaListener; consumer-side traces and metrics go dark.")
            .tagline("`spring.kafka.listener.observation-enabled` controls whether the MessageListenerContainer wraps each onMessage callback in a Micrometer Observation. Off means no spans, no `traceparent` consumption, no listener-error metrics.")
            .mechanism("In Spring Boot 3.x, the listener container picks up `observation-enabled=true` and wraps every record (or batch) invocation in an `Observation.start()/stop()` pair. This (a) extracts the `traceparent` header from the incoming ConsumerRecord and creates a child span on the consumer side, (b) emits `spring.kafka.listener` timing metrics tagged with topic/partition/exception class, and (c) propagates the trace context onto any KafkaTemplate.send() called from inside the listener (so produce-after-consume in a streaming pattern stays in one trace). Setting it to false skips every step.")
            .impact("On the consumer side, every record looks unattributed: the trace stops at the producer hop and restarts as a brand-new root span on each consumer poll. Error budgets attributed to specific listeners cannot be calculated. Long tail latency dashboards show only the network-side polling, never the user-code work done inside the listener method.")
            .whyMatters("The default in Spring Boot 3.2+ is true; setting it false is almost always copy-pasted from an older Boot 2.x baseline or from a misguided 'reduce overhead' refactor. The observation cost is microseconds per record; the value at trouble-shoot time is enormous.")
            .build());

    public static final RuleId SPRING_BOOT_PRODUCER_BATCH_SIZE_TOO_SMALL = register(builder("SPRING_BOOT_PRODUCER_BATCH_SIZE_TOO_SMALL")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_PRODUCER_BATCH_SIZE_TOO_SMALL.md")
            .message("spring.kafka.producer.batch-size is below 16 KiB — under-batched produces lose compression efficiency and inflate broker request rate.")
            .tagline("`spring.kafka.producer.batch-size` (which maps directly to kafka-clients `batch.size`) is the cap on a per-partition record batch. Setting it under the 16 KiB default explicitly trades broker-side throughput for nothing.")
            .mechanism("The producer accumulates records per partition until either `batch.size` bytes accumulate or `linger.ms` elapses, then ships the batch in one ProduceRequest. The 16 KiB default was carefully chosen: large enough that a single 1 KiB record does not stall the broker with overhead, small enough that low-traffic partitions do not wait long for `linger.ms` to trip. Setting `batch-size` to a few hundred bytes makes every record its own ProduceRequest — the broker pays the per-request CPU cost, the compression dictionary cannot warm up (compression is per-batch, so tiny batches compress worse), and the on-wire byte count goes up.")
            .impact("A producer with `batch-size=1024` sending 5000 records/sec generates 5000 ProduceRequests/sec on a single partition, instead of ~300. The broker's request-rate metric spikes 15x; the producer's `record-send-rate` plateaus because each thread is blocked on more handshakes. Compression goes from ~3x to ~1.2x. Wire egress doubles.")
            .whyMatters("Almost every 'under-default batch.size' override is a misunderstanding — someone read about latency vs throughput and lowered batch size without realising the kafka-clients producer is already low-latency-by-default thanks to `linger.ms=0`. The right knob for latency is `linger.ms`, not `batch.size`. Leave `batch.size` at its default unless you have measured a specific reason to change it.")
            .build());

    public static final RuleId STREAMS_TASK_TIMEOUT_MS_TOO_HIGH = register(builder("STREAMS_TASK_TIMEOUT_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_TASK_TIMEOUT_MS_TOO_HIGH.md")
            .message("task.timeout.ms above 30 minutes — a stuck task swallows broker errors for nearly the whole cluster-incident window before giving up.")
            .tagline("`task.timeout.ms` is the maximum time a Streams task will keep retrying transient errors (NotEnoughReplicas, broker outages, network blips) before failing the task. The 5-minute default is generous already; pushing it to >30 minutes turns a transient broker incident into a silent stall instead of a noisy task-failure that triggers your runbook.")
            .mechanism("When a StreamThread encounters a retriable broker error during processing, it does not immediately fail the task. Instead it backs off and retries, accumulating elapsed retry time. Once that elapsed time exceeds `task.timeout.ms`, the task is closed and Streams emits an `org.apache.kafka.streams.errors.TaskCorruptedException` (or similar), which the configured `StreamsUncaughtExceptionHandler` can convert into a thread restart, a process restart, or a Kubernetes-pod-restart depending on the handler. Setting `task.timeout.ms=3600000` (1 hour) means the task silently retries for a full hour before any of that machinery fires — your dashboards keep showing 'task running' while consumer lag accumulates and the rest of the topology starves on a partition the bad task should have failed off.")
            .impact("During a 20-minute broker rolling-restart, the default 5-minute timeout fails the affected tasks; the rebalance moves them off the broken broker; processing continues on healthy brokers within a minute. With `task.timeout.ms=3600000`, the same incident produces 20 minutes of zero-throughput on one partition, no alarms because the task is technically still alive, and consumer lag that takes an hour to drain after the broker recovers. On a 24x7 trading pipeline that is observable as 'mystery 20-minute gap in event time'.")
            .whyMatters("A high `task.timeout.ms` is almost always copy-pasted from a Stack Overflow answer that confused this knob with `request.timeout.ms` or `default.api.timeout.ms`. The right pattern is `task.timeout.ms ≤ 600000` (10 min upper bound), paired with a `StreamsUncaughtExceptionHandler` that decides whether to restart the thread vs the JVM. Above 30 min is essentially 'disable timeout enforcement', which surrenders Streams' ability to recover from broker-side outages.")
            .build());

    public static final RuleId CONSUMER_GROUP_INSTANCE_ID_GENERIC = register(builder("CONSUMER_GROUP_INSTANCE_ID_GENERIC")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_GROUP_INSTANCE_ID_GENERIC.md")
            .message("group.instance.id is a generic placeholder — static membership requires a per-pod unique ID, a shared placeholder defeats it.")
            .tagline("`group.instance.id` opts a consumer into KIP-345 static membership: the broker treats the configured ID as a stable identity across restarts, skipping rebalance during brief pod restarts. If two pods present the *same* `group.instance.id`, the broker rejects the second one and the whole point of static membership collapses.")
            .mechanism("Static membership is a per-consumer string the application supplies via `group.instance.id`. The group coordinator records the assignment under that string instead of generating a new member ID each time. On restart, if the same `group.instance.id` reconnects within `session.timeout.ms`, the coordinator returns the same assignment — no rebalance, no offset commit storm, no warm-up of state stores. The contract is strict: every active member of the group MUST have a distinct `group.instance.id`. The broker enforces this with `FencedInstanceIdException`: the second pod trying to join with an in-use static ID is rejected. If a developer sets `group.instance.id=consumer` (or `app`, `default`, the pod template's own placeholder) in a Deployment with replicas > 1, every restart is a coin-flip about which pod wins the lottery — the rest are fenced and never join until a session timeout expires.")
            .impact("Replicas>1 with a static `group.instance.id` literal: only one pod consumes, the others log `FencedInstanceIdException` and either hot-loop (older kafka-clients) or exit (newer; depends on error handler). Throughput collapses to single-pod, and the on-call gets paged for 'consumer not catching up' with no obvious cause. The dashboards look like the group has 3 members but only one is active. Worst case: a rolling deploy fences the new pod while the old pod is still running, and the new pod sits unable to take over until the old one shuts down — exactly the opposite of zero-downtime.")
            .whyMatters("Static membership is a useful feature, but only when each pod has a unique stable identity. The canonical Kubernetes pattern is `group.instance.id=${HOSTNAME}` (StatefulSet pod name) or `group.instance.id=${POD_NAME}` from the downward API. Hard-coding it to `consumer`, `app`, or any other generic value is strictly worse than leaving it unset (dynamic membership). The detector reuses the existing kafka-clients generic-client-ID list — same shape of bug, same fix.")
            .build());

    public static final RuleId PRODUCER_SEND_BUFFER_BYTES_TOO_SMALL = register(builder("PRODUCER_SEND_BUFFER_BYTES_TOO_SMALL")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_SEND_BUFFER_BYTES_TOO_SMALL.md")
            .message("send.buffer.bytes is set very low — undersized TCP send buffer caps producer throughput to bandwidth × buffer_size / RTT instead of full link speed.")
            .tagline("`send.buffer.bytes` is the SO_SNDBUF socket option the kafka-clients producer asks the kernel to set on every broker connection. Setting it to a small value (≤16 KiB) means the TCP stack cannot keep enough bytes in flight to fill the bandwidth-delay product of a typical cross-AZ link — throughput plateaus far below what the network can carry.")
            .mechanism("TCP throughput is bounded by `min(receiver_window, sender_buffer) / round_trip_time`. The kafka-clients default for `send.buffer.bytes` is 128 KiB; the JDK's default if you set it to `-1` is also typically 128 KiB or higher depending on platform autotuning. Setting `send.buffer.bytes=8192` (8 KiB) on a 1 ms RTT link caps throughput to 8 MB/s per connection regardless of how fast the network is. On a 5 ms cross-AZ RTT the cap is 1.6 MB/s — well below a single producer thread can saturate. The producer's accumulator fills, send() starts blocking on `max.block.ms`, and the app sees mystery latency spikes during traffic bursts.")
            .impact("The throughput cap is invisible from app code: send() returns, the Future completes, no errors. But `record-send-rate` plateaus, the broker shows low ingress per connection, and the producer's `bufferpool-wait-time` climbs. Diagnosis takes hours because nothing logs the SO_SNDBUF value or compares it to BDP. Crucially the default OS autotuning (which scales SO_SNDBUF based on observed RTT and loss) is *disabled* the moment kafka-clients sets a fixed value — so the producer never recovers as the network warms up.")
            .whyMatters("Almost every explicit low value is a copy-paste from a stale tutorial trying to 'reduce memory usage'. The right answer is `send.buffer.bytes=-1` (let the OS autotune) or leave the default 131072. Below 32 KiB is almost never correct; the detector flags anything ≤16384 because that is far below the BDP of any modern network.")
            .build());

    public static final RuleId STREAMS_REPLICATION_FACTOR_TOO_HIGH = register(builder("STREAMS_REPLICATION_FACTOR_TOO_HIGH")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_REPLICATION_FACTOR_TOO_HIGH.md")
            .message("replication.factor above 5 on Streams internal topics — broker disk, network and follower-fetch overhead scale linearly with no durability upside past RF=3.")
            .tagline("`replication.factor` for Streams internal topics (changelog and repartition) controls how many brokers each partition's data is copied to. RF=3 already survives any single AZ outage; RF=4+ multiplies storage cost and follower-fetch bandwidth without buying additional safety on a 3-AZ deployment.")
            .mechanism("Every internal topic Streams creates (`<application-id>-<store>-changelog`, `<application-id>-<sub-topology>-repartition`) is created with this value. On a cluster sized for RF=3 across 3 AZs, the broker layout has each partition's leader and two followers in distinct AZs. Raising RF to 5 forces two additional followers onto brokers that share an AZ with one of the existing replicas — every byte of changelog now costs five times the disk and the leader's outbound replication bandwidth must feed two extra followers. With Streams state-store-heavy topologies, the changelog often dominates the broker's disk footprint, so this lands directly on cluster cost.")
            .impact("On a 100 GB state store, RF=3 means 300 GB of broker disk; RF=5 means 500 GB — an extra 200 GB per Streams application, every day. The leader's outbound replication bandwidth doubles compared to RF=3. Rebalances and broker restarts take 60-70% longer because the recovery path has more followers to catch up. None of this buys you safety: surviving 4 simultaneous broker failures requires a 5-AZ deployment, which almost no team operates.")
            .whyMatters("RF=5+ is almost always either copy-pasted from a 'high durability' tutorial that did not check the AZ topology, or a misunderstanding (replication.factor protects against broker loss; against data loss, you need acks=all + min.insync.replicas=2, which works just as well at RF=3). The right default for Streams is RF=3 for production, RF=1 for local dev. Above 5 is wasteful and the linter flags it at INFO severity because the right value does depend on AZ count.")
            .build());

    public static final RuleId CONSUMER_RECEIVE_BUFFER_BYTES_TOO_SMALL = register(builder("CONSUMER_RECEIVE_BUFFER_BYTES_TOO_SMALL")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_RECEIVE_BUFFER_BYTES_TOO_SMALL.md")
            .message("receive.buffer.bytes is set very low — undersized TCP receive buffer caps consumer throughput to BDP and defeats OS autotuning.")
            .tagline("`receive.buffer.bytes` is the SO_RCVBUF socket option kafka-clients asks the kernel to set on every broker connection. A small value (≤16 KiB) caps consumer fetch throughput at `buffer / RTT` regardless of how large you set `fetch.max.bytes` or how fast the network is.")
            .mechanism("TCP receive-side throughput is bounded by the receiver's advertised window, which itself is bounded by SO_RCVBUF. The kafka-clients default for `receive.buffer.bytes` is 64 KiB (consumer) / 32 KiB (producer); setting it to `-1` lets the JDK pick the platform default, which on modern Linux is autotuned up to several MiB. Setting `receive.buffer.bytes=8192` (8 KiB) on a 5 ms cross-AZ RTT caps the consumer to ~1.6 MB/s per broker connection — far below what a single FetchRequest with the default `fetch.max.bytes=52 MiB` could deliver. Crucially, an explicit fixed value disables the kernel's autotuning algorithm, so the buffer never grows even when the connection is clearly under-utilized.")
            .impact("Consumer lag grows during high-throughput phases, the `records-consumed-rate` metric plateaus well below broker outbound capacity, and increasing `fetch.max.bytes` does not help because the bottleneck is the socket. Diagnosis is hard: the consumer's own metrics show 'plenty of headroom' on every fetch parameter, and the broker's outbound bandwidth is fine because each connection is just slow. The fingerprint is `fetch-throttle-time-avg=0` (broker not throttling) combined with a flat `bytes-consumed-rate` that does not respond to any consumer tuning knob.")
            .whyMatters("Almost every explicit low value is a copy-paste from a stale tutorial trying to 'reduce memory usage'. The correct answer is `receive.buffer.bytes=-1` (OS autotuning) or leave the default 65536. Below 32 KiB is essentially never correct on a real broker connection.")
            .build());

    public static final RuleId QK_OUTGOING_WAIT_FOR_WRITE_COMPLETION_FALSE = register(builder("QK_OUTGOING_WAIT_FOR_WRITE_COMPLETION_FALSE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_OUTGOING_WAIT_FOR_WRITE_COMPLETION_FALSE.md")
            .message("mp.messaging.outgoing.{channel}.waitForWriteCompletion=false — channel acks the upstream Multi as soon as the record is buffered, before the broker accepts it. Silently drops records on broker failure.")
            .tagline("SmallRye's outgoing Kafka connector defaults to `waitForWriteCompletion=true`: the upstream `Multi` only sees a Message ack once the broker has acknowledged the produce. Setting it to false breaks the backpressure-and-ack contract and turns broker write failures into invisible drops.")
            .mechanism("In SmallRye Reactive Messaging, an outgoing Kafka channel is implemented on top of the kafka-clients producer. By default, each emitted `Message` is acked back to the upstream `Multi` only after the producer's `Callback` fires with no exception — i.e. the broker has actually written and replicated the record. Setting `waitForWriteCompletion=false` flips this: the channel acks the message synchronously the moment `producer.send()` returns (which only confirms the record is in the accumulator). If the broker is down, the leader election is in progress, the topic does not exist, or `delivery.timeout.ms` later fires, the failure is logged in the SmallRye connector but the upstream pipeline has already moved on — there is no way to retry, no way to surface the error to the caller, no way to bubble it to a DLQ.")
            .impact("On any broker hiccup — even a 30-second leader election — silent data loss until the broker is healthy again. The application metrics show 'all messages acked', the SmallRye log shows produce failures, and the disconnect between the two is exactly what the bug shows up as in a post-mortem. Worse, transactional outboxes built around 'I emitted the message therefore the DB transaction can commit' silently break their consistency contract — the DB row is written and the message was never delivered.")
            .whyMatters("`waitForWriteCompletion=true` is the SmallRye default for a reason: it preserves end-to-end at-least-once semantics. The only legitimate reason to set false is a fire-and-forget telemetry channel where loss is acceptable AND latency is critical — and even then, the right answer is usually `acks=0` on the producer (still telemetry-correct, still observable). An explicit `waitForWriteCompletion=false` in production properties is almost always a copy-paste from a tutorial that wanted to 'reduce produce latency' without understanding that the cost is silent loss.")
            .build());

    public static final RuleId CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_LOW = register(builder("CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_LOW.md")
            .message("auto.commit.interval.ms below 1 s — floods the group coordinator with offset commits at the cost of all other group operations.")
            .tagline("`auto.commit.interval.ms` is how often the consumer's auto-commit timer flushes offsets to `__consumer_offsets`. The default 5 s is a deliberate compromise between commit latency and coordinator load; pushing it under 1 s loses the throughput compromise without buying any meaningful durability win.")
            .mechanism("With `enable.auto.commit=true`, the consumer schedules an `OffsetCommitRequest` to the group coordinator every `auto.commit.interval.ms`. Each commit is a synchronous round-trip with the coordinator (which serializes commits per group), is durably written to `__consumer_offsets`, and triggers a small replication to the offset-topic followers. The coordinator handles all consumer groups it owns sequentially, so a single misconfigured group with `auto.commit.interval.ms=100` produces 10 commits/second × N consumer instances, all serialized through the same broker — adding latency for every other group sharing that coordinator.")
            .impact("On a cluster with 200 consumer groups and one group at `auto.commit.interval.ms=100`, the offending group can take 60-80% of the coordinator's commit-processing time; healthy groups see their commit latency double or triple. The offending group's own `commit-latency-avg` metric climbs because it is queued behind itself — counterintuitively, lowering `auto.commit.interval.ms` past a point makes commits *slower*, not faster. Crash recovery improvement is marginal at this scale because at-least-once already handles whatever the small window misses.")
            .whyMatters("Sub-second `auto.commit.interval.ms` is almost always either copy-pasted from a non-production sample or a misguided attempt to 'reduce duplicate processing after crash'. The right knob to reduce duplicates is `enable.auto.commit=false` + manual `commitSync()` at the right semantic point, not faster auto-commit. Default 5 s is the right starting point; anything below 1 s should be justified with a measured commit-latency budget.")
            .build());

    public static final RuleId STREAMS_NUM_STANDBY_REPLICAS_TOO_HIGH = register(builder("STREAMS_NUM_STANDBY_REPLICAS_TOO_HIGH")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_NUM_STANDBY_REPLICAS_TOO_HIGH.md")
            .message("num.standby.replicas above 3 — every standby holds a full RocksDB copy on a peer instance, multiplying disk and changelog-restore traffic with diminishing recovery gains.")
            .tagline("`num.standby.replicas` controls how many warm copies of each state store Streams keeps on peer instances. Two standbys already cover the typical 'one instance dies + one redeploys' scenario; pushing it above three multiplies disk and changelog bandwidth on every peer without making failover meaningfully faster.")
            .mechanism("Each standby is a full RocksDB copy of the active task's state store, kept warm by continuously tailing the changelog topic. With `num.standby.replicas=N`, every active task has N standby instances, each storing the full state and consuming the changelog independently. Disk usage on the cluster of Streams instances is (1 + N) × state-store-size; changelog-restore bandwidth (broker-side outbound) is also (1 + N) × the change rate. Recovery time when an instance dies is `O(state-store-size / network)` if a standby is hot — so 1 standby gives you fast recovery, 2 covers a coincident peer failure, and 3 covers a very rare double-peer failure. Beyond that, each additional standby costs 100% of one state-store's worth of disk and changelog read bandwidth for negligible additional reliability.")
            .impact("On a 50 GB state-store topology with 4 Streams instances, `num.standby.replicas=1` uses 100 GB total disk; `=2` uses 150 GB; `=5` uses 300 GB — three times the disk for no meaningful improvement in MTTR. The changelog-fetch traffic on the broker side grows linearly too, which is the harder constraint because the changelog topic is the broker's hottest non-application topic by far.")
            .whyMatters("The right starting point is `num.standby.replicas=1` (warm failover) or `=2` (rolling-restart-safe). Going to 3 makes sense only on extreme-availability pipelines where MTTR matters more than disk cost; above 3 is almost always a misunderstanding of what standby replicas buy. INFO severity because there ARE legitimate cases for higher values, MEDIUM confidence because the linter cannot see your AZ topology or SLO targets.")
            .build());

    public static final RuleId PRODUCER_BATCH_SIZE_TOO_LARGE = register(builder("PRODUCER_BATCH_SIZE_TOO_LARGE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_BATCH_SIZE_TOO_LARGE.md")
            .message("batch.size above 1 MiB — pre-allocated per-partition accumulator buffers inflate producer memory and stall send() under buffer-memory pressure.")
            .tagline("`batch.size` is the per-partition cap on a record batch. The producer pre-allocates this much accumulator memory per active partition — set it too high and a producer talking to a few hundred partitions can exhaust `buffer.memory` instantly, forcing send() to block.")
            .mechanism("The kafka-clients producer's accumulator is structured as one per-partition queue, each of which holds batches up to `batch.size` bytes. Memory is *pre-allocated* the first time a partition becomes active: the first send() to partition P reserves a `batch.size` chunk from the global `buffer.memory` pool. With `batch.size=4194304` (4 MiB) and a producer talking to 256 partitions (a typical fanout for a partitioned analytics pipeline), the first wave of sends reserves 1 GiB of buffer memory — far more than the default `buffer.memory=32 MiB`. The producer immediately blocks on send() with `BufferExhaustedException` after `max.block.ms` expires. Worse, even partitions that no longer receive traffic keep their pre-allocated chunk for `linger.ms` cycles, so the cliff is sticky.")
            .impact("Symptoms are mysterious: send() throws `TimeoutException` or blocks, the broker shows almost no inbound traffic, and the producer's `record-send-rate` is near zero while CPU is idle. The producer's `bufferpool-wait-time-avg` metric spikes to seconds. Engineers often diagnose this as a network issue or a broker problem before realizing they've over-provisioned `batch.size` against the wrong sizing constraint.")
            .whyMatters("The 16 KiB default is good for almost everything. Beyond 64 KiB is rarely useful — the broker's per-request CPU cost flattens, compression dictionaries are already warm, and the marginal compression gain is tiny. Anything above 1 MiB is essentially a configuration bug: it cannot help throughput once `linger.ms`+`compression` are tuned, and it punches a hole in `buffer.memory` budgeting. The right tuning lever for high-throughput partitions is `linger.ms`, not larger `batch.size`.")
            .build());

    public static final RuleId PRODUCER_REQUEST_TIMEOUT_MS_TOO_HIGH = register(builder("PRODUCER_REQUEST_TIMEOUT_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_REQUEST_TIMEOUT_MS_TOO_HIGH.md")
            .message("request.timeout.ms above 5 minutes — single broker hiccup blocks the sender thread for the whole window instead of letting retries kick in.")
            .tagline("`request.timeout.ms` is how long the producer waits for a single ProduceResponse before counting the attempt as a failure and rolling it into the retry/`delivery.timeout.ms` budget. A very high value (>5 min) turns one slow broker into a sender-thread stall, because the producer cannot start the retry until the timeout fires.")
            .mechanism("Inside the producer's sender thread, each in-flight ProduceRequest is tracked with a deadline of `now + request.timeout.ms`. The sender blocks accumulator drains for the request's target partition until the broker responds OR the deadline fires. If you set `request.timeout.ms=600000` (10 min) and the broker leader for partition P goes into a long stop-the-world GC, the sender thread waits the full 10 minutes before noticing — meanwhile no other batch destined for that partition can drain because the leader is still 'in flight'. The `delivery.timeout.ms` budget (default 120 s) is supposed to bound that, but `delivery.timeout.ms ≥ request.timeout.ms` is required, so raising one forces raising the other.")
            .impact("During a 30-second leader election the producer hangs for the full `request.timeout.ms` instead of detecting the leader change quickly. With many in-flight batches across many partitions, the cumulative effect is a producer that 'feels frozen' for minutes during routine cluster operations (broker patching, leader balancing). Application code that depends on send() futures completing within a reasonable wall-clock time accumulates back-pressure that the linter cannot see.")
            .whyMatters("The 30 s default in modern kafka-clients is right for almost everything. The legitimate reasons to raise it are (a) very high latency cross-region clusters, (b) custom interceptor chains that add seconds of work. In both cases, a *targeted* increase to 60-120 s is enough; values above 5 min are essentially 'never give up', which converts cluster wobble into application-level outages. Pair with a `delivery.timeout.ms` change explicitly when raising.")
            .build());

    public static final RuleId CONSUMER_MAX_POLL_INTERVAL_MS_TOO_HIGH = register(builder("CONSUMER_MAX_POLL_INTERVAL_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_MAX_POLL_INTERVAL_MS_TOO_HIGH.md")
            .message("max.poll.interval.ms above 30 minutes — a stuck consumer holds its partition assignment for half an hour before the group coordinator evicts it.")
            .tagline("`max.poll.interval.ms` is the upper bound on how long a consumer can spend processing one poll() batch before the coordinator considers it dead and rebalances its partitions away. Raising it above 30 minutes turns a hung consumer into a 30-minute group-wide stall instead of a fast eviction.")
            .mechanism("The consumer's `max.poll.interval.ms` (default 5 min) is enforced by the group coordinator. Every successful poll() resets the timer. If processing takes longer than this between polls, the next heartbeat arrives stale; the coordinator removes the consumer from the group and triggers a rebalance to redistribute its partitions. With `max.poll.interval.ms=3600000` (1 hour), a consumer that hangs (deadlock, infinite loop, runaway GC, downstream service timeout) holds its assigned partitions for the full hour. Other consumers in the same group cannot pick those partitions up; the lag on those partitions grows without bound.")
            .impact("On a 24-partition topic with 6 consumers, a single hung consumer with `max.poll.interval.ms=3600000` makes 4 partitions stop processing for an hour even though 5 other consumers are healthy and idle. The group's consumer-lag dashboard shows specific partitions falling behind while overall throughput looks fine; the on-call engineer has to ssh into the bad consumer and kill it manually because the coordinator won't evict.")
            .whyMatters("The right pattern is `max.poll.interval.ms` = (worst-case-batch-processing × max.poll.records) + headroom, capped at a few minutes. Values above 30 minutes are almost always either copy-pasted from a batch-job tutorial that confuses Kafka consumers with batch workers, or a workaround for a hot loop that should have used `consumer.pause()` instead. The legitimate use cases (very long external API calls within the consumer) are rare and should be documented with a comment near the override.")
            .build());

    public static final RuleId STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_HIGH = register(builder("STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_HIGH")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_BUFFERED_RECORDS_PER_PARTITION_TOO_HIGH.md")
            .message("buffered.records.per.partition above 100,000 — Streams' per-partition in-memory buffer can grow huge under skew and OOM the JVM.")
            .tagline("`buffered.records.per.partition` (default 1000) caps how many records each Streams partition keeps queued in memory before the consumer is paused to let processing catch up. Setting it very high removes the back-pressure ceiling — under a single hot partition this is a fast path to OOM.")
            .mechanism("Each Streams task has a `PartitionGroup` that holds a per-partition record buffer. When any partition's buffer hits `buffered.records.per.partition`, that partition is paused at the consumer level until the processing loop drains it. The mechanism exists specifically because real topics are skewed: a partition with 10x the traffic of its peers will, with too-large buffers, exhaust JVM heap holding records waiting their turn. The 1000-record default keeps memory bounded to roughly `1000 × record-size × partition-count`; setting `buffered.records.per.partition=1000000` removes that bound, letting one runaway partition load gigabytes into the heap before the OS kills the pod.")
            .impact("On a topology consuming a 100-partition topic with one partition carrying 80% of traffic, the hot partition can fill its buffer to the cap in seconds. With `buffered.records.per.partition=1000000` and 1 KiB records, that is a 1 GiB allocation on a single thread, all of which is `byte[]` GC pressure. The JVM heap spikes; either an OOMKill terminates the pod or the GC pause cascades into rebalances. In both cases the symptom is mysterious 'Streams pod died under load' with no obvious processing-side bottleneck.")
            .whyMatters("Default 1000 is fine for almost every topology; doubling or tripling to 3000-5000 is reasonable for very low-volume streams that benefit from smoother poll() flow. Above 100,000 is essentially 'remove the back-pressure', and the right answer for a runaway partition is investigating the skew (key strategy, salting, etc.), not unbounded buffers. INFO severity because there ARE legitimate scenarios on very-low-record-rate pipelines.")
            .build());

    public static final RuleId SPRING_BOOT_CONSUMER_GROUP_ID_GENERIC = register(builder("SPRING_BOOT_CONSUMER_GROUP_ID_GENERIC")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_CONSUMER_GROUP_ID_GENERIC.md")
            .message("spring.kafka.consumer.group-id is a generic placeholder — two apps deploying with this value will collide on partition assignment and offset commits.")
            .tagline("`spring.kafka.consumer.group-id` becomes the kafka-clients `group.id`. A generic literal like `app`, `my-group`, or `consumer` is the same in production as anywhere else, so two unrelated Spring Boot services with this value silently end up in the same Kafka consumer group.")
            .mechanism("Spring Boot autoconfiguration passes `spring.kafka.consumer.group-id` straight through to the kafka-clients consumer factory as the `group.id` config. The Kafka broker considers any consumers sharing a `group.id` to be a single logical group: it assigns partitions to them collectively and commits offsets under a single key in `__consumer_offsets`. There is no team-prefix or namespace isolation at the broker level — the literal string is the entire identity.")
            .impact("Two unrelated services accidentally configured with `group.id=app` join the same group; Kafka spreads the topic's partitions across them, so each app sees only half the records (and the half it sees is random). The bug surfaces as 'we are missing events' on both apps, the dashboards show full traffic for the topic but only half on each consumer, and the root cause requires comparing the actual `__consumer_offsets` metadata against the deployed app configs. Worse, offset commits from app A overwrite the position app B was at, so restarts of either app reprocess or skip records unpredictably.")
            .whyMatters("The right shape is a domain-prefixed group id: `payments-fraud-detection-v2`, `orders-billing-reconciliation-v3`. Reusing the existing generic-group-id placeholder list keeps the detector consistent with the kafka-clients-level rule. Detection at the Spring Boot property level catches the case where the literal lives in `application.yml` rather than in code.")
            .build());

    public static final RuleId QK_FAIL_ON_DESERIALIZATION_FAILURE_FALSE = register(builder("QK_FAIL_ON_DESERIALIZATION_FAILURE_FALSE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("quarkus-kafka")
            .docPath("quarkus-kafka/QK_FAIL_ON_DESERIALIZATION_FAILURE_FALSE.md")
            .message("mp.messaging.incoming.{channel}.fail-on-deserialization-failure=false — undeserializable records are skipped with a log line and the offset advances. Silent data loss.")
            .tagline("SmallRye's `fail-on-deserialization-failure` (default true) controls whether a deserialization exception from the configured deserializer halts the channel (true) or just logs and advances the offset (false). Setting it to false converts schema mismatches and corrupt records into invisible drops.")
            .mechanism("When `fail-on-deserialization-failure=true` (default), a deserialization exception flows up to the channel's `failure-strategy` (fail / ignore / dead-letter-queue) and gets the same treatment as any processing failure: fail stops the channel, dead-letter-queue routes to a DLQ topic, ignore is the documented escape hatch. When `fail-on-deserialization-failure=false`, the SmallRye connector catches the exception inside the polling loop, logs at WARN, returns a `null`-payloaded `Message`, and commits the offset. The downstream user code never sees the broken record — it either sees `null` (if it handles null) or silently drops it (if it doesn't).")
            .impact("In a payments pipeline, a single corrupt Avro envelope from an upstream producer is logged once at WARN and the consumer moves on. The downstream reconciliation never sees the transaction. The bug surfaces hours or days later as 'why does our DB count differ from upstream's count' and requires re-reading the log to find the WARN line. Worse, the offset advances, so even replaying the topic does not recover the dropped record without a manual offset reset.")
            .whyMatters("The default `fail-on-deserialization-failure=true` is correct. The right escape hatch for bad records is a proper deserialization-error pattern: either Spring's `ErrorHandlingDeserializer` (kafka-clients level) or SmallRye's `failure-strategy=dead-letter-queue` so the broken envelope lands in a DLQ topic for human inspection. Setting `fail-on-deserialization-failure=false` is essentially 'log and lose'.")
            .build());

    public static final RuleId CONSUMER_FETCH_MAX_BYTES_TOO_HIGH = register(builder("CONSUMER_FETCH_MAX_BYTES_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_FETCH_MAX_BYTES_TOO_HIGH.md")
            .message("fetch.max.bytes above 100 MiB — single FetchResponse can stall the consumer for seconds with no other partitions making progress in the meantime.")
            .tagline("`fetch.max.bytes` is the upper bound on a single FetchResponse across all partitions of a fetch. Setting it very high (>100 MiB) trades fairness and latency for nothing — the broker can take seconds to gather and serialize that much data while the consumer's poll() blocks.")
            .mechanism("Each fetch is one request to the broker that returns records from all partitions assigned to this consumer that the broker considers ready. The broker streams up to `fetch.max.bytes` of records back; until that response is fully written, the consumer's poll() does not return. On a wide assignment (50+ partitions), a 200 MiB fetch can take several seconds to assemble broker-side, then several more to drain over the socket — during which the consumer's heartbeat thread keeps the group alive but no records are being processed. Memory cost is also linear: the consumer pre-allocates buffers sized against `fetch.max.bytes`, so heap usage scales with this value even when traffic is light.")
            .impact("On a 10 ms-RTT cluster, a 256 MiB `fetch.max.bytes` produces poll() latencies in the 2-5 second range under load — application logic that assumed sub-second polls (record-per-poll style, time-based committers) breaks subtly. Heap usage on the consumer grows by hundreds of MiB. Per-partition records are returned in big chunks, defeating the broker's per-partition fairness — one chatty partition can starve quieter ones until its records are exhausted.")
            .whyMatters("The 52 MiB default fits the largest record-batch a broker will produce by default (1 MiB per partition × 50 partitions roughly). Raising it above 100 MiB is essentially never correct unless you have explicitly tuned `max.partition.fetch.bytes` and `message.max.bytes` together AND profiled the heap impact. The right tuning lever for 'too few records per poll' is `max.poll.records`, not `fetch.max.bytes`.")
            .build());

    public static final RuleId CONSUMER_SESSION_TIMEOUT_MS_TOO_HIGH = register(builder("CONSUMER_SESSION_TIMEOUT_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_SESSION_TIMEOUT_MS_TOO_HIGH.md")
            .message("session.timeout.ms above 60 s — broker waits a full minute (or more) before evicting a dead consumer; in-flight partitions stay frozen for that whole window.")
            .tagline("`session.timeout.ms` is how long the group coordinator waits for a heartbeat before declaring a consumer dead and triggering a rebalance. Raising it above the broker's `group.max.session.timeout.ms` (default 60 s) means a crashed pod's partitions stay unowned for the full window — no progress, no error, just silent stall.")
            .mechanism("The consumer's heartbeat thread sends a Heartbeat request to the group coordinator every `heartbeat.interval.ms`. If the coordinator misses heartbeats for longer than `session.timeout.ms`, it expels the member and starts a rebalance. A high session timeout shifts that detection window: a value of 5 minutes means the coordinator will not even notice a crashed consumer for 5 minutes, during which its assigned partitions accumulate lag and consumer-group rebalance latency reports show 'healthy'. Worse, the broker enforces a server-side ceiling (`group.max.session.timeout.ms`, default 1800000 ms, often lowered to 60000 ms in managed clusters): if you exceed it, the consumer's JoinGroup request is rejected with an InvalidSessionTimeout error at startup. The pod crashloops without an obvious cause.")
            .impact("On a fleet with autoscaling or rolling deploys, a 5-minute session timeout means every pod termination produces 5 minutes of partition stall before the broker rebalances. End-to-end lag SLOs blow through. If a managed broker rejects the timeout, every consumer in the application fails to start until someone notices the cryptic 'InvalidSessionTimeout' line in logs.")
            .whyMatters("Almost every 'session.timeout.ms' override above 60 s is either a copy-pasted workaround for a different problem (long poll loop → fix `max.poll.interval.ms` instead) or a leftover from a Kafka < 2.5 codebase where the default was 10 s and you needed 30 s headroom. The modern default (45 s) is already generous; leave it alone unless you have a specific, measured reason.")
            .build());

    public static final RuleId CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_HIGH = register(builder("CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_HIGH.md")
            .message("heartbeat.interval.ms ≥ session.timeout.ms / 3 — too few heartbeats fit in the session window; one missed packet triggers a rebalance.")
            .tagline("`heartbeat.interval.ms` should fit comfortably inside `session.timeout.ms`, with margin for at least 3 heartbeats per window. Raising it close to (or past) session.timeout.ms means one dropped packet or GC pause kicks the consumer out of the group.")
            .mechanism("The consumer's background heartbeat thread sends a Heartbeat RPC every `heartbeat.interval.ms`. The coordinator declares the consumer dead if it sees no heartbeat for `session.timeout.ms`. The default ratio is 3:1 (3 s heartbeat, 45 s session) — three opportunities to recover from a transient hiccup before eviction. Raising heartbeat.interval.ms to 30 s (still below the 45 s session) means there's only one heartbeat per session window: if it happens to coincide with a stop-the-world GC, a network blip, or coordinator-side queueing, the coordinator misses it, calls the consumer dead, and rebalances the whole group. The 'true positive' rate for these evictions is essentially zero — the consumer was fine, the heartbeat just got delayed.")
            .impact("Spurious rebalances are the most expensive failure mode in a consumer group: every other consumer pauses while the join completes (often hundreds of ms to several seconds on wide assignments), in-flight records are reprocessed, and offset-commit races become visible. A heartbeat misconfiguration that produces one false eviction per hour on a 20-pod fleet costs ~20 rebalances per hour — group throughput collapses to 'mostly idle'.")
            .whyMatters("Most engineers raise heartbeat.interval.ms thinking 'fewer heartbeats = less broker load'. The cost is microscopic (a few hundred bytes of network per consumer per second), the benefit is negative. The right rule is: `heartbeat.interval.ms ≤ session.timeout.ms / 3`. The plugin flags any value above one-third of the session timeout (default session 45 s → flag heartbeat ≥ 15 s).")
            .build());

    public static final RuleId PRODUCER_TRANSACTION_TIMEOUT_MS_TOO_HIGH = register(builder("PRODUCER_TRANSACTION_TIMEOUT_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_TRANSACTION_TIMEOUT_MS_TOO_HIGH.md")
            .message("transaction.timeout.ms above 900 s — a hung transactional producer blocks the LSO of every partition it wrote to for the full timeout. Downstream EOS consumers stall completely.")
            .tagline("`transaction.timeout.ms` is the maximum wall-clock time the broker allows for a transaction before it aborts it server-side. Setting it very high (>15 min) means a crashed transactional producer can freeze the LSO on every partition it touched until the timeout elapses — every read_committed consumer downstream stops making progress.")
            .mechanism("Kafka transactions are gated by the LSO (Last Stable Offset): a read_committed consumer can only return records up to the LSO, which is the offset of the oldest in-flight transaction's first record. When a transactional producer crashes mid-transaction without sending an EndTxn marker, the broker waits `transaction.timeout.ms` before assuming the producer is dead and aborting the transaction. During that whole window, the LSO does not advance past the dead transaction's first record. Every consumer reading those partitions in `isolation.level=read_committed` (the EOS default) sees its lag grow linearly with time, with no error and no warning. If `transaction.timeout.ms` is 1 hour, that's 1 hour of zero-progress downstream after every transactional crash. The broker also enforces its own ceiling via `transaction.max.timeout.ms` (default 15 min on most managed clusters): exceeding it rejects InitProducerId at startup with InvalidTxnTimeoutException, and the producer crashloops.")
            .impact("In a payments pipeline using exactly-once-semantics, a single crashed producer with `transaction.timeout.ms=3600000` blocks the downstream reconciliation for an hour. SLO-bound alerting fires for 'consumer lag' but the consumer is healthy — the LSO just hasn't moved. The fix (rolling restart or manual transaction abort via the AdminClient) requires operator intervention. With the default 60 s timeout, the broker self-heals.")
            .whyMatters("The Kafka default (60 s) is tuned to be just long enough for the slowest legitimate batch and just short enough that crash recovery happens in under a minute. Raising it is almost always cargo-culted from a 'my transactions are timing out' incident — but the real fix for those incidents is to keep transactions short (under 30 s of work), not to widen the abort window. Anything above 15 min is well into 'this is now blocking downstream' territory.")
            .build());

    public static final RuleId CONSUMER_MAX_POLL_RECORDS_TOO_LOW = register(builder("CONSUMER_MAX_POLL_RECORDS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_MAX_POLL_RECORDS_TOO_LOW.md")
            .message("max.poll.records ≤ 5 — every poll round-trip returns at most a handful of records; per-poll fixed overhead dominates and throughput collapses.")
            .tagline("`max.poll.records` caps the number of records a single `poll()` returns. Setting it to 1 (or 2-5) means each record carries the full overhead of a poll loop iteration — network round-trip + deserialization batch start/stop + offset bookkeeping — and the consumer can never amortize that cost across a real batch.")
            .mechanism("The consumer fetcher pulls a large batch from the broker (sized by `fetch.max.bytes` / `max.partition.fetch.bytes`) and stores it in an internal buffer. `max.poll.records` is the slice size the application sees per `poll()` call. With max.poll.records=1, the buffer is sliced one record at a time; the next 499 records sit idle in the consumer's heap until the next poll loop completes. The application loop runs 500 iterations to drain what was effectively one fetch — every iteration measures one poll's worth of timing overhead (callback dispatch, position update, lag metric refresh) for one record's worth of work.")
            .impact("Compared to the default (500), a max.poll.records=1 consumer processes records at roughly 1/100th the throughput. Per-record latency *goes up*, not down, because the consumer is spending most of its CPU in poll-loop machinery instead of user code. The bug is invisible to monitoring: lag grows linearly with traffic, but each individual record is processed 'quickly'.")
            .whyMatters("The pattern is almost always 'I want to commit after every record' or 'I want to process records one at a time'. Both are achievable without max.poll.records=1: use the default batch size and call `commitSync(Map)` per-record inside the loop. The default 500 is correct for almost every workload.")
            .build());

    public static final RuleId PRODUCER_MAX_REQUEST_SIZE_TOO_LOW = register(builder("PRODUCER_MAX_REQUEST_SIZE_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_MAX_REQUEST_SIZE_TOO_LOW.md")
            .message("max.request.size below 64 KiB — caps each ProduceRequest below a typical message batch. Producer cannot batch effectively; any individual record larger than the cap fails outright.")
            .tagline("`max.request.size` is the per-request ceiling on uncompressed bytes the producer will send. Lowering it below ~64 KiB defeats batching (the accumulator can't fill a request before flushing) and turns a single oversized record into a hard `RecordTooLargeException` at send time.")
            .mechanism("The producer's record-accumulator groups records into per-partition batches; each ProduceRequest combines batches from many partitions. The request flush is triggered by linger.ms timeout, batch.size threshold *or* when adding the next batch would exceed `max.request.size`. Set the cap below typical batch sizes (default 16 KiB × multiple partitions) and the flush trigger becomes 'reached the request cap', producing many small requests instead of a few big ones. Worse: any single record whose serialized size exceeds `max.request.size` immediately throws `RecordTooLargeException` in the calling thread — no retry, no fallback. The broker-side ceiling (`message.max.bytes`, default 1 MiB) is a separate constraint; lowering the client cap below it has no safety benefit, only a throughput cost.")
            .impact("On a producer sending typical 10 KiB Avro envelopes, dropping max.request.size from the default 1 MiB to 32 KiB cuts effective throughput by ~3x (more requests, more network syscalls, more broker handshakes). Any record above 32 KiB — a rare but real long-tail event in most pipelines — fails with no retry and an exception the app code likely doesn't handle gracefully.")
            .whyMatters("The default 1 MiB matches the broker default and is the right value for almost every workload. Lowering it is almost always cargo-culted from a debugging session ('we got an OOM, let's cap memory') — but the actual memory lever is `buffer.memory`, not `max.request.size`. Leave this at its default.")
            .build());

    public static final RuleId KAFKA_METADATA_MAX_AGE_MS_TOO_HIGH = register(builder("KAFKA_METADATA_MAX_AGE_MS_TOO_HIGH")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_METADATA_MAX_AGE_MS_TOO_HIGH.md")
            .message("metadata.max.age.ms above 10 min — client keeps stale metadata for far longer than necessary; leader-failover events take that whole window to be noticed.")
            .tagline("`metadata.max.age.ms` is how long the client trusts its cached cluster metadata before forcing a refresh. Raising it above 10 minutes means leader transitions, ISR shrinks, and partition reassignments take that long to propagate; until they do, the client routes to the old (often unreachable) leader and producer requests fail with `NotLeaderOrFollowerException` retries.")
            .mechanism("The kafka-clients metadata cache holds the broker list, per-topic partition layout, and per-partition leader. It refreshes on demand (explicit error from a stale-route response) and proactively every `metadata.max.age.ms`. Between proactive refreshes, the client trusts its cache absolutely. Raising the cap to 1 hour means a leader move triggered by a broker restart, a partition reassignment, or an ISR shrink takes up to an hour to be picked up by this client — during which the producer sees retriable errors on every send to the affected partition until the metadata cache happens to refresh. The error-driven refresh path does catch these eventually, but adds latency proportional to the number of in-flight requests that hit the stale leader.")
            .impact("On a cluster doing a routine rolling restart (one broker every 5 min), a producer with metadata.max.age.ms=3600000 sees 5-10 % of sends bounce off stale leaders for the whole restart window. With the default 5 min, that drops to <1 % because each broker move triggers a metadata refresh within the next few minutes anyway.")
            .whyMatters("The default (5 min, 300_000 ms) is tuned for the broker's leader-election frequency. The only legitimate reason to raise it is to reduce metadata-request load on the brokers — but that's a load problem worth profiling, not preempting. If you're worried about metadata churn, lower it, don't raise it.")
            .build());

    public static final RuleId KAFKA_BOOTSTRAP_SERVERS_SINGLE_BROKER = register(builder("KAFKA_BOOTSTRAP_SERVERS_SINGLE_BROKER")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_BOOTSTRAP_SERVERS_SINGLE_BROKER.md")
            .message("bootstrap.servers contains a single host:port — no fallback if that broker is unreachable at startup; the client cannot discover the cluster.")
            .tagline("`bootstrap.servers` is the comma-separated list of brokers the client contacts on startup to fetch cluster metadata. With only one entry, that one broker becoming unreachable (rolling restart, AZ outage, DNS hiccup) leaves the client unable to discover the cluster at all — startup fails or stalls.")
            .mechanism("The client tries each entry in `bootstrap.servers` in turn until one responds with metadata. After that, the metadata response itself contains the full broker list, so steady-state operations work regardless of which bootstrap entry was used. The single-entry failure mode is at startup only — but every pod restart, every new consumer instance, every new producer goes through that startup. In a multi-broker cluster, listing 3+ brokers in bootstrap.servers means any single broker outage is invisible to clients spinning up during the outage. Listing one means the cluster has a per-broker SPOF that operators never see in steady state but always hits during incidents.")
            .impact("On a rolling broker restart, pods restarting at the wrong moment fail their first metadata fetch, throw `TimeoutException`, and either crashloop or hang on startup. With a 3-entry bootstrap list, the same restart is invisible — one of the other two brokers responds.")
            .whyMatters("The cost of adding more bootstrap entries is zero (the metadata cache supersedes them after first contact) and the benefit is real every time a broker is unreachable. The single-entry shape is almost always a leftover from a single-node dev cluster.")
            .build());

    public static final RuleId SCHEMA_REGISTRY_URL_LOCALHOST = register(builder("SCHEMA_REGISTRY_URL_LOCALHOST")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("schema-registry")
            .docPath("schema-registry/SCHEMA_REGISTRY_URL_LOCALHOST.md")
            .message("schema.registry.url points at localhost / 127.0.0.1 / 0.0.0.0 — every serializer/deserializer call hits the pod's loopback interface. Will fail in any non-local environment.")
            .tagline("`schema.registry.url` configures the Confluent Schema Registry HTTP endpoint. A localhost address means each Avro/Protobuf/JSON-Schema serialize call tries to reach the pod's own loopback — fine on a developer laptop, broken everywhere else.")
            .mechanism("The Confluent serializers (KafkaAvroSerializer, KafkaProtobufSerializer, KafkaJsonSchemaSerializer) call SR on every record they have not yet seen the schema for: GET /subjects/{topic}-value/versions/latest, POST /subjects/{topic}-value (if auto-register is on), GET /schemas/ids/{id}. Each call is HTTP. With `schema.registry.url=http://localhost:8081`, every one of those calls hits the loopback interface of the pod making the call; if SR is not running inside the pod (it never is in production), the calls fail with `IOException: Connection refused` and the serialize call throws `SerializationException`. The producer cannot send. The consumer cannot deserialize. Nothing moves.")
            .impact("The bug is invisible on a dev laptop where SR happens to run locally. It surfaces as a hard producer/consumer failure the moment the artifact ships to staging or prod, often during the first record after deploy. The error message references SR — operators reach for the SR oncall before realizing it's a misconfigured client.")
            .whyMatters("Mirrors the `KAFKA_BOOTSTRAP_SERVERS_LOCALHOST` shape exactly: a sensible dev default that must be overridden in real environments. Externalize via env var / Spring profile / Quarkus `%prod` override.")
            .build());

    public static final RuleId STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_NONE = register(builder("STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_NONE")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_NONE.md")
            .message("rack.aware.assignment.strategy=none — explicitly disables rack-aware task assignment. Standby tasks may be placed in the same AZ as their active, defeating cross-AZ failover.")
            .tagline("Kafka Streams 3.7+ defaults `rack.aware.assignment.strategy` to `min_traffic` (minimize cross-AZ traffic while preferring different racks for active/standby pairs). Setting it back to `none` reverts to pre-3.7 behavior: tasks land wherever the simple sticky assignor puts them, often colocating active and standby in the same AZ.")
            .mechanism("Streams' StickyTaskAssignor (rack-aware variant) inspects the `client.rack` of each Streams instance and tries to: (a) keep active+standby pairs on different racks, and (b) minimize cross-rack repartition traffic. Setting `rack.aware.assignment.strategy=none` bypasses that logic entirely — the legacy strategy ignores rack and assigns tasks based on stickiness + load balance only. On a 3-AZ deployment with `num.standby.replicas=1`, you can easily end up with the active task and its sole standby in the same AZ; an AZ-wide outage then kills both, and the application has no warm replica to fail over to.")
            .impact("In a Streams cluster relying on standbys for sub-second failover, accidentally colocating active+standby in one AZ turns a planned 30-second failover into a multi-minute cold start (rebuilding state from the changelog) when an AZ goes away. The bug only surfaces during the AZ outage you were preparing for.")
            .whyMatters("The default since 3.7 (`min_traffic`) is correct for almost every multi-AZ deployment. Setting `none` is almost always either copy-pasted from a 3.6-or-older config or a leftover debug toggle. If you genuinely need to disable rack-aware assignment, the team needs to know — flag it.")
            .build());

    public static final RuleId CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_LOW = register(builder("CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_LOW.md")
            .message("max.partition.fetch.bytes below 1 MiB — broker's default max.message.bytes is 1 MiB, so any single record above this cap is silently undelivered (fetch returns empty for that partition).")
            .tagline("`max.partition.fetch.bytes` is the per-partition ceiling on how much data the broker streams in a single fetch response. Setting it below 1 MiB (the broker's default `max.message.bytes`) means a single legitimate record above the cap blocks the partition: the broker can't fit even one record into the fetch slot, the fetch returns empty for that partition, and the consumer makes no progress on it.")
            .mechanism("On each fetch, the broker walks the log for each assigned partition and tries to fit at least one record-batch into a slot of size `max.partition.fetch.bytes`. If the next record-batch is larger than the slot, Kafka has a fallback (broker-side) that returns the oversized batch anyway — but ONLY since 0.11; older brokers (and some managed-cluster configurations that revert to strict semantics) genuinely refuse and return empty. Even on modern brokers, setting this low means: (a) the fetch wastes a round-trip because the per-partition budget is too small to amortize the request overhead; (b) lag grows on hot partitions because the consumer drains them in tiny slices; (c) any application invariant assuming 'a poll() returns roughly fetch.max.bytes / partition_count per partition' breaks. The default 1 MiB is sized to match the broker default `max.message.bytes` for a reason: it's the smallest fetch slot that guarantees a single max-sized record always fits.")
            .impact("On a topic carrying occasional 800 KiB Avro envelopes with `max.partition.fetch.bytes=524288` (512 KiB), those records bounce off the partition limit on every fetch. Modern brokers return them anyway in a single-record batch, but the consumer pays an extra round-trip per such record and the throughput on that partition collapses. On older/configured-strict brokers, the partition stops making progress until the operator widens the cap.")
            .whyMatters("The 1 MiB default is the safest value for almost every workload. Lowering it is almost always a misguided attempt to 'use less memory per fetch' — but per-fetch memory is bounded by `fetch.max.bytes`, not by this per-partition cap. Leave this at default unless you've raised broker `max.message.bytes`.")
            .build());

    public static final RuleId CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_HIGH = register(builder("CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_HIGH.md")
            .message("default.api.timeout.ms above 5 min — admin / blocking-poll-style consumer calls (commitSync, position, beginningOffsets, listTopics) hang for the full window before throwing.")
            .tagline("`default.api.timeout.ms` is the default upper bound on blocking consumer API calls that don't have an explicit timeout argument. Setting it very high (>5 min) turns transient broker hiccups into multi-minute application-thread hangs that look indistinguishable from a deadlock.")
            .mechanism("Methods like `commitSync()`, `position()`, `beginningOffsets()`, `endOffsets()`, `listTopics()`, `partitionsFor()`, and `committed()` use `default.api.timeout.ms` as their fallback timeout when no explicit one is passed. Internally each of these calls a coordinator or fetcher RPC and retries on retriable errors until the deadline. If the broker is temporarily unreachable (rolling restart, transient partition leader move), the call retries the whole window before throwing `TimeoutException`. With `default.api.timeout.ms=600000` (10 min), a `commitSync()` from a graceful-shutdown path blocks the shutdown thread for 10 minutes before the calling code even gets to see the failure. Pod terminationGracePeriodSeconds (default 30 s in k8s) is far below this — the pod gets SIGKILLed mid-hang, the application's shutdown hook never returns, and operators see 'pod stuck terminating' alerts.")
            .impact("A multi-minute `commitSync()` hang inside a graceful-shutdown path turns a 30-second rolling restart into a 10-minute one (the orchestrator force-kills the pod and reports failure). Same for `position()` calls in a healthcheck — a healthcheck that should fail fast instead blocks the readiness probe for minutes.")
            .whyMatters("The default (60 s) is tuned to be just long enough for a normal broker hiccup to recover and short enough that failure surfaces before downstream timeouts (k8s probe, ingress, caller). Raising it usually means 'I keep getting TimeoutExceptions' — but the right fix for that is faster brokers or shorter retries, not a wider timeout.")
            .build());

    public static final RuleId SPRING_BOOT_CONSUMER_MAX_POLL_RECORDS_TOO_LOW = register(builder("SPRING_BOOT_CONSUMER_MAX_POLL_RECORDS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_CONSUMER_MAX_POLL_RECORDS_TOO_LOW.md")
            .message("spring.kafka.consumer.max-poll-records ≤ 5 — every poll round-trip returns at most a handful of records; per-poll overhead dominates and effective throughput collapses.")
            .tagline("Spring Boot exposes `spring.kafka.consumer.max-poll-records` as a typed alias for the kafka-clients `max.poll.records`. Setting it to 1 (or 2-5) means each KafkaListener invocation processes one record at a time — the listener container's per-poll overhead (acknowledgment plumbing, observability instrumentation, error-handler setup) dominates the work the listener actually does.")
            .mechanism("The Spring listener container (ConcurrentMessageListenerContainer / KafkaMessageListenerContainer) runs a poll-loop that calls `consumer.poll()`, then dispatches each record (or the batch) to the user's `@KafkaListener`. For each poll iteration the container does measurable work — error-handler reset, observation span open/close, retry-template state machine, ack thread coordination. With max-poll-records=1, every record pays the full per-poll cost; with the default 500, that cost is amortized across hundreds of records. Throughput on Spring listeners with max-poll-records=1 is typically 50-100× worse than with the default, and the bottleneck shows up as 'Spring is slow' rather than 'we configured one-record-per-poll'.")
            .impact("A KafkaListener that should sustain 10000 rec/sec at default settings hits 100-500 rec/sec with max-poll-records=1. Consumer lag grows under any non-trivial traffic. The pattern is invisible to anyone who didn't write the original config — Spring's listener implementation doesn't surface it as a warning.")
            .whyMatters("The Spring-side default (500, inherited from kafka-clients) is correct for almost every workload. The 'one record at a time' pattern is achievable with the default poll size and either RECORD-level acknowledgment mode or per-record commits — no need to throttle the poll itself.")
            .build());

    public static final RuleId PRODUCER_MAX_BLOCK_MS_TOO_HIGH = register(builder("PRODUCER_MAX_BLOCK_MS_TOO_HIGH")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_MAX_BLOCK_MS_TOO_HIGH.md")
            .message("max.block.ms above 60 s — producer.send() and partitionsFor() block the calling thread for the full window when the buffer is full or metadata is stale. Caller-thread stalls become invisible.")
            .tagline("`max.block.ms` is how long `producer.send()`, `producer.partitionsFor()`, `producer.beginTransaction()` and similar APIs will block the calling thread waiting on metadata or accumulator space before throwing `TimeoutException`. Raising it above 60 s turns a transient broker hiccup or a full record-accumulator into a multi-minute calling-thread stall — and the caller is almost always a request-handling thread that must NOT block.")
            .mechanism("The producer's `send()` call is documented as 'async' but has a synchronous prelude: it may need to fetch fresh metadata for the topic (blocks up to max.block.ms) and it may need to wait for buffer.memory to free up (also blocks up to max.block.ms). With the default 60 s, those waits cap at a minute and any retry/circuit-breaker upstream sees the failure quickly. With max.block.ms=300000 (5 min), the calling thread sits in `send()` for up to 5 minutes — no log line, no error, no metric, just a held thread. If the caller is a request handler, every concurrent request waiting on the same thread pool times out at the HTTP layer; downstream sees 5xx storms while the producer 'looks healthy' from its own metrics.")
            .impact("On a brief broker outage, with max.block.ms=300000, every in-flight producer.send() call holds its calling thread for up to 5 minutes. A 200-thread Tomcat pool drains within seconds — no requests served. The same outage with max.block.ms=60000 produces 1-minute degraded service then recovers; with max.block.ms=5000 (more conservative) the caller can route the failed send to a fallback (DLQ, retry queue, log-and-continue) within seconds.")
            .whyMatters("The default (60 s) is the maximum that's still 'reasonable for a synchronous API'. Raising it is almost always cargo-culted from a 'we keep getting TimeoutException' debugging session, but the fix for that is more producer instances, more `buffer.memory`, or shorter `linger.ms` — not a wider block window. Caller threads should never be willing to wait for minutes inside a producer call.")
            .build());

    public static final RuleId CONSUMER_FETCH_MAX_WAIT_MS_TOO_LOW = register(builder("CONSUMER_FETCH_MAX_WAIT_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_FETCH_MAX_WAIT_MS_TOO_LOW.md")
            .message("fetch.max.wait.ms below 50 ms — broker returns immediately even when fetch.min.bytes is not satisfied; consumer spins in tight empty-fetch loop.")
            .tagline("`fetch.max.wait.ms` is the longest the broker will wait for `fetch.min.bytes` of data to accumulate before returning the fetch response. Setting it very low (< 50 ms) turns the broker side into 'return immediately' and the consumer side into a tight empty-fetch loop, burning broker CPU and consumer CPU for no records.")
            .mechanism("Each FetchRequest tells the broker: 'wait up to fetch.max.wait.ms for at least fetch.min.bytes to be available, then return whatever you have'. The default (500 ms with fetch.min.bytes=1) means the broker can return on the very next record, but if there are no records it waits half a second before responding empty — which lets the consumer's poll() block efficiently. Setting fetch.max.wait.ms=10 ms means the broker returns within 10 ms regardless, so an idle topic produces a fetch every 10 ms (100/sec/partition) all returning empty responses. The consumer's poll-loop iterates 100×/sec with zero records, the broker's request-handler queue fills with these empty requests, and both ends waste CPU.")
            .impact("On an idle topic, fetch.max.wait.ms=10 produces ~100 empty FetchRequests per second per partition per consumer. On a cluster with 50 consumers × 100 partitions, that's 500000 RPC/sec of pure noise — visible as elevated broker network-handler-thread CPU and a flat traffic graph that no operator can correlate to anything.")
            .whyMatters("The default (500 ms) is the right value for almost every consumer. The lever for 'lower latency on the first record' is `fetch.min.bytes=1` (default) — that's what makes the broker return on the very first byte. Dropping `fetch.max.wait.ms` below 100 ms is almost always a misunderstanding of which knob to turn.")
            .build());

    public static final RuleId PRODUCER_RETRY_BACKOFF_MS_TOO_HIGH = register(builder("PRODUCER_RETRY_BACKOFF_MS_TOO_HIGH")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_RETRY_BACKOFF_MS_TOO_HIGH.md")
            .message("retry.backoff.ms above 30 s — producer waits >30 s between retries of retriable errors; recovery from transient broker issues (leader move, throttling) takes much longer than necessary.")
            .tagline("`retry.backoff.ms` is the delay between retries for retriable errors (NotLeaderForPartition, NetworkException, broker quota throttling). The default (100 ms, with exponential backoff up to retry.backoff.max.ms = 1000 ms) recovers in seconds. Setting it to 30 s+ stretches every transient failure into a multi-minute recovery window with no operational benefit.")
            .mechanism("On a retriable error, the producer waits `retry.backoff.ms` (with exponential growth up to `retry.backoff.max.ms`, default 1000 ms) before resending the failed batch. The default tuning (100 ms → 200 ms → 400 ms → 800 ms → 1000 ms) gives ~5 retries within the first 2 seconds — enough to ride out a leader move (typically 200-500 ms) or a brief quota-throttle. Raising retry.backoff.ms to 30 s means the first retry happens 30 s after the failure, the next at 60 s, etc. — a single leader move that the default would have recovered from invisibly now produces 30+ seconds of producer pause for every affected partition.")
            .impact("Under a routine broker rolling restart (one broker every 5 min), a producer with retry.backoff.ms=30000 sees each leader move produce ~30-60 seconds of stuck-send-for-that-partition behavior. With the default 100 ms, the same restart is invisible to producer-side metrics — every batch retries within a second and continues.")
            .whyMatters("There is no production scenario where 100 ms is too aggressive. The default exists because Kafka's own retry semantics (idempotent producer, bounded delivery.timeout.ms) require the producer to keep trying quickly to make progress. Raising retry.backoff.ms above a second is almost always cargo-culted from a 'we're retrying too much' debugging session — but the real signal there is the underlying failure, not the retry cadence.")
            .build());

    public static final RuleId PRODUCER_PARTITIONER_IGNORE_KEYS_TRUE = register(builder("PRODUCER_PARTITIONER_IGNORE_KEYS_TRUE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_PARTITIONER_IGNORE_KEYS_TRUE.md")
            .message("partitioner.ignore.keys=true — record key is ignored for partition routing; key-based ordering and compacted-topic semantics break silently.")
            .tagline("`partitioner.ignore.keys=true` tells the built-in (KIP-794) partitioner to ignore record keys entirely and pick partitions purely by load/latency. Two records with the same key now land on different partitions. Anything that relies on key-based ordering (state stores, compacted topics, joins, materialized views) breaks at the data-model layer — silently, with no error.")
            .mechanism("Since Kafka 3.3, the built-in partitioner has two modes: key-hash (default — records with the same key always land on the same partition) and 'sticky/adaptive' (no key — pick a partition based on broker queue latency and rotate when batches fill). Setting `partitioner.ignore.keys=true` forces the second mode even when keys are present. The producer no longer hashes the key; it picks whichever partition will batch best right now. The choice is per-batch and depends on broker latency, so the same key can land on partition 3 at 10:00 and partition 7 at 10:01.")
            .impact("Compacted topic with this setting: the same key now has versions on different partitions, the log compactor never deduplicates them, the 'latest value per key' invariant breaks. Streams app with this setting upstream: KTable from the compacted topic now has stale duplicates, joins return wrong matches, state stores diverge across instances. Event-sourced consumer reading per-key ordering: events for entity X interleave across partitions and reorder relative to each other on consumer assignment changes.")
            .whyMatters("This is one of the most dangerous Kafka config flags because the broker accepts it, the producer accepts it, records flow, and nothing logs an error — the data model just quietly breaks. The flag exists for write-heavy null-key workloads where partition affinity doesn't matter (firehose logs, raw metric samples). For anything keyed, it is wrong. The fix is to leave it at the default `false`.")
            .build());

    public static final RuleId SPRING_BOOT_LISTENER_ACK_MODE_RECORD = register(builder("SPRING_BOOT_LISTENER_ACK_MODE_RECORD")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_LISTENER_ACK_MODE_RECORD.md")
            .message("spring.kafka.listener.ack-mode=RECORD — Spring commits the offset synchronously after every single record; per-record commit RTT collapses throughput.")
            .tagline("`ack-mode=RECORD` makes the Spring listener container call `consumer.commitSync()` after every successfully processed `ConsumerRecord`. Every record now pays a network round-trip to the group coordinator before the next record can be processed. Throughput collapses by 10-100× vs the default `BATCH` mode, for a duplicate-on-crash protection that the framework already provides at much lower cost.")
            .mechanism("Spring's listener container has six ack-modes. The default, `BATCH`, commits the entire poll() batch synchronously after the last record's listener call returns — one commit per poll, ~ once every 500 records. `RECORD` commits after each record. With a 1-3 ms commit RTT to the coordinator and a per-record processing cost in the µs range for typical CPU-bound listeners, the commit becomes the dominant cost: a listener that could sustain 50 000 records/s in BATCH mode drops to 300-1000 records/s in RECORD mode. The coordinator itself also melts under the load — a single misconfigured app can sustain enough commit volume to throttle every other consumer group on the same coordinator broker.")
            .impact("On every busy listener pod, CPU sits 80%+ idle while the consumer thread blocks on commitSync. p99 listener latency inflates from ~5 ms to ~50 ms. The broker's __consumer_offsets log fills at 100× normal rate; offset-log compaction lag grows; group-rebalance offset fetches start timing out. None of this surfaces as an obvious error — it looks like 'the app is slow' and is usually misdiagnosed as a downstream problem.")
            .whyMatters("People reach for `ack-mode=RECORD` after a duplicate-processing incident, believing it gives them per-record exactly-once. It does not — the listener call and the commit are still separate operations, and a crash between them still produces a duplicate. The actual fix for duplicate-sensitive processing is idempotent handlers (an upsert keyed by the record's key/offset), not synchronous commits. For at-least-once semantics, `BATCH` (the default) is already correct.")
            .build());

    public static final RuleId PRODUCER_CLIENT_DNS_LOOKUP_DEFAULT = register(builder("PRODUCER_CLIENT_DNS_LOOKUP_DEFAULT")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_CLIENT_DNS_LOOKUP_DEFAULT.md")
            .message("client.dns.lookup=default — value deprecated in Kafka 2.6 (KIP-602) and removed in 3.0; only the first A-record IP is used, defeating multi-IP broker DNS.")
            .tagline("`client.dns.lookup=default` was the legacy behavior: resolve the broker hostname, pick the first A record, ignore the rest. KIP-602 deprecated it in Kafka 2.6 and removed it as a valid value in Kafka 3.0. On 3.0+ the client throws ConfigException at startup. On 2.x clients still running this in production, every reconnect hits the same single IP — if that IP is the broker that just rolled, the client never tries the other IPs the DNS round-robin would have given it.")
            .mechanism("Managed Kafka services (Confluent Cloud, MSK, Aiven) return multiple A records for the same broker hostname (one per AZ/instance), expecting the client to try each one when the first fails. With `client.dns.lookup=default`, the client picks only the first record from each lookup. During a broker rolling restart, every client whose first-resolved IP points at the restarting broker enters a reconnect loop against that single IP until the broker is back, instead of failing over to the sibling IPs immediately. The new default since 2.1, `use_all_dns_ips`, resolves all addresses and rotates through them on failure — the broker restart becomes invisible to the client.")
            .impact("In Kafka 3.0+ clients: the app refuses to start (ConfigException: Invalid value default for configuration client.dns.lookup). In 2.x clients still in production: every broker rolling restart produces a 30-60 s producer outage per affected client (until TCP keepalive triggers a reconnect against a fresh DNS resolution). Mixed-version fleets see the same upgrade fail-to-start blast as upstream EOL: nothing logs that this is a deprecated value until the upgrade flips it from 'warning' to 'fatal'.")
            .whyMatters("This is one of the few Kafka config keys whose default value was changed without a migration period — Kafka 2.0 defaulted to `default`, Kafka 2.1+ defaults to `use_all_dns_ips`. Any config file still pinning the old value was carried over from an earlier deployment. Remove the override; the modern default is unambiguously correct.")
            .build());

    public static final RuleId STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_LOW = register(builder("STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_LOW")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_PROBING_REBALANCE_INTERVAL_MS_TOO_LOW.md")
            .message("probing.rebalance.interval.ms below 60 s — Streams triggers a group-wide rebalance every probe to check if standby tasks are caught up; the topology stalls for the rebalance duration each cycle.")
            .tagline("`probing.rebalance.interval.ms` controls how often Streams pings the cluster to see whether a previously-restoring standby task has caught up enough to be promoted to active. The default is 600 000 ms (10 min). Set lower than 60 s, the probe runs constantly and every probe is a real cooperative rebalance — the topology spends more time in PARTITIONS_REVOKED/ASSIGNED transitions than in steady-state processing.")
            .mechanism("After a Streams instance fails, its tasks are reassigned to peers as 'restoring' tasks; the assignor periodically issues a probing rebalance to ask the group 'is anyone now caught up enough on lag to take ownership?'. Each probe is a real rebalance through the group coordinator: PARTITIONS_REVOKED → SyncGroup → PARTITIONS_ASSIGNED. With the default 10 min interval, that costs ~50-200 ms once every 10 min — invisible. With a 10 s interval, the same cost lands every 10 s — 1-2% of processing time on every instance, plus elevated lag, plus pressure on the coordinator broker.")
            .impact("End-to-end latency p99 inflates by the rebalance cost (typically 100-500 ms); RocksDB caches get repeatedly evicted as instances rejoin; the coordinator broker sees an N×(60s/interval) increase in rebalance traffic. Lower-still values (1-5 s) cause the topology to never finish a steady-state window — every record sees the rebalance window straddling its processing.")
            .whyMatters("The default exists because probing rebalances are useful but expensive: 10 min is the right balance between 'standby promotion latency' and 'steady-state overhead'. People lower this thinking it speeds recovery, but actual recovery time is bound by the changelog restore (which is independent of probing), not by the probe cadence. Raise this if anything; never lower it.")
            .build());

    public static final RuleId CONSUMER_INTERCEPTOR_CLASSES_LEGACY = register(builder("CONSUMER_INTERCEPTOR_CLASSES_LEGACY")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("observability")
            .docPath("observability/CONSUMER_INTERCEPTOR_CLASSES_LEGACY.md")
            .message("interceptor.classes wires the legacy Confluent MonitoringConsumerInterceptor/MonitoringProducerInterceptor — Confluent Control Center's deprecated metric pipeline; on managed Kafka with native metrics this just adds latency and a dead network call per record.")
            .tagline("Confluent's MonitoringConsumerInterceptor and MonitoringProducerInterceptor were the original 'Control Center' integration: every consumed/produced record was mirrored to an internal `_confluent-monitoring` topic so Control Center could compute lag and throughput dashboards. On managed Kafka (Confluent Cloud, MSK, Aiven, etc.) — and on Confluent Platform 7+ with the modern Health+ pipeline — this interceptor is dead weight: the monitoring topic doesn't exist, the writes fail silently, every record pays the serialization + queue cost for nothing.")
            .mechanism("Kafka's client builds the interceptor chain from `interceptor.classes` at construction. Each `onConsume()`/`onSend()` callback runs synchronously on the user thread; the Confluent interceptors serialize a `MonitoringMessage` and asynchronously enqueue it to the monitoring topic. If the topic doesn't exist (managed clusters typically don't have it), the interceptor's internal producer logs a NOT_LEADER error every few seconds and the records are lost — but the per-record CPU and allocation cost is still paid on the hot path.")
            .impact("Per-record overhead 5-30 µs depending on key/value size; at 50 000 rec/s that is 250 ms - 1.5 s of CPU per second per client (so 25%-150% of one core, on a hot consumer). Log noise: the interceptor's internal producer logs WARN every 5-30 s about the missing monitoring topic; log volumes climb and on-call gets paged for noise.")
            .whyMatters("This interceptor is from the Confluent Platform 5.x era and was replaced by Confluent Health+ (push-based, no per-record cost). Most code that still wires it is from copy-pasted starter templates or a half-finished migration off Control Center. If the monitoring topic doesn't exist in your cluster, the rule is simple: remove the interceptor entirely. If it does exist, switch to Health+ and remove anyway.")
            .build());

    public static final RuleId SPRING_BOOT_PRODUCER_IDEMPOTENCE_FALSE = register(builder("SPRING_BOOT_PRODUCER_IDEMPOTENCE_FALSE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_PRODUCER_IDEMPOTENCE_FALSE.md")
            .message("spring.kafka.producer.properties.enable.idempotence=false — explicit opt-out of the idempotent producer; reintroduces duplicate / out-of-order writes on retry.")
            .tagline("Setting `enable.idempotence=false` via Spring Boot properties disables the idempotent producer that has been on by default since kafka-clients 3.0. Every retry of a retriable send now risks writing the same record twice or out of order: the producer-id/sequence-number ordering that prevents duplicates is gone.")
            .mechanism("The idempotent producer attaches a producer-id and a monotonically increasing sequence number to every batch. On a retriable error (NotLeaderForPartition, network failure), the producer retries the same batch with the same sequence number; the broker dedupes on producer-id+sequence. Disabling idempotence drops the sequence number — the broker sees the retry as a fresh batch and appends it again, even though the first attempt may have succeeded. The producer also reverts to silent reordering: with `max.in.flight.requests.per.connection > 1`, a retry of an earlier batch can land after a later batch.")
            .impact("Duplicate records are the most common consequence — downstream consumers see the same payload twice, idempotent handlers absorb it but non-idempotent ones (count aggregators, billing, monetary transfers) double-spend. Reordering is the subtler one: state machines reject out-of-order events as 'invalid transition' and the producer's own delivery callback never knows the record landed late.")
            .whyMatters("This setting is almost always carried over from a pre-3.0 Spring Boot starter (Boot 2.x defaulted kafka-clients 2.x, where idempotence was opt-in). The migration path is to delete the line — Boot 3.x with kafka-clients 3.0+ enables idempotence by default at no cost. If the override exists because someone hit `OutOfOrderSequenceException`, the fix is to investigate the underlying cause (e.g. concurrent transactional producers with the same transactional.id), not to disable idempotence.")
            .build());

    public static final RuleId STREAMS_TRANSFORM_DEPRECATED = register(builder("STREAMS_TRANSFORM_DEPRECATED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_TRANSFORM_DEPRECATED.md")
            .message("KStream.transform() is deprecated since Kafka Streams 3.3 (KIP-820) — replaced by KStream.process(ProcessorSupplier) which uses the new ProcessorContext API and supports forwarding multiple records, named children, and proper punctuation lifecycles.")
            .tagline("`KStream.transform(TransformerSupplier, ...)` and its sibling `transformValues` come from the original Streams API (2016). They wrap each record into the legacy `Transformer<K, V, KeyValue<K1, V1>>` interface — one input, exactly one output, no support for forwarding to multiple downstream nodes by name. KIP-820 (Kafka 3.3, Oct 2022) replaced this with `KStream.process(ProcessorSupplier)` using the new `org.apache.kafka.streams.processor.api.Processor`, which can call `context.forward(record, childName)` to fan out, returns `void`, and uses typed `Record<K, V>` instead of `KeyValue`.")
            .mechanism("At runtime both eventually hit `ProcessorContext.forward`, but the deprecated transform wraps the call: it accepts one `KeyValue` returned from the `transform()` method and forwards it (or drops if null) to all downstream nodes; the new `Processor.process(Record)` calls `context.forward(record)` zero, one, or many times to chosen child names. The old API also has subtler bugs — `transform()` returning `null` is the only way to filter out, whereas `process()` simply doesn't forward; punctuator scheduling on the old API exposes the deprecated `PunctuationType.STREAM_TIME` semantics that interact poorly with `MAX_TASK_IDLE_MS`.")
            .impact("Two operational consequences. (1) The Streams 4.x roadmap (now in milestone for late 2026) is expected to remove the deprecated `Transformer*` types entirely; code still on `transform` will fail to compile on the next major. (2) Topologies built with `transform` cannot use the named-child-forwarding optimizations or the typed `Record` improvements introduced over 2023-2024 — refactoring late is mechanical but verbose, and the longer it sits the more `transform` calls accumulate.")
            .whyMatters("Migrate to `KStream.process(ProcessorSupplier)` returning a `Processor<KIn, VIn, KOut, VOut>` (from `org.apache.kafka.streams.processor.api`, NOT the deprecated `org.apache.kafka.streams.processor` package). Replace `return KeyValue.pair(k, v)` with `context.forward(new Record<>(k, v, timestamp))`; replace `return null` with simply not forwarding. The migration is local — no topology rewiring needed since `process()` takes the same store names.")
            .build());

    public static final RuleId STREAMS_TRANSFORM_VALUES_DEPRECATED = register(builder("STREAMS_TRANSFORM_VALUES_DEPRECATED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_TRANSFORM_VALUES_DEPRECATED.md")
            .message("KStream.transformValues() is deprecated since Kafka Streams 3.3 (KIP-820) — replaced by KStream.processValues(FixedKeyProcessorSupplier) which uses the new FixedKeyProcessor API and preserves the key-invariant guarantee required by downstream groupByKey/join operations.")
            .tagline("`transformValues` was the value-only variant of `transform`: by contract it kept the key unchanged so the partition assignment downstream stayed stable — but the old `ValueTransformerWithKey` API let you accidentally return a different key anyway, breaking the invariant silently. KIP-820 (Kafka 3.3) introduced `KStream.processValues(FixedKeyProcessorSupplier)` whose `FixedKeyProcessor` interface gives you `FixedKeyRecord` — a record where the key is genuinely immutable (the type system enforces it).")
            .mechanism("`transformValues` calls the legacy `ValueTransformerWithKey.transform(K, V)` and forwards `KeyValue.pair(originalKey, returnedValue)`. The new `processValues` calls `FixedKeyProcessor.process(FixedKeyRecord)`, which exposes `context.forward(record.withValue(newV))` — the API never gives you access to mutate the key. A downstream `groupByKey()` on the result is then guaranteed safe; the old API only enforced this by convention.")
            .impact("Same as `transform`: deprecated for removal in the Streams 4.x cycle. Additionally, the type-level key-invariant is a real correctness win — code that uses the old API and accidentally returns the wrong key produces silent partition skew that is only detected by inspecting the consumer-group lag distribution. Migrating to `processValues` makes such bugs uncompilable.")
            .whyMatters("Migrate to `KStream.processValues(FixedKeyProcessorSupplier)`. Replace `return newValue` with `context.forward(record.withValue(newValue))`. The store-name argument list is identical. The migration is mechanical and should happen at the same time as the `transform → process` migration.")
            .build());

    public static final RuleId STREAMS_BRANCH_DEPRECATED = register(builder("STREAMS_BRANCH_DEPRECATED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_BRANCH_DEPRECATED.md")
            .message("KStream.branch(Predicate...) is deprecated since Kafka Streams 2.8 (KIP-418) — replaced by KStream.split().branch(...) which uses a fluent builder, supports named branches, and offers a default branch for unmatched records.")
            .tagline("The old `KStream.branch(Predicate...)` returns a `KStream[]` indexed by predicate order. It has no way to name the branches, no default branch for records that match no predicate (they're silently dropped), and the array-index API is a common source of off-by-one bugs when predicates are reordered. KIP-418 (Kafka 2.8, April 2021) introduced `KStream.split(Named).branch(Predicate, Branched.as(name))` which returns a `Map<String, KStream<K, V>>` keyed by branch name and supports a `defaultBranch()` for unmatched records.")
            .mechanism("Both APIs ultimately wire to the same internal `KStreamBranch` processor — the difference is purely in the API surface. The old `branch(p1, p2, p3)` evaluates predicates in order and routes each record to the first matching predicate's child node (or drops it if none match); the new `split().branch(p1, Branched.as(\"hot\")).branch(p2, Branched.as(\"warm\")).defaultBranch(Branched.as(\"cold\"))` does the same routing but returns a named `Map<String, KStream>` and lets you attach a default sink. The dropped-on-no-match behavior of the old API is the source of most production bugs from `branch`: records vanish without an error.")
            .impact("Two concrete bugs the old API enables. (1) Silent drop: any record matching none of the predicates is gone forever and the operator gets no metric for it; the only signal is downstream lag on a topic that should have records. (2) Order coupling: reordering the `Predicate` arguments quietly reroutes records to different branches; a refactor that 'just renames variables' triggers a data-flow incident. The new `split().branch(.., Branched.as(name))` makes the routing intent explicit; reorder-safe.")
            .whyMatters("Migrate `KStream<K,V>[] branches = stream.branch(p1, p2, p3)` to `Map<String, KStream<K,V>> branches = stream.split(Named.as(\"router\")).branch(p1, Branched.as(\"a\")).branch(p2, Branched.as(\"b\")).branch(p3, Branched.as(\"c\")).defaultBranch(Branched.as(\"other\"));`. Each downstream consumer of an old `branches[i]` becomes `branches.get(\"a\")`. Always wire `defaultBranch` even if the body is `.foreach(record -> log.warn(...))` — silent drops are an anti-pattern.")
            .build());

    public static final RuleId CONSUMER_GROUP_INSTANCE_ID_PLACEHOLDER = register(builder("CONSUMER_GROUP_INSTANCE_ID_PLACEHOLDER")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_GROUP_INSTANCE_ID_PLACEHOLDER.md")
            .message("group.instance.id is set to an unresolved ${...} placeholder — every pod claims the same static-membership identity; the second pod that registers fences the first off the group and the deploy thrashes.")
            .tagline("`group.instance.id` (KIP-345 'static membership') is the explicit instance identifier that lets a consumer keep its assignment across short restarts without triggering a rebalance. The whole point is *uniqueness per instance*. A literal `\"${POD_NAME}\"` reaching the coordinator means every pod registers the same static identity; the coordinator treats the second pod's `JoinGroup` as 'the existing instance restarted' and fences the first pod out.")
            .mechanism("The coordinator stores `(group.id, group.instance.id) → memberId`. On rejoin, a static member's previous `memberId` is reused if the same `group.instance.id` shows up within `session.timeout.ms`. If two live pods both ship the literal `\"${POD_NAME}\"`, the coordinator hands the existing `memberId` to whichever pod's `JoinGroup` arrived second; the first pod's heartbeats become unrecognized and it gets `FencedInstanceIdException` (or, on older clients, `UnknownMemberIdException`) on its next poll.")
            .impact("Rolling deploys turn into ping-pong fencing: pod N joins, fences pod N-1; pod N+1 joins, fences pod N. The group never stabilizes; lag rises while pods crash-loop on `FencedInstanceIdException`. Worse, because static membership suppresses rebalances, the group's partition-to-member map gets stuck pointing at the most-recently-fenced pod and the lag never drains.")
            .whyMatters("Resolve the instance id at startup — for Kubernetes StatefulSets use `metadata.name` from the downward API (`POD_NAME` env var → pass the resolved String); for VM fleets use the persistent VM identifier. The literal `\"${POD_NAME}\"` is *worse* than not setting `group.instance.id` at all (which gives dynamic membership, where each member gets a random unique id).")
            .build());

    public static final RuleId SECURITY_PROTOCOL_PLACEHOLDER = register(builder("SECURITY_PROTOCOL_PLACEHOLDER")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/SECURITY_PROTOCOL_PLACEHOLDER.md")
            .message("security.protocol is set to an unresolved ${...} placeholder — Kafka rejects any value not in {PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL}; the client throws ConfigException at construction and crashes at startup.")
            .tagline("`security.protocol` is one of the few Kafka config keys with a fixed enum: only `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` are valid. A literal `\"${KAFKA_SECURITY}\"` is none of those — the validator throws `ConfigException: Invalid value \"${KAFKA_SECURITY}\" for configuration security.protocol`. The pod crash-loops before it can even attempt a connection.")
            .mechanism("Kafka's `CommonClientConfigs.SECURITY_PROTOCOL_CONFIG` definition includes `ConfigDef.ValidString.in(\"PLAINTEXT\", \"SSL\", ...)`. The validator runs during `AbstractConfig.parse()`, before any network code. A literal `\"${...}\"` reaches the validator unchanged from `Properties.put` and fails immediately with a clear error message — but the error message is only clear once you read it; the symptom in production is 'pod fails to start' with no observable Kafka activity to debug.")
            .impact("Pod crash-loops at startup; readiness probe never goes green; the deploy rolls back. The lucky case is that the bug is caught in CI/test. The unlucky case is a config drift where the env var was set on staging and forgotten on prod — the same image deploys cleanly to staging and crash-loops on prod.")
            .whyMatters("Resolve before putting: `System.getenv(\"KAFKA_SECURITY\")`, `@Value(\"${KAFKA_SECURITY}\")`, `ConfigProvider.getConfig().getValue(\"kafka.security\", String.class)`. Defensive: validate the resolved value against the four allowed strings before passing to Kafka, so a missing env var fails with a useful 'KAFKA_SECURITY env var missing' rather than 'Invalid value ${KAFKA_SECURITY}'.")
            .build());

    public static final RuleId KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_HIGH = register(builder("KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_HIGH")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_HIGH.md")
            .message("connections.max.idle.ms above 600 000 ms (10 min) — exceeds the broker's default `connections.max.idle.ms` and any cloud load-balancer / NAT idle timeout; the broker closes the socket while the client believes it is still healthy.")
            .tagline("`connections.max.idle.ms` on the client (default 9 minutes) decides when the client *proactively* closes idle connections. The broker has the same property (default 10 minutes); whichever side hits its budget first closes the connection. When the client side is set higher than the broker side, the broker silently closes the connection on idle and the client keeps it in its connection pool — every next request on that connection fails with `DisconnectException` and has to redial.")
            .mechanism("The Kafka network thread schedules an idle-close timer at `connections.max.idle.ms` from the last network activity. On 'idle close', it sends a TCP FIN. The broker does the same independently. With client=900s and broker=600s, every 10 minutes the broker closes the connection; the client only notices at the next send/fetch, which fails with 'Connection reset by peer' or 'Disconnected before response was received'. The client recovers by redialing — but the request that triggered the discovery is lost (or retried with extra latency).")
            .impact("Periodic spurious 'Disconnected' / 'connection reset' errors at the broker's idle interval. Producer p99 latency develops a periodic spike at the broker-idle cadence (every ~10 min). On clients behind cloud LBs or NAT (AWS NLB idle timeout 350 s; GCP LB 600 s; Azure NAT 4 min default), the same problem shows up at the LB's idle cadence regardless of broker config — and the client's higher idle setting just hides the LB's existence.")
            .whyMatters("Match the client's `connections.max.idle.ms` to the *minimum* of the broker's setting and any intermediary's idle timeout (LB, NAT, proxy). The default 540 000 ms (9 min) was chosen specifically to land below the typical broker 10-min default; raising the client's setting above 10 min only makes sense in a fully-controlled environment where you have also raised the broker side.")
            .build());

    public static final RuleId KAFKA_METRICS_RECORDING_LEVEL_DEBUG = register(builder("KAFKA_METRICS_RECORDING_LEVEL_DEBUG")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.MEDIUM).category("observability")
            .docPath("observability/KAFKA_METRICS_RECORDING_LEVEL_DEBUG.md")
            .message("metrics.recording.level=DEBUG (or TRACE) enables per-thread / per-partition metric updates on every record path; per-record CPU and allocation overhead climbs 10-30% on a busy client.")
            .tagline("`metrics.recording.level` is a knob on the Kafka client's `Metrics` subsystem (the same one that produces records-lag, request-latency, etc.). At the default `INFO` only the coarse counters update on the hot path. At `DEBUG`, fine-grained per-partition / per-broker / per-thread metric updates fire on every poll/send/fetch; at `TRACE`, per-record metrics fire too. None of these are wrong to enable for short-term debugging — they are wrong to leave on in production.")
            .mechanism("Each `recordSensor.record()` call walks the sensor's child sensors and updates atomic counters / histograms. With `INFO`, the per-record path touches ~5 sensors; with `DEBUG`, ~20-50; with `TRACE`, ~100+. At 50 000 rec/s these add up to milliseconds of CPU per second per client — enough to push high-throughput pipelines over their CPU budget. The metrics objects also allocate boxed values on each update; GC pressure climbs measurably.")
            .impact("Throughput drops 5-30% on the affected producer/consumer; allocation rate climbs proportionally; the JIT spends more time inside the Metrics package and less in user code. For Streams topologies (which contain producer + consumer + admin clients all reading the same property), the overhead stacks across all three. End-to-end p99 latency typically increases 10-50%.")
            .whyMatters("`metrics.recording.level=DEBUG` is almost always left in place after a debugging session ('we needed broker-side per-partition stats for one incident; the config never got rolled back'). The fix is to read fine-grained metrics on demand via the existing JMX MBeans (which exist regardless of recording level) rather than turning the hot-path knob on globally. Remove the override; default is correct for production.")
            .build());

    public static final RuleId KAFKA_RECONNECT_BACKOFF_MAX_MS_TOO_LOW = register(builder("KAFKA_RECONNECT_BACKOFF_MAX_MS_TOO_LOW")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_RECONNECT_BACKOFF_MAX_MS_TOO_LOW.md")
            .message("reconnect.backoff.max.ms below 1000 ms — the exponential reconnect backoff is capped before it grows; client reconnects to a down broker faster than the broker can recover, defeating the entire point of backoff.")
            .tagline("`reconnect.backoff.max.ms` is the *cap* of the exponential reconnect backoff that grows from `reconnect.backoff.ms` (default 50 ms) doubling on each failed attempt. The default cap is 1000 ms. Lower the cap below 1 s and the exponential grows for one or two doublings then plateaus — the client hammers the disconnected broker every few hundred milliseconds forever.")
            .mechanism("On TCP connect failure, the Kafka client schedules the next attempt at `min(reconnect.backoff.ms × 2^attempts, reconnect.backoff.max.ms)` with ±20% jitter. With `max.ms=1000`, attempts taper to ~1 s after a few failures. With `max.ms=200`, the cap kicks in at attempt 2-3 and stays at ~200 ms for the entire outage. A 500-pod fleet pointing at a broker that just crashed produces ~2500 TCP-SYN per second against that broker until the broker comes back — which makes the broker take longer to come back.")
            .impact("During broker rolling restarts, the restarting broker's TCP listener gets swamped before it can finish binding; the LISTEN queue overflows and legitimate health-check connections (k8s readiness probe, broker-to-broker controller fetch) fail. The broker takes 5-20× longer to rejoin the cluster than it would with default backoff caps. The same shape repeats on every restart.")
            .whyMatters("People lower this expecting 'faster reconnect after a network blip' — but the *floor* on reconnect is `reconnect.backoff.ms` (50 ms), not `reconnect.backoff.max.ms`. The cap only matters when an outage persists for tens of seconds; lowering it makes prolonged outages worse without speeding up brief ones. Leave at the default.")
            .build());

    public static final RuleId KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MS_TOO_LOW = register(builder("KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MS_TOO_LOW")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MS_TOO_LOW.md")
            .message("socket.connection.setup.timeout.ms below 5000 ms — the TCP+TLS+SASL handshake budget is shorter than a single cross-AZ round-trip; new connections fail before SSL completes on healthy clusters.")
            .tagline("`socket.connection.setup.timeout.ms` (KIP-601, since Kafka 2.7) bounds the *entire* TCP-connect → TLS-handshake → SASL-authenticate sequence. Default is 10 000 ms — chosen to absorb worst-case TLS negotiation (slow handshake on a cold broker), SASL/PLAIN over high-latency cross-region links, and cross-AZ networks with elevated jitter. Drop it to 1-3 s and the client falsely diagnoses healthy brokers as down whenever any handshake step takes longer than the truncated budget.")
            .mechanism("On each new connection the Kafka network thread starts a single deadline timer at this value. The deadline covers: TCP SYN/SYN-ACK (1 RTT), TLS ClientHello / ServerHello / key exchange / Finished (3-4 RTTs for TLS 1.2, 1-2 for TLS 1.3), and SASL handshake (1-2 RTTs depending on mechanism). On a typical cross-AZ link (1-5 ms RTT), the total budget is 30-60 ms — well under 10 s. On a cross-region link (50-200 ms RTT) it can hit 2-4 s. Under load or with a JIT-warming broker, handshake times can spike to 5-8 s without anything being wrong. Drop the timeout below those spikes and connections fail spuriously.")
            .impact("Spurious 'Connection setup failed' / 'Disconnecting before handshake completed' errors during normal operation. The client retries (with `reconnect.backoff.ms` between attempts) but each attempt fails again; the producer's `max.block.ms` eventually expires; `send()` throws TimeoutException; the application sees record-loss with no broker-side root cause visible.")
            .whyMatters("This setting exists for environments where 10 s is genuinely too long — air-gapped LANs with sub-millisecond RTT where you want fast-fail. In any cloud or cross-AZ deployment, 10 s is barely enough. The fix is almost always to remove the override; if you genuinely need fail-fast, leave the cap at the default and tune `request.timeout.ms` instead (which bounds in-flight requests, not handshakes).")
            .build());

    public static final RuleId KAFKA_BOOTSTRAP_SERVERS_PLACEHOLDER = register(builder("KAFKA_BOOTSTRAP_SERVERS_PLACEHOLDER")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/KAFKA_BOOTSTRAP_SERVERS_PLACEHOLDER.md")
            .message("bootstrap.servers contains an unresolved ${...} placeholder — Java does not expand templates in string literals; the client tries to resolve a literal hostname with brace characters and crashes at startup.")
            .tagline("`bootstrap.servers` is parsed by `ClientUtils.parseAndValidateAddresses` at client construction. A literal `\"${KAFKA_BROKERS}\"` reaching that parser is not a template — plain Java has no implicit `${VAR}` resolver. The client either rejects the string outright (ConfigException: 'Invalid url in bootstrap.servers') or accepts it and immediately fails DNS resolution for a hostname literally named `${KAFKA_BROKERS}`.")
            .mechanism("`bootstrap.servers` is the only required client property and the first thing the client validates. With `\"${KAFKA_BROKERS}\"` the parser sees a host:port-shaped string where the host is `${KAFKA_BROKERS}` (or the entire string if there is no `:`). DNS resolution returns NXDOMAIN; the producer's `metadata.fetch.timeout.ms` (default 60 s) expires; the construction call throws `KafkaException: Failed to construct kafka producer` with a chained `TimeoutException` on the cluster metadata fetch.")
            .impact("The application crash-loops at startup. There is no degraded mode — without a valid bootstrap connection, the Kafka client has no path forward. In Kubernetes this means CrashLoopBackOff within seconds of deploy; the readiness probe never goes green.")
            .whyMatters("Read the bootstrap servers from an explicit configuration source (System.getenv, @Value, MicroProfile Config) and pass the resolved String to the Properties bag. The `${...}` syntax in Java string literals is *always* a bug — it has no defined semantics outside of explicit template-engine code. The fix is the same in every framework: resolve before putting.")
            .build());

    public static final RuleId PRODUCER_TRANSACTIONAL_ID_PLACEHOLDER = register(builder("PRODUCER_TRANSACTIONAL_ID_PLACEHOLDER")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_TRANSACTIONAL_ID_PLACEHOLDER.md")
            .message("transactional.id is set to an unresolved ${...} placeholder — every pod gets the same literal transactional.id, immediately fencing all but one out of the transaction coordinator.")
            .tagline("Transactional producers use `transactional.id` as the *fencing identifier*: when two producers register the same `transactional.id` against the same coordinator, the second one's `initTransactions()` succeeds by *fencing* the first — the first producer's subsequent commit throws `ProducerFencedException`. Set every pod's `transactional.id` to the literal string `\"${POD_NAME}\"` and you have created the exact pathological pattern transactional semantics are designed to prevent.")
            .mechanism("On `initTransactions()`, the producer registers `(transactional.id, epoch)` at the transaction coordinator. The coordinator bumps the epoch and fences any in-flight transaction tied to the old epoch. A second pod registering the same literal `\"${POD_NAME}\"` bumps the epoch again, fencing the first pod — and so on for every pod in a rolling-deploy. The fenced pods crash with `ProducerFencedException` on their first commit; the surviving pod is the one to register most recently, which thrashes as the deploy progresses.")
            .impact("During a normal rolling deploy of N pods, on average N-1 of them die with `ProducerFencedException` mid-transaction; in-flight records on those pods are aborted; the application sees a steady stream of transaction-failure exceptions until the deploy stabilizes. Throughput collapses to whatever the single non-fenced pod can produce. Worse — if your deploy is fast (k8s rolling-update), the fencing race never converges and all pods crash.")
            .whyMatters("The `transactional.id` must be stable and unique per producer instance. The pattern is `<service>-<stable-instance-id>` where `<stable-instance-id>` is the StatefulSet ordinal (k8s) or the persistent VM id (cloud). Resolve it from the runtime before construction; if `${POD_NAME}` is what you want, read `System.getenv(\"POD_NAME\")` and pass the result. The literal `\"${POD_NAME}\"` is not a transactional.id — it is one shared fencing token across all pods.")
            .build());

    public static final RuleId STREAMS_APPLICATION_ID_PLACEHOLDER = register(builder("STREAMS_APPLICATION_ID_PLACEHOLDER")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_APPLICATION_ID_PLACEHOLDER.md")
            .message("application.id contains an unresolved ${...} placeholder — Streams uses the literal text as consumer-group id, internal-topic prefix, and EOS transactional.id; every artifact is named after the placeholder and collides across services.")
            .tagline("`application.id` is Streams' single most load-bearing identifier: it is the consumer-group name, the prefix of every changelog / repartition topic (e.g. `${SERVICE_NAME}-orders-store-changelog`), and (with EOS) the `transactional.id` prefix used for fencing. A literal `\"${SERVICE_NAME}\"` reaching Streams creates topics, groups, and transactional ids named after the literal placeholder text — every Streams app shipping this bug shares the same Kafka artifacts.")
            .mechanism("Streams reads `application.id` from `StreamsConfig` and stamps it on: the internal `__consumer_offsets` group entry, every changelog topic name, every repartition topic name, every state-store directory under `state.dir`, and the `transactional.id` of internal producers under EOS. A literal `\"${SERVICE_NAME}\"` reaches all of these — no framework subsequently rewrites it. Two unrelated Streams services both shipping the bug then share the same `__consumer_offsets` entry, the same changelog topics on the broker, and the same EOS transactional fencing token; one app sees the other app's input partitions, reads its state, and fences its transactions.")
            .impact("Worst case is data loss + corruption: the wrong app's state is restored from the wrong changelog; record processing produces wrong outputs; transactional fencing exceptions cascade across both services. Even in the best case where only one app is affected, every restart uses the placeholder-named group — so when the bug is fixed, the new (correctly-named) app starts from `auto.offset.reset` because no offsets exist for it.")
            .whyMatters("Resolve `application.id` at startup time, before constructing the `StreamsConfig` — read `System.getenv(\"SERVICE_NAME\")`, `@Value(\"${SERVICE_NAME}\")`, or `ConfigProvider.getConfig().getValue(\"...\", String.class)`. Never rely on string-literal `${...}` to be expanded by Kafka or by Streams. If `${SERVICE_NAME}` resolves to nothing, fail-fast at startup rather than letting the literal text reach Kafka.")
            .build());

    public static final RuleId CONSUMER_GROUP_ID_PLACEHOLDER = register(builder("CONSUMER_GROUP_ID_PLACEHOLDER")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_GROUP_ID_PLACEHOLDER.md")
            .message("group.id is set to a placeholder string like ${SERVICE_NAME} or {{group}} — a literal Java String never goes through environment-variable or Spring property substitution; the consumer joins the group whose name is literally '${SERVICE_NAME}'.")
            .tagline("A Java string literal `\"${SERVICE_NAME}\"` looks like a template, but plain `Properties.put(..., \"${SERVICE_NAME}\")` from inside a class file is never expanded. There is no implicit resolver for `${...}`, `{{...}}`, `%{...}` outside of explicit frameworks (Spring `@Value` / `Environment.resolvePlaceholders`, Micronaut `${...}`, Quarkus `${...}` in `microprofile-config`); from a raw Java caller, the literal string is what reaches `KafkaConsumer`.")
            .mechanism("The consumer accepts any non-empty string as `group.id`. With `\"${SERVICE_NAME}\"` reaching it, the consumer joins a consumer-group literally named `${SERVICE_NAME}`. Two pods running the same code join the same accidental group, share partition assignments, and corrupt each other's offsets in `__consumer_offsets`. If a different code path is using the correctly-resolved name, the two groups race each other for the same partitions — assignment thrashes between them and the rebalance never converges.")
            .impact("The offending pod 'silently works' for the developer running locally (because local env-var substitution at the shell level might produce a clean string for tests), but in production it leaks data across services that happen to share the unresolved placeholder. Worse, the offsets it commits are bound to the placeholder-name group, so even after the bug is fixed the lag picture is permanently divergent — the next deploy of the fixed pod restarts from `auto.offset.reset` because no offsets exist for the right group.")
            .whyMatters("The fix is one of: (a) inject the resolved value as a constructor / `@Value` field rather than embedding `${...}` in a literal, (b) read it via `System.getenv` / `Environment.getProperty` and pass the resolved String, or (c) hand the entire Properties bag to Spring's `PropertyPlaceholderConfigurer` before constructing the consumer. There is no scenario where shipping the unresolved placeholder is correct.")
            .build());

    public static final RuleId KAFKA_CLIENT_ID_PLACEHOLDER = register(builder("KAFKA_CLIENT_ID_PLACEHOLDER")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("observability")
            .docPath("observability/KAFKA_CLIENT_ID_PLACEHOLDER.md")
            .message("client.id is set to a literal placeholder string like ${POD_NAME} or {{client}} — no resolver runs in plain Java code, so every metric, log line, and broker-side client-quota lookup is keyed by the literal placeholder string.")
            .tagline("`client.id` is the label that flows from this client into JMX metrics, broker `request-log`, `kafka-acls` audit entries, and (most importantly) into per-client-id quota matches. A literal `\"${POD_NAME}\"` reaching the client means every pod shows up under the same `${POD_NAME}` identity — observability, quotas, and ACLs collapse to a single bucket.")
            .mechanism("Plain `Properties.put(\"client.id\", \"${POD_NAME}\")` from a Java class file ships the literal `${POD_NAME}` string to `KafkaProducer`/`KafkaConsumer`. The client uses it verbatim in `request.client.id`, JMX `client-id=` tags, and the broker's `request-log` entries. On clusters with per-client-id quotas (`kafka-configs --add-config 'producer_byte_rate=...' --entity-type clients --entity-name ${POD_NAME}`) the literal string is what matches.")
            .impact("Metrics: per-pod throughput / latency dashboards merge into a single line. Quotas: a quota intended for `pod-a` accidentally throttles every pod that ships the same literal. ACL audit: every operation appears to come from one logical client. The bug is hard to spot because the client still works — it just talks under the wrong name.")
            .whyMatters("Resolve the placeholder before the value reaches `Properties.put`: read `System.getenv(\"POD_NAME\")` (Kubernetes downward API), `@Value(\"${POD_NAME}\")`, or `ConfigProvider.getConfig().getValue(...)`. If the placeholder syntax is intentional (e.g. you want every pod to share a client.id), use a hardcoded constant — `\"${...}\"` is never the right way to express that.")
            .build());

    public static final RuleId STREAMS_MAX_WARMUP_REPLICAS_ZERO = register(builder("STREAMS_MAX_WARMUP_REPLICAS_ZERO")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_MAX_WARMUP_REPLICAS_ZERO.md")
            .message("max.warmup.replicas=0 — Streams will not warm up tasks on new instances before promoting them; on every scale-up the topology stops processing while the new owner restores state from the changelog.")
            .tagline("`max.warmup.replicas` (default 2, since 2.6) caps how many 'extra' tasks Streams may concurrently restore in the background on new instances before promoting them to active owners. Setting it to 0 disables warmup entirely: every scale-up or rebalance starts the new owner cold; the topology must wait for the changelog replay before processing resumes on those partitions.")
            .mechanism("When the assignor decides that task `T` should move from instance A to instance B, with warmup enabled B receives `T` as a *warmup replica*: it consumes the changelog topic in the background until B's local state matches A's. The probing rebalance (controlled by `probing.rebalance.interval.ms`) then promotes B to active and revokes T from A. With `max.warmup.replicas=0`, the assignor cannot create warmup tasks — task T moves to B *cold*, and B's first job is to do the full changelog restore as an active task, blocking record consumption for T's partitions until restore finishes.")
            .impact("On a topology with 1 GB of state per task and a 50 MB/s changelog restore rate, every scale-up freezes that task's partitions for ~20 s. With many tasks moving (large rebalance), the freeze stacks: end-to-end p99 latency climbs by minutes, autoscale events look like outages, and SLAs are missed even though the application logs no errors.")
            .whyMatters("The default `max.warmup.replicas=2` is conservative — it caps the *extra* changelog-consume bandwidth used during rebalance to 2 tasks at a time, so the impact on steady-state throughput is bounded. Setting it to 0 trades a small, hidden cost (extra background consume) for a huge, visible cost (cold restore on every scale event). Raise it carefully if rebalances are slow; never lower it to 0.")
            .build());

    public static final RuleId CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_MIXED = register(builder("CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_MIXED")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_MIXED.md")
            .message("partition.assignment.strategy mixes eager assignors (RangeAssignor/RoundRobinAssignor/StickyAssignor) and the cooperative CooperativeStickyAssignor in the same list — the rebalance protocol assumes a single mode and deadlocks at GroupCoordinatorRequest.")
            .tagline("Mixing an eager assignor (Range/RoundRobin/Sticky) with `CooperativeStickyAssignor` in `partition.assignment.strategy` is not the 'migration path' people sometimes assume it is. The two protocols disagree on a single rebalance round-trip count: eager revokes everything then receives a new assignment in one Join/SyncGroup; cooperative revokes only the partitions that moved across multiple rounds. The coordinator picks the lowest-common assignor between all members; if any member only sends 'eager', the whole group rebalances eagerly even when the leader chose CooperativeSticky — and now some members are in a state the others don't understand.")
            .mechanism("Each consumer ships its full `partition.assignment.strategy` list with `JoinGroupRequest`. The coordinator picks the highest-ranked strategy that is present in every member's list. If the chosen strategy is eager (RangeAssignor / RoundRobinAssignor / StickyAssignor) the rebalance follows the eager protocol; if it is CooperativeSticky, the cooperative protocol. With a mixed list `[RangeAssignor, CooperativeStickyAssignor]` on every member, the coordinator picks RangeAssignor (first match), and the group runs in eager mode forever — defeating the migration. With heterogeneous mixed lists across members (some `[Range, Cooperative]`, some only `[Cooperative]`), the cooperative-only members get assignments that assume incremental revocation while the others assume full revocation; partitions are dropped on the floor mid-rebalance and the JoinGroup loop never converges. The KIP-429 migration procedure is explicit: deploy with both in the list (cooperative second), then on the second deploy remove the eager one.")
            .impact("Either you silently never finish migrating to cooperative rebalances (best case), or you trigger a rebalance storm that pauses consumption for minutes while consumers loop on JoinGroup→SyncGroup→Heartbeat failure→JoinGroup (worst case, especially when the group is mid-deploy). Logs show 'inconsistent member metadata' warnings; the coordinator broker spends elevated CPU on rebalance bookkeeping.")
            .whyMatters("KIP-429 documents a strict two-step migration: in step 1, every member lists `[OldAssignor, CooperativeStickyAssignor]` and the group keeps using OldAssignor; in step 2, every member lists `[CooperativeStickyAssignor]` only and the group switches. Skipping step 2 (or doing step 2 partially) traps the group at the rebalance protocol mismatch. The fix is always 'remove the eager assignor from the list' — never 'add another one'.")
            .build());

    public static final RuleId STREAMS_APPLICATION_SERVER_LOCALHOST = register(builder("STREAMS_APPLICATION_SERVER_LOCALHOST")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_APPLICATION_SERVER_LOCALHOST.md")
            .message("application.server is set to localhost / 127.0.0.1 — interactive queries from other Streams instances cannot reach this host; the cross-instance state-store lookup will time out.")
            .tagline("`application.server=host:port` advertises this Streams instance's interactive-queries HTTP endpoint to peers in the same application.id group. When another instance receives a query for a key whose state lives on this instance, it does an HTTP call to `application.server`. Pointing it at `localhost` (or `127.0.0.1`, or `0.0.0.0`) means the peer dials its own loopback interface — the query reaches the wrong process or times out, and the user sees 'StateStore not available' even though one of the instances does have the data.")
            .mechanism("Streams stores `application.server` in the consumer-group subscription metadata as part of each member's JoinGroup payload. Peers read the field via `KafkaStreams.metadataForKey(...)` and route a query for partition P to whichever instance currently owns P. The metadata is stored verbatim — Streams does not rewrite `localhost` to the advertised hostname — so any peer asking for data on this instance gets a host string that resolves to *itself*, not to this instance.")
            .impact("Interactive-queries failures are silent at the API level: `KafkaStreams.store(...)` succeeds, but the returned proxy issues HTTP calls that hit the wrong process. The user-facing symptom is a stale or empty result, not a clean error. In production, this typically shows up as 'works on a single-node test, breaks on a 3-node cluster'.")
            .whyMatters("`localhost` ends up in `application.server` almost always because a `application.server=localhost:8080` snippet was copied from a single-node sample. The fix is to resolve the advertised address at startup from the pod's downward API / hostname (Kubernetes: `${HOSTNAME}.${SERVICE}.${NAMESPACE}.svc:8080`; VM: `InetAddress.getLocalHost().getHostAddress()`). Leave the property unset entirely if interactive queries are not used.")
            .build());

    public static final RuleId SPRING_BOOT_LISTENER_AUTO_STARTUP_FALSE = register(builder("SPRING_BOOT_LISTENER_AUTO_STARTUP_FALSE")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("spring-kafka")
            .docPath("spring-kafka/SPRING_BOOT_LISTENER_AUTO_STARTUP_FALSE.md")
            .message("spring.kafka.listener.auto-startup=false — all @KafkaListener containers are constructed but never started; no records are consumed until application code explicitly calls KafkaListenerEndpointRegistry.start().")
            .tagline("`spring.kafka.listener.auto-startup` controls whether the `KafkaListenerEndpointRegistry` brings up its message-listener containers when the Spring context finishes refreshing. Default is `true` — containers start as soon as the app is ready. Setting it to `false` leaves every `@KafkaListener` constructed but idle: `subscribe()` is never called, no consumer thread runs, no rebalance happens, no records flow.")
            .mechanism("`KafkaListenerEndpointRegistry` implements `SmartLifecycle`. Its `start()` method walks each registered endpoint and calls `MessageListenerContainer.start()`. With `auto-startup=true`, Spring calls `start()` during context refresh; with `false`, the registry stays in the stopped state. The endpoints are still wired (factory, error handlers, retry topics) but the underlying consumer is never created. Annotation-level `autoStartup=\"true\"` on a specific `@KafkaListener` overrides the global default, so per-listener startup is still possible — but the property-level default is what governs everything that does not override.")
            .impact("In production the symptom is 'records pile up on Kafka, the consumer-group is stuck at lag=N, the app logs show no consumer activity'. There is no exception, no warning — just silence. Operators sometimes lose hours tracing 'connectivity issues' before noticing the listener never started.")
            .whyMatters("This setting exists for tests (start the context without flooding test brokers) and for apps that need to delay consumption until a manual readiness signal (warm caches, run migrations, then start consuming). When it shows up in `application.properties` at runtime, it is almost always a leaked test config. Production should either remove the line or set `true` explicitly.")
            .build());

    public static final RuleId STREAMS_STATESTORE_CACHE_MAX_BYTES_ZERO = register(builder("STREAMS_STATESTORE_CACHE_MAX_BYTES_ZERO")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("kafka-streams")
            .docPath("kafka-streams/STREAMS_STATESTORE_CACHE_MAX_BYTES_ZERO.md")
            .message("statestore.cache.max.bytes=0 — Streams state-store cache disabled via the modern config key; every record update flushes to downstream stores and changelog topics, amplifying write traffic.")
            .tagline("`statestore.cache.max.bytes` (Kafka Streams 3.4+, replaces the deprecated `cache.max.bytes.buffering`) controls how much heap Streams uses to coalesce successive updates to the same key inside each state store. Setting it to 0 disables coalescing entirely: every `put()` in a KTable or aggregation flushes a record downstream, every change writes to the changelog topic, and every change crosses the network. The default 10 MiB shared across stores typically absorbs 80-99% of redundant writes.")
            .mechanism("Streams writes to a state store go through a write-behind cache. Within a commit interval (default 30 s, configurable via commit.interval.ms), repeated puts to the same key in the cache overwrite each other and only the latest value reaches the underlying RocksDB store, the changelog topic, and downstream operators on commit. With cache size 0, that coalescing layer is gone: every `put()` is an immediate RocksDB write + immediate changelog produce + immediate emit downstream. A 10 000-update-per-second stream on 100 hot keys (typically 100 effective writes/s after caching) becomes 10 000 RocksDB writes/s and 10 000 changelog records/s.")
            .impact("RocksDB write amplification jumps 10-100× — disk I/O bandwidth, SST compaction backlog, and L0 stalls all scale with raw write rate. Changelog topic ingest rate scales the same way: at the broker, per-partition write throughput climbs proportionally and replication traffic with it. Downstream operators see record bursts that match the upstream's raw rate rather than the smoothed cache output; flatMap/aggregate fan-out compounds. Net effect: 2-5× CPU, 5-10× disk, 5-20× network on every Streams instance, and the changelog retention bill rises proportionally.")
            .whyMatters("This is the modern incarnation of `cache.max.bytes.buffering=0` after KIP-770 (Kafka 3.4); same trap, new config key. People disable the cache thinking it controls 'when records flush downstream' to get lower latency — but with EOS the commit interval bounds emission latency anyway, and increasing cache size never increases latency past commit.interval.ms. Use commit.interval.ms to tune emit latency; leave statestore.cache.max.bytes at its default unless heap pressure is measured.")
            .build());

    public static final RuleId CONSUMER_GROUP_PROTOCOL_CLASSIC = register(builder("CONSUMER_GROUP_PROTOCOL_CLASSIC")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_GROUP_PROTOCOL_CLASSIC.md")
            .message("group.protocol=classic — pins the consumer to the legacy heartbeat/session-based rebalance protocol; the modern KIP-848 'consumer' protocol moves rebalance state to the broker, removes stop-the-world rebalances, and is GA since Kafka 4.0.")
            .tagline("`group.protocol` selects between the legacy embedded-protocol (`classic`) and the new server-side group protocol (`consumer`, KIP-848, GA in Apache Kafka 4.0). Setting it explicitly to `classic` opts out of the new protocol's advantages: incremental cooperative rebalances handled entirely on the broker (no JoinGroup/SyncGroup round-trip from clients), heartbeat decoupled from poll(), and no global pause when one member joins or leaves.")
            .mechanism("In the classic protocol, every rebalance is a four-phase dance among all group members: the leader receives all subscriptions, computes assignments in JVM code on the client, returns them via SyncGroup; until that completes the whole group is in PAUSED state and poll() returns nothing. Session timeout and heartbeat are tied to poll(): a long processing loop misses heartbeats and triggers a rebalance. The new `consumer` protocol moves the assignor to the broker side, uses a target-assignment / current-assignment reconciliation loop, and runs heartbeat on a background thread independent of poll(); rebalances are incremental and the group never pauses.")
            .impact("On `classic`: every consumer join/leave (auto-scale event, rolling deploy, pod restart) freezes consumption for the entire group for the rebalance duration (typically 100-2000 ms). Long processing loops risk being kicked out of the group as 'failed heartbeat' even when the consumer is healthy. At scale (50+ consumers per group), rebalance time grows superlinearly because the leader computes the entire assignment on a single JVM. On the new `consumer` protocol, the same scale and scaling patterns produce near-zero rebalance pauses and no heartbeat-vs-processing tension.")
            .whyMatters("Most code still pinning `group.protocol=classic` either copied an old config template or set it intentionally during the KIP-848 EA/Preview window (Kafka 3.7-3.9) to avoid a feature still flagged as preview. With 4.0 GA the new protocol is production-ready and the migration is one config change — there is no application-level behavior difference for typical use cases. Remove the override and let the default ('consumer' on 4.0+ clusters, 'classic' on 3.x for compatibility) handle it.")
            .build());

    public static final RuleId KAFKA_ENABLE_METRICS_PUSH_FALSE = register(builder("KAFKA_ENABLE_METRICS_PUSH_FALSE")
            .defaultSeverity(Severity.INFO).confidence(Confidence.MEDIUM).category("observability")
            .docPath("observability/KAFKA_ENABLE_METRICS_PUSH_FALSE.md")
            .message("enable.metrics.push=false — disables KIP-714 client telemetry push; brokers and managed Kafka admins lose visibility into client-side latency, throughput, and error metrics for this client.")
            .tagline("`enable.metrics.push` (KIP-714, Kafka 3.7+) controls whether the client emits its internal Kafka-metrics subsystem (record-send-rate, request-latency-avg, fetch-throttle-time-avg, etc.) to the broker via the new PushTelemetry request. The broker forwards them to the cluster's telemetry pipeline (Confluent Cloud's metrics service, MSK's CloudWatch integration, Strimzi's OpenTelemetry exporter). With it disabled, the broker side has no visibility into how the client is actually performing — only what the client chooses to publish via JMX/OpenTelemetry on its own.")
            .mechanism("On connection establishment, a KIP-714-capable client receives a GetTelemetrySubscriptions response listing which client metrics the broker wants (configurable via cluster-level `client.telemetry.*`). The client then periodically pushes a serialized OpenTelemetry MetricsData payload to the broker via PushTelemetry. With `enable.metrics.push=false`, the client skips the subscription handshake entirely and never sends metrics, even when the cluster has the feature enabled. The opt-out is per-client, so you can have an observable fleet and a single dark client whose tail-latency contribution is invisible to the cluster operators.")
            .impact("On managed Kafka (Confluent Cloud, MSK with KIP-714 enabled): support engineers cannot see this client's latency/throughput/error metrics when troubleshooting an incident — only the client's own application logs and whatever it publishes to its own observability stack. Mean-time-to-recovery on cross-client issues (rebalance storms, slow consumers, throttling) increases proportionally to the fraction of clients running with push disabled.")
            .whyMatters("Two common reasons to flip this off: privacy/compliance worries about client telemetry leaving the org, and a misunderstanding that 'metrics push = double the metrics traffic'. The actual payload is small (a few kilobytes per minute) and includes no application data — only the client's own performance counters. Unless the cluster operator explicitly requested clients to disable it, leaving the default `true` keeps the broker-side observability story coherent.")
            .build());

    public static final RuleId KAFKA_AUTO_INCLUDE_JMX_REPORTER_FALSE = register(builder("KAFKA_AUTO_INCLUDE_JMX_REPORTER_FALSE")
            .defaultSeverity(Severity.WARNING).confidence(Confidence.HIGH).category("observability")
            .docPath("observability/KAFKA_AUTO_INCLUDE_JMX_REPORTER_FALSE.md")
            .message("auto.include.jmx.reporter=false — the built-in JmxReporter is disabled and all client metrics (lag, in-flight, record-send-rate, request-latency) disappear unless a replacement reporter is wired via metric.reporters.")
            .tagline("`auto.include.jmx.reporter=false` turns off the JmxReporter the Kafka client registers by default. The Kafka client emits dozens of metrics through the Metrics subsystem (records-lag-max, records-sent-rate, request-latency-avg, batch-size-avg, ...); they reach observability tooling via reporters. The JmxReporter is always wired by default — disabling it without configuring an alternative drops every client metric on the floor.")
            .mechanism("Each Kafka client builds a `Metrics` instance and registers reporters from two sources: the comma-separated `metric.reporters` config, plus an implicit JmxReporter unless `auto.include.jmx.reporter=false` is set. JmxReporter exposes the metrics as MBeans under `kafka.producer:*`, `kafka.consumer:*`, `kafka.streams:*`. JMX-scraping agents (Prometheus jmx_exporter, Datadog Java agent, Dynatrace OneAgent) read these MBeans and ship them out. Setting the property to false skips the JmxReporter registration; if `metric.reporters` is empty, the metrics still tick internally but nothing exports them.")
            .impact("Consumer-lag dashboards go flat-line; producer-latency alerts never fire; capacity planning loses every per-client signal. The Kafka client itself behaves identically — sends, polls and commits still work — so the regression is invisible from inside the app. Operators usually only notice during the next incident, when they reach for the dashboards and the data isn't there.")
            .whyMatters("This setting is almost always a half-finished migration to push-based metrics. Someone adds an OTLP reporter to `metric.reporters`, sets `auto.include.jmx.reporter=false` to 'avoid double-counting', then realises the OTLP reporter never actually shipped to prod — and the observability gap is the only signal that something went wrong. Keep the JmxReporter on (the default) unless you have verified the replacement reporter end-to-end in the same environment.")
            .build());

    public static final RuleId CONSUMER_AUTO_OFFSET_RESET_INVALID = register(builder("CONSUMER_AUTO_OFFSET_RESET_INVALID")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/CONSUMER_AUTO_OFFSET_RESET_INVALID.md")
            .message("auto.offset.reset set to a value other than earliest/latest/none — the consumer throws ConfigException at startup and the pod crash-loops on first deploy.")
            .tagline("Only `earliest`, `latest`, and `none` are valid values for `auto.offset.reset`. Anything else (typos like `earleist`, `lastest`, `oldest`, `newest`, `beginning`, `end`) is rejected by Kafka's `ConfigDef.ValidString` validator and the consumer fails to construct.")
            .mechanism("During `new KafkaConsumer<>(props)`, the client runs `ConsumerConfig.validate()` which checks `auto.offset.reset` against a hard-coded allow-list. An invalid value triggers `org.apache.kafka.common.config.ConfigException: Invalid value <typo> for configuration auto.offset.reset: String must be one of: latest, earliest, none`. The exception propagates out of the constructor; the pod fails health checks; k8s restarts it on a loop with the same config; the alert page eventually fires on CrashLoopBackOff.")
            .impact("First deploy after the typo: the new pod fails to start, the old pod handles the topic until it's drained, lag balloons. If the typo is introduced via env-var substitution at runtime, the binary that worked yesterday now crash-loops today with no code change visible. Because the error is at consumer construction, no records are processed at all — there is no partial-availability window.")
            .whyMatters("Common typos this rule catches: `earleist`, `latests`, `eariest`, `beginning`, `end`, `oldest`, `newest`. Any of them turns into a 100% outage at first deploy. Catching it at lint time costs one regex; catching it in production costs a page-out.")
            .build());

    public static final RuleId PRODUCER_ACKS_INVALID = register(builder("PRODUCER_ACKS_INVALID")
            .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
            .docPath("kafka-clients/PRODUCER_ACKS_INVALID.md")
            .message("acks set to a value other than 0/1/-1/all — the producer throws ConfigException at construction and the pod crash-loops on first deploy.")
            .tagline("Only `0`, `1`, `-1`, and `all` are valid values for `acks`. Anything else (`true`, `2`, `3`, `yes`, `enabled`, `quorum`) is rejected by Kafka's `ProducerConfig` validator and `new KafkaProducer<>(props)` throws.")
            .mechanism("`acks` is validated by `ProducerConfig.acksValidator()`, which only accepts the four documented values. Any other value triggers `org.apache.kafka.common.config.ConfigException: Invalid value <typo> for configuration acks: Expected value to be one of [all, -1, 0, 1] but received <typo>`. The constructor never returns; no records can be sent; the pod fails liveness.")
            .impact("Pod crash-loops at startup; rolling deploy stalls at the first replica. Because the error is at construction time, no producer-side metrics ever register — observability dashboards stay empty and the only signal is the k8s pod status. A common variant: someone reads 'acks should be all' as 'acks should be true' and ships `acks=true` — the producer never even sends a SYN.")
            .whyMatters("Common typos this rule catches: `acks=true`, `acks=yes`, `acks=enable`, `acks=2`, `acks=quorum`. The producer config validator is unforgiving; catching the typo at lint time prevents a 100% startup-time outage of the next deploy.")
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
