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
