package sample;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * RULE: CONSUMER_NOT_CLOSED.
 *
 * <p>Symmetric counterpart to {@code PRODUCER_NOT_CLOSED}. Fires when
 * a method constructs a {@code KafkaConsumer} into a local variable,
 * uses it (any non-close method call on the same slot — subscribe,
 * assign, poll, commitSync, position, seekToBeginning, etc.), never
 * calls {@code close()} on that slot, and does not let the consumer
 * escape (no ARETURN, no PUTFIELD, no PUTSTATIC).
 *
 * <p>Why a missing close() on a consumer is a real bug — what the
 * Coordinator actually does:
 * <ol>
 *   <li>A consumer holds a long-lived TCP connection to ONE specific
 *       broker — the GroupCoordinator for its consumer group. The
 *       coordinator is the broker responsible for membership, offset
 *       commits, and partition assignment for the group.</li>
 *   <li>The consumer is identified to the coordinator by a member id,
 *       which is the (group.id, member.id) tuple. The coordinator
 *       considers the member ALIVE as long as it sees a heartbeat
 *       every {@code session.timeout.ms} (default 45 s in 3.x).</li>
 *   <li>{@code close()} sends an explicit {@code LeaveGroupRequest}
 *       to the coordinator. The coordinator removes the member from
 *       the group IMMEDIATELY and triggers a rebalance for the
 *       remaining members. Total downtime for the partitions this
 *       consumer owned: a few hundred ms.</li>
 *   <li>Without close(), the JVM exits and the TCP connection
 *       half-closes. The coordinator does NOT find out. It keeps
 *       the member in the group until {@code session.timeout.ms}
 *       elapses with no heartbeat. During that ENTIRE window:
 *       <ul>
 *         <li>The partitions owned by the dead consumer are NOT
 *             reassigned to anyone else. Records sitting on those
 *             partitions are not consumed → consumer lag grows
 *             linearly with time.</li>
 *         <li>Any commit the dead consumer made just before exit
 *             may not have been persisted (offsets are
 *             written to {@code __consumer_offsets} via an
 *             asynchronous fetcher). The next owner of the
 *             partition reads stale offsets and replays records,
 *             OR jumps forward past records the dead consumer was
 *             working on but hadn't committed yet.</li>
 *       </ul></li>
 *   <li>For a static-membership consumer (set
 *       {@code group.instance.id}) the situation is worse: static
 *       members are designed to survive restarts WITHOUT triggering
 *       a rebalance, so the coordinator waits the FULL
 *       {@code session.timeout.ms} (default 45 s) before deciding
 *       the member is really gone. If your deployment cycles
 *       containers in seconds, every redeploy creates a 45-second
 *       blind spot per partition.</li>
 * </ol>
 *
 * <p>Why a leaked consumer slot is worse than a leaked producer
 * slot in one specific way: the producer's failure mode (silent
 * record drop on the way OUT of the JVM) is bad but bounded to
 * records still in the in-memory accumulator. The consumer's
 * failure mode is unbounded in time — every record that arrives on
 * the orphaned partitions while the coordinator waits out
 * session.timeout.ms is delayed. If consumer.poll() was the only
 * reader, that's pure consumer lag the dashboards will see.
 *
 * <p>What the rule catches (per-method, local-slot data flow,
 * mirrors PRODUCER_NOT_CLOSED):
 * <ol>
 *   <li>Track every {@code new KafkaConsumer(...)} that ASTOREs
 *       into a local slot. Record the slot number.</li>
 *   <li>For every method call whose receiver resolves to a tracked
 *       slot:
 *       <ul>
 *         <li>If the method name starts with {@code "close"}, mark
 *             the slot CLOSED.</li>
 *         <li>Otherwise (subscribe / assign / poll / commitSync /
 *             commitAsync / position / seek* / endOffsets / etc.),
 *             mark the slot USED.</li>
 *       </ul></li>
 *   <li>Detect escape: any {@code PUTFIELD} / {@code PUTSTATIC} /
 *       {@code ARETURN} immediately preceded by an {@code ALOAD N}
 *       that points to a tracked slot marks the slot ESCAPED. The
 *       rule does not fire on escaped slots — the assumption is
 *       that another method (a {@code @PreDestroy}, a shutdown
 *       hook, a wakeup-driven cleanup) will close it.</li>
 *   <li>Try-with-resources is handled transparently: javac emits a
 *       synthetic {@code close()} in the generated finally region,
 *       which step 2 picks up as a normal close.</li>
 *   <li>Fire only when slot was USED, was NOT CLOSED, and did NOT
 *       ESCAPE. The fire site is the constructor line.</li>
 * </ol>
 *
 * <p>This Bad class triggers TWO fires — two methods that construct
 * + use + leak a consumer in different ways:
 * <ol>
 *   <li>{@code pollAndForget}: subscribe(), poll(), return. The
 *       member id stays in the group until session.timeout.ms
 *       elapses.</li>
 *   <li>{@code commitButNeverClose}: subscribe(), poll(),
 *       commitSync() — commit went out, but no LeaveGroupRequest.
 *       The group still treats this member as live.</li>
 * </ol>
 */
public final class BadConsumerNotClosed {

    /** Anti-pattern: subscribe + poll + return. No LeaveGroupRequest sent. Coordinator waits session.timeout.ms. */
    public void pollAndForget() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props); // reported
        consumer.subscribe(Collections.singletonList("orders"));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        records.forEach(r -> {
            // process record (omitted)
        });
        // no close() — partitions owned by this consumer go cold until session.timeout.ms expires
    }

    /** Anti-pattern: commit went through but LeaveGroupRequest never did. Still leaks the member slot. */
    public void commitButNeverClose() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props); // reported
        consumer.subscribe(Collections.singletonList("orders"));
        consumer.poll(Duration.ofMillis(100));
        consumer.commitSync();
        // commit persisted, BUT no close() — the coordinator does not know we left.
        // Until session.timeout.ms (default 45 s) elapses, our partitions are orphaned.
    }

    private static Properties baseConsumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-consumer-not-closed");
        p.put("group.id", "orders-processor");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
