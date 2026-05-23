package sample;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * RULE: ADMIN_NOT_CLOSED.
 *
 * <p>Fires when a method calls {@code Admin.create(...)} (or the legacy
 * {@code AdminClient.create(...)}), ASTOREs the result into a local
 * variable, performs at least one operation through that variable, and
 * never calls {@code close()} on the same slot, AND does not let the
 * client escape via PUTFIELD / PUTSTATIC / ARETURN.
 *
 * <p>Symmetric to {@link sample.BadProducerNotClosed}-style rules. The
 * only mechanical difference is that an AdminClient is constructed by
 * an INVOKESTATIC factory ({@code Admin.create(Properties)} returns
 * {@code Admin}), not by an INVOKESPECIAL constructor. The data-flow
 * analysis is otherwise identical.
 *
 * <p>Why an unclosed AdminClient is a real bug — what AdminClient
 * actually holds:
 * <ol>
 *   <li>AdminClient owns a dedicated, NON-DAEMON background thread —
 *       the {@code AdminClientRunnable}. This thread is the central
 *       I/O loop: every {@code listTopics()} / {@code describeCluster()}
 *       / {@code createTopics()} / etc. enqueues an internal "call"
 *       and waits for the runnable to drain its queue and complete
 *       the returned {@code KafkaFuture}.</li>
 *   <li>Being non-daemon is intentional — admin operations are usually
 *       part of a deployment flow (Terraform provider, Helm hook,
 *       provisioning script) where you do NOT want the JVM to exit
 *       while an in-flight describe/create is still racing the broker.
 *       But the non-daemon-ness means the thread keeps the JVM ALIVE
 *       on its own. A script that creates one AdminClient, does its
 *       work, and forgets to close()  will visibly HANG at the end
 *       of main() — the JVM cannot exit because the
 *       AdminClientRunnable refuses to die without an explicit
 *       close().</li>
 *   <li>The runnable holds a strong reference back to the AdminClient
 *       instance (it has to — it needs the network client, the
 *       metadata cache, the metric registry). The JVM GC cannot
 *       reclaim either the client OR the runnable until the client
 *       is closed. This is invisible in heap dumps unless you know
 *       the relationship: the client looks "dead" from application
 *       code's perspective (no references), but the runnable thread
 *       roots both objects.</li>
 *   <li>The broker side keeps the AdminClient's TCP connections in
 *       its connection table until the half-closed sockets are
 *       reaped by the OS. On a busy CI fleet that runs hundreds of
 *       admin scripts a day, this is observable as steady-state
 *       broker connection-count drift upward.</li>
 *   <li>The metric registry the client owns is NOT shared across
 *       instances. Every leaked client adds another tree of MBeans
 *       under {@code kafka.admin.client:type=admin-client-metrics,...}
 *       — JMX consumers see duplicated, slowly-drifting metric trees
 *       in long-lived JVMs (an operator that spawns transient admin
 *       clients).</li>
 * </ol>
 *
 * <p>What the rule catches (per-method, local-slot data flow):
 * <ol>
 *   <li>Track every {@code INVOKESTATIC Admin.create(...)} (or the
 *       legacy {@code AdminClient.create(...)}) followed by
 *       {@code ASTORE N}. Record N.</li>
 *   <li>For every method call whose receiver resolves to slot N:
 *       <ul>
 *         <li>If the method name starts with {@code "close"}, mark
 *             slot N CLOSED.</li>
 *         <li>Otherwise (listTopics, createTopics, describeCluster,
 *             etc.), mark slot N USED.</li>
 *       </ul></li>
 *   <li>Detect escape: any {@code PUTFIELD} / {@code PUTSTATIC} /
 *       {@code ARETURN} immediately preceded by an {@code ALOAD N}
 *       marks the slot ESCAPED — caller / containing object owns
 *       the close.</li>
 *   <li>Fire only when slot was USED, was NOT CLOSED, and did NOT
 *       ESCAPE. Fire site is the {@code Admin.create} line.</li>
 * </ol>
 *
 * <p>This Bad class triggers TWO fires — two methods that create + use
 * + leak an AdminClient in different ways:
 * <ol>
 *   <li>{@code listAndForget}: listTopics() then return. The JVM will
 *       hang on shutdown because the AdminClientRunnable refuses to
 *       die.</li>
 *   <li>{@code createTopicAndReturn}: createTopics() then return —
 *       returns void (no escape), the operation is enqueued, the
 *       runnable processes it, and then it sits there idle holding
 *       the JVM open.</li>
 * </ol>
 */
public final class BadAdminNotClosed {

    /** Anti-pattern: create + listTopics + return. The non-daemon runnable keeps the JVM alive. */
    public void listAndForget() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props); // reported
        admin.listTopics();
        // no close() — AdminClientRunnable refuses to die, JVM hangs at shutdown
    }

    /** Anti-pattern: create + createTopics + return. Same leak, different operation. */
    public void createTopicAndReturn() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props); // reported
        admin.createTopics(Collections.singletonList(new NewTopic("orders-events", 12, (short) 3)));
        // no close() — broker side still has our connection, JMX still has our MBean tree
    }

    private static Properties baseAdminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-admin-not-closed");
        return p;
    }
}
