package sample;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * RULE: CONSUMER_NO_WAKEUP_SHUTDOWN.
 *
 * <p>Fires when a class contains a {@code KafkaConsumer.poll(...)}
 * call INSIDE A LOOP (or inside an iterating-lambda body) AND no
 * method anywhere in that class calls {@code consumer.wakeup()}
 * (and no method is annotated {@code @KafkaListener}).
 *
 * <p>Why a poll-without-wakeup loop is a real bug — what
 * shutdown actually does:
 * <ol>
 *   <li>{@code KafkaConsumer.poll(Duration)} is BLOCKING. The
 *       polling thread sleeps on the consumer's network select loop
 *       until either records arrive OR the duration elapses. The
 *       duration is usually generous (1 s, 5 s, 30 s — whatever
 *       balances latency vs CPU).</li>
 *   <li>The consumer is NOT thread-safe. The only API that is safe
 *       to call from a different thread than the polling thread is
 *       {@code wakeup()}. wakeup() does ONE thing: it sets an
 *       internal flag that the next call into the network selector
 *       checks. The selector returns immediately and the in-flight
 *       poll() throws {@code WakeupException}.</li>
 *   <li>Without wakeup(), the polling thread is unreachable from
 *       outside. Setting a volatile boolean {@code running = false}
 *       is checked between polls but does NOT interrupt the
 *       CURRENTLY-IN-FLIGHT poll() — the thread sleeps in the
 *       network selector for up to the full poll duration.</li>
 *   <li>JVM shutdown sequence without wakeup():
 *       <ul>
 *         <li>SIGTERM arrives. JVM enters shutdown.</li>
 *         <li>Non-daemon application threads start unwinding.</li>
 *         <li>The polling thread is sitting in poll(Duration.ofSeconds(30)).
 *             The shutdown hook would like to call close() — but
 *             close() must run on a thread that holds the consumer
 *             monitor, and the polling thread has it.</li>
 *         <li>The shutdown hook either: (a) tries to call close()
 *             from another thread and throws
 *             ConcurrentModificationException (the consumer's
 *             multi-threaded-use detector), or (b) sets a boolean
 *             and waits for the polling thread, which is asleep
 *             for the next 30 seconds.</li>
 *         <li>If the orchestrator's terminationGracePeriodSeconds
 *             is shorter than the poll duration (k8s default 30 s,
 *             very common), the JVM is hard-killed. No close() ran.
 *             No LeaveGroup was sent.</li>
 *         <li>The group coordinator only finds out the member is
 *             gone after session.timeout.ms (default 45 s in 3.x).
 *             For that ENTIRE window, the partitions this consumer
 *             owned go unpolled — consumer lag grows linearly.</li>
 *       </ul></li>
 *   <li>With wakeup(): SIGTERM hook calls consumer.wakeup() →
 *       poll() throws WakeupException immediately → finally block
 *       runs close() → close() sends LeaveGroupRequest → coordinator
 *       triggers a rebalance in MILLISECONDS. Total downtime: a
 *       few hundred ms instead of 45 s.</li>
 *   <li>For a static-membership consumer
 *       ({@code group.instance.id} set), the picture is worse:
 *       static members are designed NOT to trigger a rebalance on
 *       restart, so the coordinator waits the FULL
 *       session.timeout.ms before deciding the member is really
 *       gone. Without wakeup() that 45 s blackout happens on every
 *       redeploy.</li>
 * </ol>
 *
 * <p>What the rule catches (class-scope check):
 * <ol>
 *   <li>Pre-pass: scan every method in the class for ANY
 *       {@code MethodInsnNode} whose owner is the Consumer
 *       interface or KafkaConsumer class AND whose method name is
 *       {@code "wakeup"}. If found → class is safe, no fires.</li>
 *   <li>Pre-pass: scan every method for the
 *       {@code Lorg/springframework/kafka/annotation/KafkaListener;}
 *       annotation. If found → consumer is framework-managed,
 *       Spring's container owns the lifecycle, no fires.</li>
 *   <li>If neither suppression applies, scan every method body for
 *       {@code Consumer.poll(...)} calls. For each, ask the
 *       {@code RuleContext} whether the call is inside a loop body
 *       or an iterating-lambda body (covers
 *       {@code while/for/do-while/forEach}). If yes — fire on the
 *       poll() instruction.</li>
 *   <li>Conservative: a poll() call NOT in a loop (one-shot fetch)
 *       does not fire — the class doesn't need wakeup() to interrupt
 *       a one-shot.</li>
 * </ol>
 *
 * <p>This Bad class triggers ONE fire — one poll() in a while-loop
 * with no wakeup() and no @KafkaListener anywhere.
 */
public final class BadConsumerNoWakeup {

    private volatile boolean running = true;

    /** Anti-pattern: blocking poll in a while-loop with no wakeup() escape hatch. */
    public void runForever(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Collections.singletonList("orders"));
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30)); // reported
            records.forEach(r -> {
                // process record (omitted)
            });
        }
        consumer.close();
        // shutdown() (below) sets running=false, but the in-flight poll(30s) won't notice for up to 30s
    }

    /** "Shutdown" hook that flips the flag. Does NOT call wakeup(). Useless for interrupting an in-flight poll. */
    public void shutdown() {
        running = false;
    }
}
