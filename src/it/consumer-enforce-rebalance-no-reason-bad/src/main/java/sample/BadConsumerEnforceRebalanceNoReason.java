package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * RULE: CONSUMER_ENFORCE_REBALANCE_NO_REASON — must fire on EVERY
 * method below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.enforceRebalance()V};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.enforceRebalance()V};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::enforceRebalance} bound to a custom no-arg
 *       SAM ({@code RebalanceTrigger}) and to {@link Runnable};</li>
 *   <li>{@code INVOKEDYNAMIC} capture against the {@link Consumer}
 *       interface-typed receiver (REF_invokeInterface) vs. against
 *       the {@link KafkaConsumer} typed receiver (REF_invokeVirtual)
 *       — both are caught;</li>
 *   <li>{@link Runnable}-wrapped dispatch via
 *       {@link ScheduledExecutorService} and {@link ExecutorService}.</li>
 * </ol>
 *
 * <h2>Why no-reason enforceRebalance is an observability hazard</h2>
 *
 * <p>KIP-735 (Kafka 3.0+) added the {@code enforceRebalance(String
 * reason)} overload precisely because operators investigating "why
 * did this group rebalance N times in M minutes?" were left with no
 * cross-actor attribution: the no-reason overload sends a
 * coordinator-side rebalance request whose log line is identical
 * regardless of caller. With a reason every rebalance becomes
 * self-describing ("autoscaler scale-out svc-payments-prod 21→30",
 * "operator manual rebalance MTR-4831", "graceful shutdown pod=X");
 * without one every rebalance becomes a forensic puzzle.
 */
public final class BadConsumerEnforceRebalanceNoReason {

    @FunctionalInterface
    interface RebalanceTrigger {
        void fire();
    }

    // ===== Direct INVOKEVIRTUAL =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.enforceRebalance()V}. Autoscaler-side
     * rebalance trigger: scale-out adds N consumer instances and the
     * existing instances enforceRebalance() to expedite partition
     * handoff; without a reason the broker-side log cannot
     * distinguish autoscaler-triggered rebalances from any other
     * client-initiated rebalance.
     */
    public void autoscalerScaleOutTrigger(KafkaConsumer<String, String> consumer) {
        consumer.enforceRebalance();
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL. Spring @PreDestroy hook
     * triggers rebalance on shutdown to expedite partition handoff;
     * without a reason the broker cannot distinguish this graceful
     * event from a panic-mode trigger.
     */
    public void preDestroyShutdownTrigger(KafkaConsumer<String, String> consumer) {
        consumer.enforceRebalance();
    }

    // ===== Direct INVOKEINTERFACE =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code Consumer.enforceRebalance()V}. Helper that hides the
     * concrete consumer behind the {@link Consumer} interface —
     * same hazard.
     */
    public void controlPlaneRpcTrigger(Consumer<String, String> consumer) {
        consumer.enforceRebalance();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::enforceRebalance} bound to a
     * custom no-arg SAM. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.enforceRebalance()V}.
     */
    public RebalanceTrigger buildTrigger(KafkaConsumer<String, String> consumer) {
        return consumer::enforceRebalance;
    }

    /**
     * MUST FIRE — same indy capture against the Consumer
     * interface-typed receiver. Handle is REF_invokeInterface; the
     * (owner, name, desc) tuple matches.
     */
    public RebalanceTrigger buildTriggerInterface(Consumer<String, String> consumer) {
        return consumer::enforceRebalance;
    }

    /**
     * MUST FIRE — {@code consumer::enforceRebalance} bound to
     * {@link Runnable}. The SAM {@code void run()} erases to
     * {@code ()V} which matches the no-reason descriptor exactly.
     * Common when triggering periodic rebalances via
     * {@link ScheduledExecutorService}.
     */
    public Runnable buildRunnable(KafkaConsumer<String, String> consumer) {
        return consumer::enforceRebalance;
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code consumer.enforceRebalance()}. The synthetic lambda
     * method's bytecode contains a direct INVOKEVIRTUAL on the
     * no-reason overload — the rule walks all methods including
     * synthetic lambda bodies and fires there. The scheduler context
     * amplifies the hazard: a periodic rebalance trigger with no
     * reason makes the broker-side rebalance counter unusable as a
     * signal.
     */
    public ScheduledFuture<?> scheduleEnforceRebalanceFixedRate(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer) {
        return scheduler.scheduleAtFixedRate(
                () -> consumer.enforceRebalance(), 0, 5, TimeUnit.MINUTES);
    }

    /**
     * MUST FIRE — Runnable method-reference captured into a local
     * binding and submitted to an {@link ExecutorService}. The indy
     * site itself targets {@code Consumer.enforceRebalance()V}.
     */
    public Future<?> submitTrigger(
            ExecutorService executor,
            Consumer<String, String> consumer) {
        Runnable trigger = consumer::enforceRebalance;
        return executor.submit(trigger);
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerEnforceRebalanceNoReason().autoscalerScaleOutTrigger(c);
        }
    }
}
