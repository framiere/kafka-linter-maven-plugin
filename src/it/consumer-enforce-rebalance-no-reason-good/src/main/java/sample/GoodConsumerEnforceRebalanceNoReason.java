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
 * RULE: CONSUMER_ENFORCE_REBALANCE_NO_REASON — must NOT fire on any
 * method below.
 *
 * <p>Mirror of {@code BadConsumerEnforceRebalanceNoReason} where every
 * call site uses the reason-carrying overload
 * {@code enforceRebalance(String)V} (descriptor
 * {@code (Ljava/lang/String;)V}) — does not match the rule's
 * no-reason descriptor.
 */
public final class GoodConsumerEnforceRebalanceNoReason {

    @FunctionalInterface
    interface RebalanceTrigger {
        void fire();
    }

    private static final String AUTOSCALER_REASON = "autoscaler scale-out svc-payments-prod 21→30";
    private static final String SHUTDOWN_REASON = "graceful shutdown pod=svc-payments-7";
    private static final String CONTROL_PLANE_REASON = "control-plane req=abc123 svc=foo";

    public void autoscalerScaleOutTriggerBounded(KafkaConsumer<String, String> consumer) {
        consumer.enforceRebalance(AUTOSCALER_REASON);
    }

    public void preDestroyShutdownTriggerBounded(KafkaConsumer<String, String> consumer) {
        consumer.enforceRebalance(SHUTDOWN_REASON);
    }

    public void controlPlaneRpcTriggerBounded(Consumer<String, String> consumer) {
        consumer.enforceRebalance(CONTROL_PLANE_REASON);
    }

    public RebalanceTrigger buildTriggerBounded(KafkaConsumer<String, String> consumer) {
        return () -> consumer.enforceRebalance(AUTOSCALER_REASON);
    }

    public RebalanceTrigger buildTriggerInterfaceBounded(Consumer<String, String> consumer) {
        return () -> consumer.enforceRebalance(CONTROL_PLANE_REASON);
    }

    public Runnable buildRunnableBounded(KafkaConsumer<String, String> consumer) {
        return () -> consumer.enforceRebalance(SHUTDOWN_REASON);
    }

    public ScheduledFuture<?> scheduleEnforceRebalanceFixedRateBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer) {
        return scheduler.scheduleAtFixedRate(
                () -> consumer.enforceRebalance("periodic-trigger sidecar"),
                0, 5, TimeUnit.MINUTES);
    }

    public Future<?> submitTriggerBounded(
            ExecutorService executor,
            Consumer<String, String> consumer) {
        Runnable trigger = () -> consumer.enforceRebalance(CONTROL_PLANE_REASON);
        return executor.submit(trigger);
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerEnforceRebalanceNoReason().autoscalerScaleOutTriggerBounded(c);
        }
    }
}
