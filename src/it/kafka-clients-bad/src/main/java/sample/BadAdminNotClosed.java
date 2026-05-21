package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Properties;

/**
 * RULE: ADMIN_NOT_CLOSED.
 *
 * AdminClient is heavyweight: it spawns a non-daemon background thread
 * ({@code kafka-admin-client-thread | <client-id>}), opens TCP connections to brokers,
 * registers a Selector for non-blocking IO, allocates request buffers, and publishes a
 * metric registry. {@code close()} is the only path that signals the background thread to
 * exit, drains in-flight requests, and unregisters resources. Without it, the JVM holds
 * everything until process exit — and the non-daemon thread blocks orderly shutdown.
 *
 * Each method below constructs an admin, uses it, then returns without closing — the
 * rule must fire once per construction site.
 *
 * The trailing {@code cleanCase} method is a control: try-with-resources is the canonical
 * shape; the synthetic close() emitted by javac must satisfy the rule, so it must NOT
 * fire.
 */
public final class BadAdminNotClosed {

    private static Properties props() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "broker:9092");
        return p;
    }

    public void factoryAdminViaInterface() {
        Admin admin = Admin.create(props());
        admin.listTopics();  // used, never closed — FIRES at the Admin.create() line
    }

    public void factoryAdminViaAdminClient() {
        AdminClient admin = AdminClient.create(props());
        admin.describeCluster();  // used, never closed — FIRES at the AdminClient.create() line
    }

    public void usedForCreateTopicsNotClosed() {
        Admin admin = Admin.create(props());
        admin.createTopics(List.of(new NewTopic("events", 6, (short) 3)));  // used, never closed — FIRES
    }

    public void cleanCase() {
        try (Admin admin = Admin.create(props())) {
            admin.listTopics();
        }  // try-with-resources => synthetic admin.close() in finally — must NOT fire
    }
}
