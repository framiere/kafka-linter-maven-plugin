package sample;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;

/**
 * Control for ADMIN_NOT_CLOSED.
 *
 * <p>Four methods, each constructs an {@code Admin} into a local slot
 * and avoids firing the rule by satisfying one of the four exits the
 * rule supports — same structure as {@link sample.GoodProducerClosed}.
 *
 * <ol>
 *   <li>{@code tryWithResources} — javac emits a synthetic {@code close()}
 *       in the generated finally region. The rule sees the close call
 *       on slot N. Slot CLOSED. Silent for ADMIN_NOT_CLOSED. Note: the
 *       synthetic close is the NO-ARG overload, which is what would
 *       trigger ADMIN_CLOSE_NO_TIMEOUT — that rule is silenced in this
 *       fixture's pom because it is orthogonal to what we are
 *       exercising here.</li>
 *   <li>{@code explicitFinallyClose} — manual try/finally with
 *       {@code admin.close(Duration.ofSeconds(30))}. This is the
 *       PRODUCTION pattern: the bounded overload guarantees that
 *       close() will return within the deadline even if a broker is
 *       unreachable. The rule still recognizes any method whose name
 *       starts with {@code "close"} as a close call (regardless of
 *       descriptor), so slot CLOSED. Silent.</li>
 *   <li>{@code returnsTheClient} — slot is USED (listTopics) then
 *       ALOAD N + ARETURN. The escape detector marks slot ESCAPED.
 *       Silent. Pattern: a factory method that hands ownership to
 *       the caller.</li>
 *   <li>{@code stashesIntoField} — escape via PUTFIELD onto a
 *       containing object. Containing class is expected to close
 *       the admin client in its own close()/@PreDestroy/etc.
 *       Pattern: long-lived admin client owned by a Spring bean
 *       or a Helm operator class.</li>
 * </ol>
 */
public final class GoodAdminClosed {

    /** Stashed-into-field admin for case 4. */
    private Admin ownedAdmin;

    /** Try-with-resources: synthetic close() in the generated finally. Slot CLOSED. */
    public void tryWithResources() {
        Properties props = baseAdminProps();
        try (Admin admin = Admin.create(props)) {
            admin.listTopics();
        } // synthetic close() — slot CLOSED, silent for ADMIN_NOT_CLOSED
    }

    /** Production-grade close(Duration). The bounded overload is what real shutdown code uses. */
    public void explicitFinallyClose() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        try {
            admin.listTopics();
        } finally {
            admin.close(Duration.ofSeconds(30)); // slot CLOSED, silent
        }
    }

    /** Escape via ARETURN: caller takes ownership. */
    public Admin returnsTheClient() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.listTopics();
        return admin; // ALOAD N + ARETURN — slot ESCAPED, silent
    }

    /** Escape via PUTFIELD: containing object owns the admin client's lifecycle. */
    public void stashesIntoField() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.listTopics();
        this.ownedAdmin = admin; // ALOAD this; ALOAD N; PUTFIELD — slot ESCAPED, silent
    }

    private static Properties baseAdminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "good-admin-closed");
        return p;
    }
}
