package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.KafkaFuture;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * RULE: ADMIN_DESCRIBE_LOG_DIRS_RESULT_LEGACY_DEPRECATED — must NOT fire.
 *
 * <p>The four methods below exercise the supported public-API accessors
 * introduced by KIP-743:
 *
 * <ol>
 *   <li>{@code descriptions()} direct call — per-broker Map of public-API
 *       {@link LogDirDescription}.</li>
 *   <li>{@code allDescriptions()} direct call — KafkaFuture<Map<...>> of
 *       public-API {@code LogDirDescription}.</li>
 *   <li>{@code result::descriptions} method-ref capture into Supplier.</li>
 *   <li>{@code result::allDescriptions} method-ref capture into Supplier.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>{@link LogDirDescription} is in the public
 * {@code org.apache.kafka.clients.admin} package with documented,
 * version-stable getter contracts: {@code totalBytes()},
 * {@code usableBytes()}, {@code replicaInfos()}, {@code error()}.
 * Capacity-monitoring jobs and rolling-restart preflight checks built
 * on these getters see the actual on-disk capacity and survive Kafka
 * client version upgrades. The rule's INVOKEDYNAMIC walk inspects
 * each bsmArg handle's (owner, name) pair against the deprecated set
 * {values, all} on owner DescribeLogDirsResult — the names
 * {@code descriptions} and {@code allDescriptions} are not in that
 * set, so neither direct calls nor method-ref captures fire here.
 */
public final class GoodDescribeLogDirsResultPublicApi {

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "good-describe-log-dirs-public");
        return p;
    }

    public Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> askDescriptions() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeLogDirsResult result = admin.describeLogDirs(List.of(0, 1, 2));
            // DOES NOT FIRE — supported public-API accessor. The per-broker
            // KafkaFuture wrapping is preserved (so call sites can compose
            // per-broker timeouts); LogDirDescription exposes the stable
            // getters (totalBytes, usableBytes, replicaInfos, error) added
            // by KIP-630/859.
            return result.descriptions();
        }
    }

    public KafkaFuture<Map<Integer, Map<String, LogDirDescription>>> askAllDescriptions() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeLogDirsResult result = admin.describeLogDirs(List.of(0, 1, 2));
            // DOES NOT FIRE — supported KafkaFuture<Map> accessor returning
            // the same public-API LogDirDescription. Use this when you want
            // a single Future to compose with downstream capacity checks.
            return result.allDescriptions();
        }
    }

    public Supplier<Map<Integer, KafkaFuture<Map<String, LogDirDescription>>>> capturedDescriptions() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeLogDirsResult result = admin.describeLogDirs(List.of(0));
            // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture targeting a
            // SUPPORTED accessor. The bsmArg handle's name is "descriptions",
            // not in the LEGACY_NAMES set the rule matches, so the walk
            // rejects this site.
            return result::descriptions;
        }
    }

    public Supplier<KafkaFuture<Map<Integer, Map<String, LogDirDescription>>>> capturedAllDescriptions() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeLogDirsResult result = admin.describeLogDirs(List.of(0));
            // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture targeting the
            // KafkaFuture-returning supported accessor.
            return result::allDescriptions;
        }
    }
}
