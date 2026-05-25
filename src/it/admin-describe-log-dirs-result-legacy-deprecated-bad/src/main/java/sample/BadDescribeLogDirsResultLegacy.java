package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * RULE: ADMIN_DESCRIBE_LOG_DIRS_RESULT_LEGACY_DEPRECATED.
 *
 * <p>Exercises four shapes the rule must catch:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on {@code DescribeLogDirsResult.values()}
 *       — the legacy per-broker Map accessor whose value-type bottoms out in
 *       the internal {@code common.requests.DescribeLogDirsResponse$LogDirInfo}.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on {@code DescribeLogDirsResult.all()}
 *       — the legacy KafkaFuture<Map<...>> accessor with the same internal
 *       value type.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture {@code result::values}
 *       into a {@code Supplier<Map>}. User-class bytecode contains ZERO
 *       {@code INVOKE*} targeting {@code values}; the call lives in the
 *       {@code LambdaMetafactory}-synthesized bridge.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture {@code result::all} into
 *       a {@code Supplier<KafkaFuture>}.</li>
 * </ol>
 *
 * <h2>Why these accessors are dangerous</h2>
 *
 * <p>{@code values()} and {@code all()} expose
 * {@code org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo}
 * — a wire-protocol class in the non-public {@code common.requests}
 * package whose field layout is changed in-place between minor Kafka
 * releases when the wire-format grows. Code compiled against one
 * client version can fail to link against another, and the newer
 * public fields ({@code totalBytes}, {@code usableBytes} from KIP-630;
 * {@code futureReplica} fields from KIP-859) are simply ABSENT from
 * {@code LogDirInfo} — they live on the public-API
 * {@code LogDirDescription} type that only the supported
 * {@code descriptions()} / {@code allDescriptions()} accessors return.
 * Disk-pressure monitoring and rolling-restart preflight checks built
 * on the legacy accessors silently degrade to less-safe heuristics.
 *
 * <p>The return types are kept raw / Object below to avoid pulling the
 * internal {@code LogDirInfo} type into the fixture's surface; the
 * bytecode call-site descriptor is what matters to the rule, not the
 * local variable's static type.
 */
public final class BadDescribeLogDirsResultLegacy {

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "bad-describe-log-dirs-legacy");
        return p;
    }

    public Map<Integer, ?> askLegacyValues() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeLogDirsResult result = admin.describeLogDirs(List.of(0, 1, 2));
            // FIRES — direct INVOKEVIRTUAL on the legacy per-broker accessor
            // returning Map<Integer, KafkaFuture<Map<String, LogDirInfo>>>.
            // LogDirInfo is the internal wire-protocol type; capacity-monitoring
            // code built on this Map cannot see totalBytes/usableBytes.
            return result.values();
        }
    }

    public KafkaFuture<? extends Map<Integer, ?>> askLegacyAll() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeLogDirsResult result = admin.describeLogDirs(List.of(0, 1, 2));
            // FIRES — direct INVOKEVIRTUAL on the legacy KafkaFuture<Map>
            // accessor. Same internal LogDirInfo leak as above.
            return result.all();
        }
    }

    public Supplier<? extends Map<Integer, ?>> capturedValues() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeLogDirsResult result = admin.describeLogDirs(List.of(0));
            // FIRES — INVOKEDYNAMIC method-ref capture. SAM Supplier#get
            // returns Object; descriptor erases to ()Ljava/util/Map; on the
            // captured handle. User-class bytecode contains ZERO INVOKE*
            // targeting values; the call lives in the LambdaMetafactory
            // bridge, so a name-only MethodInsnNode walk would miss it.
            return result::values;
        }
    }

    public Supplier<? extends KafkaFuture<?>> capturedAll() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeLogDirsResult result = admin.describeLogDirs(List.of(0));
            // FIRES — INVOKEDYNAMIC method-ref capture targeting the legacy
            // KafkaFuture<Map> accessor. Same INVOKEDYNAMIC-bridge logic as
            // capturedValues(); the rule's bsm-arg walk catches the handle's
            // owner+name.
            return result::all;
        }
    }
}
