package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link CreateTopicsOptions} pinning a per-call deadline, so
 * retry logic upstream can distinguish "in flight" from "failed"
 * by comparing against the chosen deadline.
 */
public final class GoodAdminCreateTopicsNoOptions {

    /** 2-arg SAM matching {@code (Collection, CreateTopicsOptions)
     *  CreateTopicsResult}. */
    @FunctionalInterface
    interface CreateTopicsWithOptionsFactory {
        CreateTopicsResult apply(Collection<NewTopic> topics,
                                 CreateTopicsOptions options);
    }

    private static List<NewTopic> spec(String name) {
        return List.of(new NewTopic(name, 12, (short) 3));
    }

    private static CreateTopicsOptions opts() {
        return new CreateTopicsOptions().timeoutMs(60_000);
    }

    public CreateTopicsResult createOrders(Admin admin) {
        return admin.createTopics(spec("orders"), opts());
    }

    public CreateTopicsResult createOrdersViaClient(AdminClient client) {
        return client.createTopics(spec("orders-client"), opts());
    }

    public CreateTopicsResult createMany(Admin admin) {
        return admin.createTopics(List.of(
                new NewTopic("a", 3, (short) 1),
                new NewTopic("b", 3, (short) 1),
                new NewTopic("c", 3, (short) 1)), opts());
    }

    public BiFunction<Collection<NewTopic>, CreateTopicsOptions, CreateTopicsResult>
            buildFunctionFactory(Admin admin) {
        return admin::createTopics;
    }

    public BiFunction<Collection<NewTopic>, CreateTopicsOptions, CreateTopicsResult>
            buildClientFactory(AdminClient client) {
        return client::createTopics;
    }

    public CreateTopicsWithOptionsFactory
            buildCustomFactory(Admin admin) {
        return admin::createTopics;
    }

    public CreateTopicsResult useLocalFactory(Admin admin) {
        CreateTopicsWithOptionsFactory factory = admin::createTopics;
        return factory.apply(spec("local-topic"), opts());
    }

    public Stream<CreateTopicsResult> createAll(
            Admin admin, List<String> names) {
        return names.stream().map(n -> admin.createTopics(spec(n), opts()));
    }

    public static void main(String[] args) {
        new GoodAdminCreateTopicsNoOptions();
    }
}
