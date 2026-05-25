package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.TopicCollection;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link DeleteTopicsOptions} pinning a per-call deadline, so
 * retry logic upstream can distinguish "in flight" from
 * "failed" before issuing a follow-up destructive operation.
 */
public final class GoodAdminDeleteTopicsNoOptions {

    /** 2-arg SAM matching {@code (Collection, DeleteTopicsOptions)
     *  DeleteTopicsResult}. */
    @FunctionalInterface
    interface DeleteTopicsWithOptionsFactory {
        DeleteTopicsResult apply(Collection<String> names,
                                 DeleteTopicsOptions options);
    }

    private static DeleteTopicsOptions opts() {
        return new DeleteTopicsOptions().timeoutMs(60_000);
    }

    public DeleteTopicsResult deleteOrders(Admin admin) {
        return admin.deleteTopics(List.of("orders"), opts());
    }

    public DeleteTopicsResult deleteByTopicCollection(Admin admin) {
        return admin.deleteTopics(
                TopicCollection.ofTopicNames(List.of("orders-tc")), opts());
    }

    public DeleteTopicsResult deleteOrdersViaClient(AdminClient client) {
        return client.deleteTopics(List.of("orders-client"), opts());
    }

    public BiFunction<Collection<String>, DeleteTopicsOptions, DeleteTopicsResult>
            buildCollectionFactory(Admin admin) {
        return admin::deleteTopics;
    }

    public BiFunction<TopicCollection, DeleteTopicsOptions, DeleteTopicsResult>
            buildTopicCollectionFactory(Admin admin) {
        return admin::deleteTopics;
    }

    public DeleteTopicsWithOptionsFactory
            buildCustomFactory(Admin admin) {
        return admin::deleteTopics;
    }

    public DeleteTopicsResult useLocalFactory(Admin admin) {
        DeleteTopicsWithOptionsFactory factory = admin::deleteTopics;
        return factory.apply(List.of("local-topic"), opts());
    }

    public Stream<DeleteTopicsResult> deleteAll(
            Admin admin, List<String> names) {
        return names.stream().map(n -> admin.deleteTopics(List.of(n), opts()));
    }

    public static void main(String[] args) {
        new GoodAdminDeleteTopicsNoOptions();
    }
}
