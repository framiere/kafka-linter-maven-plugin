package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.TopicCollection;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: ADMIN_DELETE_TOPICS_NO_OPTIONS — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises both unsafe overload descriptors on Admin
 * (INVOKEINTERFACE), AdminClient (INVOKEVIRTUAL), and via
 * INVOKEDYNAMIC method-reference captures.
 */
public final class BadAdminDeleteTopicsNoOptions {

    /** 1-arg SAM matching {@code (Collection)DeleteTopicsResult}. */
    @FunctionalInterface
    interface DeleteTopicsFactory {
        DeleteTopicsResult apply(Collection<String> names);
    }

    // ===== Direct INVOKE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.deleteTopics(Collection<String>). */
    public DeleteTopicsResult deleteOrders(Admin admin) {
        return admin.deleteTopics(List.of("orders"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.deleteTopics(TopicCollection). */
    public DeleteTopicsResult deleteByTopicCollection(Admin admin) {
        return admin.deleteTopics(TopicCollection.ofTopicNames(List.of("orders-tc")));
    }

    /** MUST FIRE — direct INVOKEVIRTUAL on
     *  AdminClient.deleteTopics(Collection<String>). */
    public DeleteTopicsResult deleteOrdersViaClient(AdminClient client) {
        return client.deleteTopics(List.of("orders-client"));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code admin::deleteTopics} bound to
     *  {@link Function Function&lt;Collection, DeleteTopicsResult&gt;}.
     *  Indy bsm-arg points at {@code deleteTopics(Collection)
     *  DeleteTopicsResult}. */
    public Function<Collection<String>, DeleteTopicsResult>
            buildCollectionFactory(Admin admin) {
        return admin::deleteTopics;
    }

    /** MUST FIRE — {@code admin::deleteTopics} bound to
     *  {@link Function Function&lt;TopicCollection, DeleteTopicsResult&gt;}.
     *  Indy bsm-arg points at {@code deleteTopics(TopicCollection)
     *  DeleteTopicsResult}. */
    public Function<TopicCollection, DeleteTopicsResult>
            buildTopicCollectionFactory(Admin admin) {
        return admin::deleteTopics;
    }

    /** MUST FIRE — {@code admin::deleteTopics} bound to a custom
     *  1-arg Collection SAM. Indy implMethod handle descriptor
     *  is the no-Options overload exactly. */
    public DeleteTopicsFactory
            buildCustomFactory(Admin admin) {
        return admin::deleteTopics;
    }

    /** MUST FIRE — local SAM bound and applied immediately. The
     *  indy site is in this method's bytecode. */
    public DeleteTopicsResult useLocalFactory(Admin admin) {
        Function<Collection<String>, DeleteTopicsResult> factory = admin::deleteTopics;
        return factory.apply(List.of("local-topic"));
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code admin.deleteTopics(List.of(name))}. The synthetic
     *  lambda method's bytecode contains a direct
     *  INVOKEINTERFACE on the no-Options overload. */
    public Stream<DeleteTopicsResult> deleteAll(
            Admin admin, List<String> names) {
        return names.stream().map(n -> admin.deleteTopics(List.of(n)));
    }

    public static void main(String[] args) {
        new BadAdminDeleteTopicsNoOptions();
    }
}
