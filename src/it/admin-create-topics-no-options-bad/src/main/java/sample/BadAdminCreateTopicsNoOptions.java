package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: ADMIN_CREATE_TOPICS_NO_OPTIONS — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptor on both Admin
 * (INVOKEINTERFACE) and AdminClient (INVOKEVIRTUAL).
 */
public final class BadAdminCreateTopicsNoOptions {

    /** 1-arg SAM matching {@code (Collection)CreateTopicsResult}. */
    @FunctionalInterface
    interface CreateTopicsFactory {
        CreateTopicsResult apply(Collection<NewTopic> topics);
    }

    private static List<NewTopic> spec(String name) {
        return List.of(new NewTopic(name, 12, (short) 3));
    }

    // ===== Direct INVOKE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on Admin.createTopics. */
    public CreateTopicsResult createOrders(Admin admin) {
        return admin.createTopics(spec("orders"));
    }

    /** MUST FIRE — direct INVOKEVIRTUAL on AdminClient.createTopics. */
    public CreateTopicsResult createOrdersViaClient(AdminClient client) {
        return client.createTopics(spec("orders-client"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on Admin.createTopics
     *  with multiple topics in the same call. */
    public CreateTopicsResult createMany(Admin admin) {
        return admin.createTopics(List.of(
                new NewTopic("a", 3, (short) 1),
                new NewTopic("b", 3, (short) 1),
                new NewTopic("c", 3, (short) 1)));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code admin::createTopics} bound to
     *  {@link Function Function&lt;Collection, CreateTopicsResult&gt;}.
     *  Indy bsm-arg points at {@code createTopics(Collection)
     *  CreateTopicsResult}. */
    public Function<Collection<NewTopic>, CreateTopicsResult>
            buildFunctionFactory(Admin admin) {
        return admin::createTopics;
    }

    /** MUST FIRE — {@code client::createTopics} bound to
     *  {@link Function Function&lt;Collection, CreateTopicsResult&gt;}
     *  against AdminClient (REF_invokeVirtual handle). */
    public Function<Collection<NewTopic>, CreateTopicsResult>
            buildClientFactory(AdminClient client) {
        return client::createTopics;
    }

    /** MUST FIRE — {@code admin::createTopics} bound to a custom
     *  1-arg Collection SAM. Indy implMethod handle descriptor
     *  is the no-Options overload exactly. */
    public CreateTopicsFactory
            buildCustomFactory(Admin admin) {
        return admin::createTopics;
    }

    /** MUST FIRE — local SAM bound and applied immediately. The
     *  indy site is in this method's bytecode. */
    public CreateTopicsResult useLocalFactory(Admin admin) {
        Function<Collection<NewTopic>, CreateTopicsResult> factory = admin::createTopics;
        return factory.apply(spec("local-topic"));
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code admin.createTopics(spec)}. The synthetic lambda
     *  method's bytecode contains a direct INVOKEINTERFACE on
     *  the no-Options overload. */
    public Stream<CreateTopicsResult> createAll(
            Admin admin, List<String> names) {
        return names.stream().map(n -> admin.createTopics(spec(n)));
    }

    public static void main(String[] args) {
        new BadAdminCreateTopicsNoOptions();
    }
}
