package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of the deprecated 2-arg
 * {@link org.apache.kafka.clients.admin.TopicListing} constructor
 * {@code (String name, boolean isInternal)} — whether the call lands
 * directly via {@code INVOKESPECIAL} or indirectly through an
 * {@code INVOKEDYNAMIC} constructor-reference capture (e.g.
 * {@code TopicListing::new} bound to a factory functional interface used
 * by parameterized-test sources, fixture-driven Admin mocks, or
 * inventory-reconciler test harnesses that fabricate topic-list entries).
 *
 * <h2>Why this constructor is deprecated, not merely cosmetic</h2>
 *
 * <p>{@link org.apache.kafka.clients.admin.TopicListing} is the row type
 * returned by {@code Admin.listTopics()} — one entry per topic the
 * caller can see, carrying the topic's name, its {@link
 * org.apache.kafka.common.Uuid topicId}, and the {@code isInternal} flag.
 *
 * <p>KIP-516 (Kafka 2.8) introduced topic IDs cluster-wide: every topic
 * receives a 16-byte {@link org.apache.kafka.common.Uuid} on creation
 * and the broker carries that UUID through every protocol that references
 * the topic (metadata, fetch, produce, delete, configure). Topic IDs
 * survive recreation: a {@code delete-then-create} of topic
 * {@code orders} produces a different topicId on the new topic, which is
 * the exact discriminator the broker uses to reject stale produces or
 * fetches against the recreated topic.
 *
 * <p>The legacy 2-arg constructor predates topic IDs and constructs a
 * {@code TopicListing} whose {@code topicId()} returns
 * {@code Uuid.ZERO_UUID} — the documented sentinel for pre-Kafka-2.8
 * clusters that did not yet assign topic IDs. Three consequences follow
 * for test scaffolding built on this constructor:
 *
 * <ul>
 *   <li><b>Reconcilers that detect topic recreation by topicId
 *       comparison silently degrade against fixtures.</b> A reconciler
 *       reading {@code listing.topicId().equals(persistedId)} compares
 *       a real broker-assigned UUID against {@code Uuid.ZERO_UUID} from
 *       the fixture; the comparison always fails, so test assertions
 *       on the &laquo;topic recreated&raquo; branch trigger
 *       regardless of whether the test simulated recreation.</li>
 *   <li><b>Routing rules that match topicId against a known set
 *       silently match the &laquo;zero topic&raquo;.</b> Code that
 *       treats {@code Uuid.ZERO_UUID} as a special value (legacy-broker
 *       fallback, pre-create placeholder) fires on every fixture-built
 *       {@code TopicListing}, making it impossible to test the
 *       happy-path branch where a real UUID is present.</li>
 *   <li><b>INVOKEDYNAMIC {@code TopicListing::new} captures silently
 *       bind to the deprecated 2-arg ctor whenever the factory SAM
 *       arity is 2.</b> A test harness using a {@code BiFunction<String,
 *       Boolean, TopicListing>} factory resolves the constructor-ref by
 *       arity to the deprecated overload — the user-class bytecode
 *       contains zero direct {@code INVOKESPECIAL} on the legacy ctor
 *       and a name-only MethodInsnNode walk misses the call.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>The non-deprecated 3-arg constructor takes
 * {@code (String name, Uuid topicId, boolean isInternal)}. Tests that
 * genuinely cannot supply a topicId should pass {@code Uuid.ZERO_UUID}
 * explicitly — that makes the intent visible at the call site, lets
 * code reviewers flag fixtures that should be using a real UUID, and
 * keeps the constructor-resolution unambiguous.
 *
 * <h2>Descriptor discrimination</h2>
 *
 * <p>{@code <init>} is overloaded on {@code TopicListing}: the legacy
 * 2-arg ctor has descriptor {@code (Ljava/lang/String;Z)V} and the
 * modern 3-arg ctor has
 * {@code (Ljava/lang/String;Lorg/apache/kafka/common/Uuid;Z)V}. The rule
 * matches the legacy descriptor exactly. The modern descriptor differs
 * by a {@code Uuid} parameter, so it never matches — descriptor
 * discrimination is mandatory because a name-only filter would
 * false-positive on the supported migration target.
 *
 * <h2>Constructor-reference capture path</h2>
 *
 * <p>{@code TopicListing::new} bound to a factory functional interface
 * compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_newInvokeSpecial} handle pointing at the resolved
 * constructor. The rule's bsm-arg walk catches this case by checking
 * the handle's {@code (owner, name, desc)} triple against the same
 * filter used for direct calls.
 */
public final class AdminTopicListingNameInternalCtorDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.ADMIN_TOPIC_LISTING);
    private static final String CTOR_NAME = "<init>";
    private static final String LEGACY_DESC = "(Ljava/lang/String;Z)V";

    private final Severity severity;

    public AdminTopicListingNameInternalCtorDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_TOPIC_LISTING_NAME_INTERNAL_CTOR_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && CTOR_NAME.equals(mi.name)
                        && LEGACY_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, CTOR_NAME, LEGACY_DESC);
                    if (h != null) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.ADMIN_TOPIC_LISTING_NAME_INTERNAL_CTOR_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "TopicListing(String name, boolean isInternal) 2-arg constructor is reached "
                        + "here — either as a direct call or as an INVOKEDYNAMIC "
                        + "constructor-reference capture (e.g. `TopicListing::new` bound to "
                        + "a BiFunction<String, Boolean, TopicListing> factory used by a "
                        + "parameterized-test source, fixture-driven Admin mock, or "
                        + "inventory-reconciler test harness). This constructor is "
                        + "deprecated since Kafka 3.0 (KIP-516) because it predates "
                        + "cluster-wide topic IDs (introduced in Kafka 2.8) and constructs "
                        + "a TopicListing whose topicId() returns Uuid.ZERO_UUID — the "
                        + "documented sentinel for pre-Kafka-2.8 clusters that did not yet "
                        + "assign topic IDs. Three consequences for test scaffolding built "
                        + "on this constructor: (1) reconcilers that detect topic recreation "
                        + "via listing.topicId().equals(persistedId) compare a real "
                        + "broker-assigned UUID against Uuid.ZERO_UUID from the fixture and "
                        + "always trigger the `recreated` branch regardless of whether the "
                        + "test simulated recreation; (2) routing rules that treat "
                        + "Uuid.ZERO_UUID as a special value (legacy-broker fallback, "
                        + "pre-create placeholder) fire on every fixture-built TopicListing, "
                        + "making the happy-path branch untestable; (3) INVOKEDYNAMIC "
                        + "`TopicListing::new` captures silently bind to the deprecated "
                        + "2-arg ctor whenever the factory SAM arity is 2 — the user-class "
                        + "bytecode contains zero direct INVOKESPECIAL on the legacy ctor "
                        + "and a name-only walk misses it. Migrate to the non-deprecated "
                        + "3-arg constructor `new TopicListing(name, topicId, isInternal)`. "
                        + "If the test genuinely cannot supply a topicId, pass "
                        + "`Uuid.ZERO_UUID` explicitly — the intent becomes visible at the "
                        + "call site, reviewers can flag fixtures that should use a real "
                        + "UUID, and constructor-resolution stays unambiguous.");
    }
}
