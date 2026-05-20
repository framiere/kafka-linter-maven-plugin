package io.conductor.kafkalinter.rules.quarkus;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generic detector for a "bad value" on a SmallRye Reactive Messaging
 * channel-level property under {@code mp.messaging.{incoming|outgoing}.<channel>.<setting>}.
 *
 * <p>Walks every loaded {@code application.properties} once, finds every key that
 * matches the SmallRye channel template, and when the setting suffix is the one
 * this rule watches AND the value passes the bad-value predicate, emits one
 * violation per (file, channel) pair.
 *
 * <p>Channel name is reported as the violation's {@code methodName} ({@code channel:orders})
 * so the operator sees exactly which mapping the warning is about.
 */
public final class SmallRyeChannelConfigRule implements ProjectScopedRule {

    private static final Pattern CHANNEL_KEY = Pattern.compile(
            "^mp\\.messaging\\.(incoming|outgoing)\\.([^.]+)\\.(.+)$");

    private final RuleId ruleId;
    private final Severity severity;
    private final String watchedSettingSuffix;
    private final Predicate<String> badValue;
    private final String detailTemplate;
    /** "incoming", "outgoing", or null for either. */
    private final String directionFilter;

    public SmallRyeChannelConfigRule(RuleId ruleId, Severity severity,
                                     String directionFilter,
                                     String watchedSettingSuffix,
                                     Predicate<String> badValue,
                                     String detailTemplate) {
        this.ruleId = ruleId;
        this.severity = severity;
        this.directionFilter = directionFilter;
        this.watchedSettingSuffix = watchedSettingSuffix;
        this.badValue = badValue;
        this.detailTemplate = detailTemplate;
    }

    @Override
    public RuleId id() {
        return ruleId;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (ProjectContext.PropertyHit hit : ctx.allPropertyHits()) {
            Matcher m = CHANNEL_KEY.matcher(hit.key());
            if (!m.matches()) continue;
            String direction = m.group(1);
            String channel = m.group(2);
            String setting = m.group(3);
            if (directionFilter != null && !directionFilter.equals(direction)) continue;
            if (!watchedSettingSuffix.equals(setting)) continue;
            String value = hit.value() == null ? "" : hit.value().trim();
            if (!badValue.test(value)) continue;

            String detail = detailTemplate
                    .replace("{channel}", channel)
                    .replace("{value}", value)
                    .replace("{direction}", direction);
            out.add(new Violation(ruleId, severity,
                    ctx.relativize(hit.file()),
                    "channel:" + channel, 0, detail));
        }
        return out;
    }

    public static SmallRyeChannelConfigRule literal(RuleId id, Severity sev,
                                                    String direction, String setting,
                                                    String badLiteral, String detail) {
        return new SmallRyeChannelConfigRule(id, sev, direction, setting,
                badLiteral::equalsIgnoreCase, detail);
    }
}
