package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.RuleContext;

import java.util.List;

public interface Rule {
    RuleId id();
    List<Violation> check(RuleContext ctx);
}
