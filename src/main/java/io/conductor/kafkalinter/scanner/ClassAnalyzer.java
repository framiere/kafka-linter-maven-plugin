package io.conductor.kafkalinter.scanner;

import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.ClassNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public final class ClassAnalyzer {

    private final List<Rule> rules;

    public ClassAnalyzer(List<Rule> rules) {
        this.rules = rules;
    }

    public List<Violation> analyze(InputStream classBytes) throws IOException {
        ClassReader cr = new ClassReader(classBytes);
        ClassNode cn = new ClassNode();
        cr.accept(cn, ClassReader.SKIP_FRAMES);
        RuleContext ctx = new RuleContext(cn);

        List<Violation> all = new ArrayList<>();
        for (Rule r : rules) {
            all.addAll(r.check(ctx));
        }
        return all;
    }
}
