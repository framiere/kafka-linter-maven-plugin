package io.conductor.kafkalinter.scanner;

import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public final class ProjectScanner {

    private final ClassAnalyzer analyzer;

    public ProjectScanner(List<Rule> rules) {
        this.analyzer = new ClassAnalyzer(rules);
    }

    public List<Violation> scanDirectory(Path classesDir) throws IOException {
        List<Violation> all = new ArrayList<>();
        if (!Files.isDirectory(classesDir)) return all;

        try (Stream<Path> stream = Files.walk(classesDir)) {
            List<Path> classFiles = stream
                    .filter(p -> p.toString().endsWith(".class"))
                    .filter(Files::isRegularFile)
                    .toList();
            for (Path cf : classFiles) {
                try (InputStream in = Files.newInputStream(cf)) {
                    all.addAll(analyzer.analyze(in));
                }
            }
        }
        return all;
    }
}
