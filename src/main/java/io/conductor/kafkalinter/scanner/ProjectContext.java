package io.conductor.kafkalinter.scanner;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.project.MavenProject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Project-wide read-only view consumed by {@link io.conductor.kafkalinter.rules.ProjectScopedRule}.
 *
 * <p>Bundles together:
 * <ul>
 *   <li>the resolved Maven artifact graph (for version-gate / CVE rules),</li>
 *   <li>the project's Java {@code source}/{@code target} levels (for JVM version checks),</li>
 *   <li>parsed {@code application.properties} / {@code application.yaml} from
 *       {@code src/main/resources} (for framework-config rules).</li>
 * </ul>
 *
 * <p>The class does the (cheap, idempotent) parsing once in the constructor so
 * that every rule sees the same snapshot and no rule has to know about Maven APIs.
 */
public final class ProjectContext {

    private final MavenProject project;
    private final Map<String, Artifact> artifactsByGa;
    private final Map<Path, Properties> propertiesFiles;
    private final Map<Path, String> yamlFiles;

    public ProjectContext(MavenProject project) {
        this.project = project;
        this.artifactsByGa = indexArtifacts(project);
        this.propertiesFiles = new LinkedHashMap<>();
        this.yamlFiles = new LinkedHashMap<>();
        loadConfigFiles(project);
    }

    private static Map<String, Artifact> indexArtifacts(MavenProject p) {
        Map<String, Artifact> m = new LinkedHashMap<>();
        Set<Artifact> all = p.getArtifacts();
        if (all == null) return m;
        for (Artifact a : all) {
            m.put(a.getGroupId() + ":" + a.getArtifactId(), a);
        }
        return m;
    }

    private void loadConfigFiles(MavenProject p) {
        Path resourcesDir = Path.of(p.getBasedir().getAbsolutePath(), "src", "main", "resources");
        if (!Files.isDirectory(resourcesDir)) return;
        try (Stream<Path> walk = Files.walk(resourcesDir)) {
            for (Path f : walk.filter(Files::isRegularFile).toList()) {
                String name = f.getFileName().toString();
                if (name.endsWith(".properties")) {
                    Properties props = new Properties();
                    try {
                        props.load(Files.newBufferedReader(f));
                    } catch (IOException ignored) { }
                    propertiesFiles.put(f, props);
                } else if (name.endsWith(".yml") || name.endsWith(".yaml")) {
                    try {
                        yamlFiles.put(f, Files.readString(f));
                    } catch (IOException ignored) { }
                }
            }
        } catch (IOException ignored) { }
    }

    public MavenProject project() { return project; }

    /**
     * Convert an absolute path under the project basedir to a project-relative form
     * ({@code src/main/resources/application.properties}). Anything outside the basedir
     * is returned unchanged. Used by rules so that emitted violations don't bake in
     * a developer's home directory.
     */
    public String relativize(Path file) {
        if (file == null) return "";
        try {
            Path base = project.getBasedir().toPath().toAbsolutePath().normalize();
            Path abs = file.toAbsolutePath().normalize();
            if (abs.startsWith(base)) {
                return base.relativize(abs).toString().replace('\\', '/');
            }
        } catch (RuntimeException ignored) { }
        return file.toString();
    }

    /** Look up a resolved artifact by {@code groupId:artifactId}. */
    public Optional<Artifact> artifact(String groupId, String artifactId) {
        return Optional.ofNullable(artifactsByGa.get(groupId + ":" + artifactId));
    }

    /** Resolve the project's effective {@code maven.compiler.target} (or fall back to source). */
    public Optional<String> javaTargetVersion() {
        String t = project.getProperties().getProperty("maven.compiler.target");
        if (t == null || t.isBlank()) t = project.getProperties().getProperty("maven.compiler.source");
        if (t == null || t.isBlank()) t = project.getProperties().getProperty("maven.compiler.release");
        return Optional.ofNullable(t);
    }

    public Map<Path, Properties> propertiesFiles() { return propertiesFiles; }

    public Map<Path, String> yamlFiles() { return yamlFiles; }

    /**
     * Convenience: collect every {@code key=value} pair from every loaded .properties file,
     * paired with the file each pair came from. Multi-file projects (e.g. Spring profile
     * variants) yield duplicate keys with different values, which is the correct behaviour
     * — rules typically want to flag any occurrence.
     */
    public List<PropertyHit> allPropertyHits() {
        List<PropertyHit> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : propertiesFiles.entrySet()) {
            for (String key : e.getValue().stringPropertyNames()) {
                out.add(new PropertyHit(e.getKey(), key, e.getValue().getProperty(key)));
            }
        }
        return out;
    }

    public record PropertyHit(Path file, String key, String value) { }
}
