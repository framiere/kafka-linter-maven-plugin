# LOGGING_IMPL_CONFLICT

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: Two slf4j bindings on the classpath is a coin flip on which one logs.

## TL;DR

`kafka-clients` declares `org.slf4j:slf4j-api` and leaves the implementation to the consumer. Most Kafka apps then end up with **multiple** slf4j bindings on the classpath — typically Spring Boot's `logback-classic` plus a transitive `slf4j-log4j12` or `slf4j-reload4j` from an old library. slf4j picks one binding at random with a single-line warning, and the others are silent.

## The setup

A project uses Spring Boot (Logback by default) and adds `kafka-streams` (transitively brings `slf4j-api` only, no binding). Fine. Later, someone adds `org.apache.hadoop:hadoop-common` for HDFS state stores — Hadoop transitively pulls `log4j:log4j:1.2.17` and `org.slf4j:slf4j-log4j12`. Now two bindings exist; slf4j prints `SLF4J: Class path contains multiple SLF4J bindings` at startup, picks one (often the wrong one), and the team's Logback config goes unused.

## What's actually happening

slf4j is a facade. At startup it looks for exactly one binding (e.g. `org.slf4j.impl.StaticLoggerBinder`) and uses it. If multiple bindings are on the classpath, slf4j 1.x prints a warning and picks the first one returned by the classloader. slf4j 2.x uses `ServiceLoader` and behavior is similar but more deterministic.

Common conflicting bindings in a Kafka project:

| Binding | Pulled in by | Carries |
|---------|--------------|---------|
| `logback-classic` | Spring Boot | Logback |
| `log4j-slf4j2-impl` | Quarkus, modern Spring opt-in | Log4j 2 |
| `slf4j-log4j12` | Old Hadoop, old Zookeeper-client, old Curator | Log4j 1 |
| `slf4j-reload4j` | Some forks of the above | reload4j (Log4j 1 fork) |
| `slf4j-simple` | Test scope leakage | Simple impl |
| `slf4j-jdk14` | java.util.logging adapter | JUL |

Once you have two bindings:
- Your `logback.xml` or `log4j2.xml` may be ignored if the picked binding doesn't read it.
- Log levels are inconsistent across libraries — `kafka-clients` logs at INFO via slf4j-api, but the underlying impl decides what to do.
- Performance traps — `log4j 1.x` ignores parameter substitution and may format messages eagerly, costing CPU.

## Why this is subtle

- The warning is one line at startup. CI logs scroll past.
- The "winning" binding can change between local and prod depending on the classloader iteration order — same JAR set, different result.
- Test scopes leak — `<scope>test</scope>` libraries can introduce a binding that's only visible in `mvn test`, not in production.

## Operational impact

- **Logback / Log4j2 config silently ignored** — your carefully-tuned async appender doesn't apply.
- **Log4j 1.x carrying CVEs** — log4j 1.x is unmaintained and has known unfixed CVEs.
- **Different log levels in different environments** — when slf4j's binding pick order changes between local and prod.

## How to fix

```xml
<!-- BAD: log4j 1 binding leaking in transitively -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>3.3.6</version>
    <!-- pulls slf4j-log4j12 + log4j:log4j -->
</dependency>

<!-- GOOD: exclude the unwanted binding -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>3.3.6</version>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
        </exclusion>
        <exclusion>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

For Spring Boot apps, exclude any non-Logback binding everywhere. For Quarkus apps (which bridge slf4j → JBoss Logging), exclude every other slf4j impl in transitive paths.

The cleanest defense is `maven-enforcer-plugin`:

```xml
<plugin>
    <artifactId>maven-enforcer-plugin</artifactId>
    <executions>
        <execution>
            <goals><goal>enforce</goal></goals>
            <configuration>
                <rules>
                    <banDuplicateClasses>
                        <findAllDuplicates>true</findAllDuplicates>
                    </banDuplicateClasses>
                </rules>
            </configuration>
        </execution>
    </executions>
</plugin>
```

## When this might be a false positive

- A project deliberately uses two bindings for different classloader hierarchies (rare, mostly OSGi). Suppress with documentation.

## Detection strategy

- Walk `project.getArtifacts()` and identify candidates with `org.slf4j:slf4j-*` or `log4j:log4j` or `org.apache.logging.log4j:log4j-slf4j*`.
- Bindings to detect (any artifactId matching these patterns is a "binding"):
  - `slf4j-log4j12`, `slf4j-reload4j`, `slf4j-simple`, `slf4j-jdk14`, `slf4j-nop`
  - `logback-classic`
  - `log4j-slf4j-impl`, `log4j-slf4j2-impl`
  - `jcl-over-slf4j` is a bridge, not a binding — don't flag.
- Count bindings. If `> 1`: WARNING with the conflict list.
- Bonus: if `log4j:log4j` (Log4j 1.x) is present anywhere, escalate — it's unmaintained.

## References

- [SLF4J — multiple bindings warning](https://www.slf4j.org/codes.html#multiple_bindings)
- [Spring Boot logging documentation](https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#features.logging)
- [Quarkus logging guide](https://quarkus.io/guides/logging)
