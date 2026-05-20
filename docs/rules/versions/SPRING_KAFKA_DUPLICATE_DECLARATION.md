# SPRING_KAFKA_DUPLICATE_DECLARATION

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: Declaring spring-kafka and spring-boot-starter-kafka is one of them too many.

## TL;DR

`spring-boot-starter-kafka` is a thin wrapper around `spring-kafka`. Adding both as explicit dependencies is redundant and risks version conflicts when one is pinned and the other isn't. Pick the starter (for Boot apps) or the bare library (for non-Boot apps) — not both.

## The setup

A team had `spring-kafka` declared explicitly long before they adopted Spring Boot. When they added Boot, they followed the docs and added `spring-boot-starter-kafka`. Both stayed. The starter pulls `spring-kafka` transitively; the explicit declaration overrides it; Maven nearest-wins makes the resolution non-obvious.

## What's actually happening

`spring-boot-starter-kafka` is just:

```xml
<dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter</artifactId></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
</dependencies>
```

— a vehicle for "pull spring-kafka plus the Boot autoconfigure". The Boot BOM pins both at the right version.

When the project also declares `spring-kafka` explicitly:

- Without a version: harmless; both resolve to Boot's pinned version.
- With a version: the explicit version wins. Boot's autoconfigure expects a specific spring-kafka API; mismatched versions can break the listener container at startup.

## Why this is subtle

- The two coordinates look like they do different things. Newcomers add both "to be safe".
- Even when they resolve to the same version, the redundancy is a maintenance burden: every Boot bump needs to confirm the explicit declaration is still in sync.

## Operational impact

- **Version drift** when the explicit declaration pins a different version than Boot manages.
- **Cognitive load** — reviewers reading the pom can't tell which version is in effect without running `mvn dependency:tree`.

## How to fix

```xml
<!-- BAD: both -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-kafka</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
    <version>3.3.5</version>
</dependency>

<!-- GOOD: starter alone -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-kafka</artifactId>
</dependency>
```

## When this might be a false positive

- A non-Spring-Boot project that uses spring-kafka directly — no starter needed; rule doesn't apply.
- A multi-module project where one module needs the starter and another needs only the library (rare, but valid).

## Detection strategy

- Walk `project.getDependencies()` (declared, not resolved).
- If both `org.springframework.boot:spring-boot-starter-kafka` and `org.springframework.kafka:spring-kafka` are explicitly declared in the same module, WARNING.
- Bonus: if the spring-kafka explicit declaration also carries a `<version>`, escalate to ERROR (version override on top of redundant declaration).

## References

- [Spring Boot starter list](https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#using.build-systems.starters)
- [spring-boot-starter-kafka source on GitHub](https://github.com/spring-projects/spring-boot/blob/main/spring-boot-project/spring-boot-starters/spring-boot-starter-kafka/build.gradle)
