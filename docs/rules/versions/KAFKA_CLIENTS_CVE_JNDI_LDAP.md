# KAFKA_CLIENTS_CVE_JNDI_LDAP

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: CVE-2023-25194 / CVE-2025-27818 — the Kafka spiritual successor to Log4Shell.

## TL;DR

`kafka-clients` and `kafka-connect-*` versions before `3.4.0` are wide open to **CVE-2023-25194**, a JNDI deserialization RCE via SASL JAAS `JndiLoginModule`. Even after the 3.4 mitigation, `LdapLoginModule` remained exploitable until **3.9.1 / 4.0.0** (**CVE-2025-27818 / 27819**). Any client that can have its JAAS config influenced by an untrusted party is one carefully-crafted property away from arbitrary code execution.

## The setup

The trust boundary that matters here is "who can set Kafka client configuration?" If the answer includes anyone other than the deployment owner, you have an attack surface:

- **Kafka Connect** — any user with REST permission to create/edit connectors.
- **MirrorMaker 2** with dynamic remote-cluster config.
- **Multi-tenant streaming platforms** that accept tenant-supplied client config.
- **JMX-driven dynamic reconfig** of brokers and Connect workers.
- **Admin tools** that load config from S3, ConfigMaps, or other shared stores.

## What's actually happening

SASL/JAAS configuration in Kafka lets you pick a `LoginModule`. Java's standard library ships several. Two were exploitable:

- `com.sun.security.auth.module.JndiLoginModule` — accepts a JNDI URL. Pre-3.4, you could point it at `ldap://attacker/payload` and get classic JNDI deserialization. Same primitive as Log4Shell.
- `com.sun.security.auth.module.LdapLoginModule` — same shape, different module, also disabled by default only as of 3.9.1.

Mitigation evolution:

| Kafka version | Behavior |
|---------------|----------|
| < 3.4.0 | `JndiLoginModule` allowed by default — exploitable |
| 3.4.0 – 3.9.0 | `JndiLoginModule` disabled by default; `LdapLoginModule` still allowed |
| 3.9.1 / 4.0.0+ | Both disabled by default; new ones disallowable via `-Dorg.apache.kafka.disallowed.login.modules` |

The system property `org.apache.kafka.disallowed.login.modules` lets you extend the deny-list further. It does **not** disable login-module use entirely — modules like `PlainLoginModule` and `ScramLoginModule` continue to work.

## Why this is subtle

- Three CVEs cover the same family. A team that "patched 25194" by going to 3.4 may not realize 27818/27819 still apply.
- The Apache CVE wording is precise: "if the attacker can configure the connector". Teams read this and assume "we don't expose Connect to the internet, we're fine". But the Connect REST API is often reachable from any pod in the namespace; lateral movement is exactly how this gets used.
- The fix is **a deny-list system property** — versions that "support" the fix don't apply it automatically in all cases. Bumping the version is necessary but not always sufficient — read the upgrade notes.
- `kafka-clients` is consumed by hundreds of downstream libraries — Connect, Streams, MirrorMaker 2, Strimzi operators, Confluent components. Any of them transitively pulling an old client introduces the issue.

## Operational impact

- **Remote code execution** as the user the client/Connect process runs as.
- **Lateral movement** within the cluster — Connect workers typically have credentials to all source/sink systems.
- **Credential theft** — the LDAP module can be coerced into authenticating outbound, sending the configured credentials to attacker infrastructure.
- **Audit logging blindspot** — by the time JNDI fires, the malicious config has already been accepted by the API.

## How to fix

```xml
<!-- BAD -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.3.2</version>      <!-- JndiLoginModule exploitable -->
</dependency>

<!-- BETTER (closes CVE-2023-25194) -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.4.1</version>      <!-- JndiLoginModule disabled -->
</dependency>

<!-- BEST (closes CVE-2025-27818 / 27819) -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>      <!-- both modules disabled -->
</dependency>
```

JVM arg defense in depth (apply to Connect workers, MM2 workers, brokers):

```
-Dorg.apache.kafka.disallowed.login.modules=com.sun.security.auth.module.JndiLoginModule,com.sun.security.auth.module.LdapLoginModule
```

If you also run Kafka Connect, set a connector client config override policy that restricts which JAAS modules tenants can specify.

## When this might be a false positive

- A pure-client app with hard-coded JAAS config and no surface for runtime config change — exposure is low. Upgrade is still cheap.
- Brokers / Connect / MM2 already running 3.9.1+ with the deny-list system property set: enforced by JVM args, not by version alone. Confirm in the deployment manifest.

## Detection strategy

- Resolved `kafka-clients` version:
  - `< 3.4.0` → ERROR, name CVE-2023-25194.
  - `[3.4.0, 3.9.1)` → ERROR, name CVE-2025-27818 / 27819 (LDAP still allowed).
  - `>= 3.9.1` → OK.
- Also check `kafka-connect-runtime` / `connect-runtime`, since Connect is the headline attack surface.
- If `quarkus-kafka-connect` / `spring-cloud-stream-binder-kafka-connect` or similar Connect artifacts are present, escalate severity in the message.

## References

- [Apache CVE list — CVE-2023-25194](https://kafka.apache.org/cve-list)
- [NVD entry for CVE-2023-25194](https://nvd.nist.gov/vuln/detail/CVE-2023-25194)
- [NVD entry for CVE-2025-27818](https://nvd.nist.gov/vuln/detail/CVE-2025-27818)
- [NVD entry for CVE-2025-27819](https://nvd.nist.gov/vuln/detail/CVE-2025-27819)
- [Kafka 3.4 release notes — JndiLoginModule mitigation](https://kafka.apache.org/blog#apache_kafka_340_release_announcement)
