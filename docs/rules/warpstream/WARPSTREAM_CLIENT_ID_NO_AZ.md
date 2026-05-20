# WARPSTREAM_CLIENT_ID_NO_AZ
**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: No `ws_az=<az>` in `client.id` — every byte routed cross-AZ is $0.05/GB, paid silently in your AWS bill.
**Source**: Confluent agent-skills — kafka-streams-programming/references/warpstream-optimization.md § Zone-Aware Routing.

## TL;DR

When the target is WarpStream, the linter flags Kafka client configurations where `client.id` does not include the `ws_az=<availability-zone>` marker. WarpStream's service discovery uses this marker to route the client to an Agent in the same AZ; without it, traffic may cross AZ boundaries and incur AWS NAT charges of roughly $0.05/GB. There is no functional failure — the bill just gets larger every month.

## What's happening (the mechanism)

WarpStream Agents are stateless and any Agent can serve any partition. When a client connects without an AZ hint, the service-discovery layer picks an Agent based on load and proximity heuristics — which may or may not be the same AZ as the client. Cross-AZ traffic on AWS costs ~$0.01-0.02/GB each direction (intra-region) — for a high-volume topic, this aggregates to thousands of dollars per month.

The fix is a literal string marker appended to the Kafka client.id:

```properties
client.id=my-app,ws_az=us-east-1a
```

WarpStream parses out the `ws_az=` segment and uses it to route the client to a same-AZ Agent. The marker is also used for `ws_sle=true` (single-leader-epoch workaround) and `ws_dfat=true` (disable fetch-auto-tune).

**Do NOT use Kafka's `client.rack` instead.** The Confluent guide is explicit that `client.rack` and rack-aware consumer assignment cause unnecessary rebalances on WarpStream (Agents are stateless and interchangeable, so the rack metadata triggers reassignments that achieve nothing).

For Kafka Streams: set `client.id` in the Streams config (not `application.id`). Streams uses `client.id` as the base for the embedded producer/consumer client IDs.

## Operational impact

- Linear cost regression with topic throughput. A 100 MB/s topic crossing AZ boundaries: ~8.6 TB/day cross-AZ × $0.02/GB ≈ $5,200/month. Per topic. Per direction.
- Invisible — no error, no warning, no log line. Only visible in the AWS Cost Explorer breakdown under "data transfer".
- Multi-region deployments amplify the issue: the AZ assignment must be determined dynamically (from the EC2 instance metadata, the Kubernetes node label, etc.) and injected into the client config at startup.

## How to fix (bad → good code)

```properties
# BAD on WarpStream
client.id=order-processor

# GOOD
client.id=order-processor,ws_az=us-east-1a
```

For dynamic AZ injection in a Kubernetes / EC2 deployment:

```java
// At application startup
String az = System.getenv("AWS_AVAILABILITY_ZONE");   // injected via downward API or IMDS
String clientId = "order-processor,ws_az=" + az;
props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
```

```yaml
# Kubernetes — set AZ from the node label
env:
  - name: AWS_AVAILABILITY_ZONE
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName   # then resolve via init-container, or
  # or use the topology label directly:
  - name: AWS_AVAILABILITY_ZONE
    valueFrom:
      configMapKeyRef:
        name: az-config
        key: zone
```

Remove `client.rack` if present:

```properties
# REMOVE on WarpStream — causes unnecessary rebalances
# client.rack=us-east-1a
```

## When this might be a false positive

- The target is not WarpStream. `ws_az=` is a WarpStream-specific marker; on Apache Kafka, it's just an opaque string in `client.id`, harmless but useless.
- Single-AZ deployment (no AZ to cross). Print-only.
- WarpStream cluster running on-prem / single-DC where cross-AZ charges don't apply.

## Detection strategy

- WarpStream context signal (see `WARPSTREAM_IDEMPOTENCE_ENABLED` for detection criteria).
- Config-file: find every `client.id=` line (or `props.put(CLIENT_ID_CONFIG, ...)` in code). If the value does NOT contain `ws_az=`, flag.
- Also flag `client.rack=` lines under WarpStream — should be removed.
- Confidence: CONTEXT — depends on cloud provider and deployment topology. Print-only by default; bump to WARNING when both WarpStream targeting AND multi-AZ deployment are confirmed.

## References

- Confluent agent-skills — kafka-streams-programming/references/warpstream-optimization.md § Zone-Aware Routing, § Quick Checklist
- WarpStream docs — Zone-aware routing: https://docs.warpstream.com/warpstream/reference/configuration/client-configuration-recommendations
- AWS — Data transfer pricing: https://aws.amazon.com/ec2/pricing/on-demand/
