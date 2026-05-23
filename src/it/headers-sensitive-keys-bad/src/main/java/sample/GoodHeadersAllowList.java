package sample;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Negative control for HEADERS_SENSITIVE_KEYS — every header emit in this
 * class must be silent. Two distinct categories of silence are exercised
 * here, and the reader should be able to point at any single emit and
 * explain WHICH of the two reasons keeps the rule from firing on it.
 *
 * <p>Category A — the allow-list overrides the substring check.
 * The rule's sensitive-substring set contains the bare word
 * {@code token}, which means by default any header key containing the
 * letters t-o-k-e-n would fire. That is too aggressive in practice:
 * three header keys ending in {@code -token} are industry-standard
 * conventions that explicitly do NOT carry credentials, and the rule
 * carries an allow-list with exactly those three keys to keep them
 * silent:
 * <ol>
 *   <li>{@code idempotency-token} — the deduplication nonce emitted
 *       by HTTP API gateways (Stripe, AWS API Gateway, etc.) to make
 *       client retries safe. It is a random opaque string that
 *       authenticates NOTHING; an attacker who reads it cannot replay
 *       any privileged action with it. Its only purpose is to let the
 *       server recognise "I have already processed this request" on a
 *       retry. Carrying it on the Kafka record header is legitimate
 *       so downstream consumers can also de-duplicate.</li>
 *   <li>{@code trace-token} — the distributed-tracing correlation id
 *       (Zipkin, Jaeger, OpenTelemetry — though in OTel the canonical
 *       header is "traceparent", many in-house stacks still use the
 *       "trace-token" name). Like the idempotency nonce it
 *       authenticates nothing; it is a request-scoped label used by
 *       the observability stack to stitch spans together.</li>
 *   <li>{@code csrf-token} — the cross-site-request-forgery token
 *       that was used to validate the HTTP request that produced this
 *       Kafka record. Once the HTTP layer accepted the request, the
 *       CSRF token has done its job and is just an audit-trail
 *       breadcrumb. It is not a session token; it cannot be used to
 *       impersonate the user; it is bound to the user's session
 *       cookie which is NOT on the wire here. So carrying it
 *       downstream is harmless.</li>
 * </ol>
 * The rule lowercases the header key before checking, so "Trace-Token"
 * or "CSRF-Token" or "Idempotency-Token" all hit the allow-list.
 *
 * <p>Category B — keys that simply do not contain any sensitive
 * substring. The rule's substring set is closed (password / passwd /
 * secret / apikey / api_key / api-key / token / authorization /
 * bearer / credential). Any key whose lowercase form contains none
 * of those substrings is silent for the trivial reason that no check
 * matched — no allow-list consultation needed. The headers below
 * are the kind of business-context metadata that legitimately rides
 * on a Kafka record:
 * <ol>
 *   <li>{@code trace-id} — the modern OpenTelemetry trace identifier.
 *       Different name from "trace-token", different code path: the
 *       allow-list is not consulted because the substring check
 *       already returned no-match. "trace-id" contains none of the
 *       sensitive substrings.</li>
 *   <li>{@code tenant-id} — the multi-tenancy partition discriminator
 *       used to route a record to the correct customer-specific
 *       processing pipeline. Public information by design — every
 *       consumer needs to read it to decide whether to process or
 *       skip.</li>
 *   <li>{@code event-version} — schema versioning for forward and
 *       backward compatibility. Tells the consumer "this record was
 *       written by a producer using the v3 schema". Public, audited,
 *       not sensitive.</li>
 *   <li>{@code source-system} — provenance metadata identifying the
 *       upstream system that originated the record. Audit data,
 *       not a credential.</li>
 * </ol>
 *
 * <p>The class deliberately mixes the two emit sites
 * ({@code Headers.add(String, byte[])} and
 * {@code new RecordHeader(String, byte[])}) across both categories so
 * the test asserts that the rule's silence holds across the entire
 * Cartesian product (3 allow-list keys + 4 benign keys) x (2 emit
 * sites) — though we don't enumerate all 14 combinations, we cover
 * enough to prove the path independence.
 */
public final class GoodHeadersAllowList {

    public void emitIdempotencyTokenHeader(ProducerRecord<String, String> record, String nonce) {
        Headers headers = record.headers();
        headers.add("Idempotency-Token", nonce.getBytes(StandardCharsets.UTF_8)); // silent — allow-list
    }

    public void emitTraceTokenHeader(ProducerRecord<String, String> record, String correlationId) {
        Headers headers = record.headers();
        headers.add("trace-token", correlationId.getBytes(StandardCharsets.UTF_8)); // silent — allow-list
    }

    public RecordHeader buildCsrfTokenHeader(String value) {
        return new RecordHeader("csrf-token", value.getBytes(StandardCharsets.UTF_8)); // silent — allow-list, constructor form
    }

    public void emitTraceIdHeader(ProducerRecord<String, String> record, String traceId) {
        Headers headers = record.headers();
        headers.add("trace-id", traceId.getBytes(StandardCharsets.UTF_8)); // silent — no sensitive substring
    }

    public void emitTenantIdHeader(ProducerRecord<String, String> record, String tenantId) {
        Headers headers = record.headers();
        headers.add("tenant-id", tenantId.getBytes(StandardCharsets.UTF_8)); // silent — no sensitive substring
    }

    public RecordHeader buildEventVersionHeader(String version) {
        return new RecordHeader("event-version", version.getBytes(StandardCharsets.UTF_8)); // silent — no sensitive substring
    }

    public RecordHeader buildSourceSystemHeader(String system) {
        return new RecordHeader("source-system", system.getBytes(StandardCharsets.UTF_8)); // silent — no sensitive substring
    }
}
