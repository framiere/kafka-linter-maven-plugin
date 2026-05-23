package sample;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * RULE: HEADERS_SENSITIVE_KEYS.
 *
 * <p>Fires when a Kafka record header is emitted with a KEY string
 * that contains a credential-shaped substring. Two emit sites are
 * checked:
 * <ol>
 *   <li>{@code Headers.add(String, byte[])} — the canonical
 *       fluent-builder API, called on whatever Headers
 *       implementation the producer / consumer hands back.</li>
 *   <li>{@code new RecordHeader(String, byte[])} — the constructor
 *       form, often used when building a header list externally
 *       and then passing it into a ProducerRecord constructor.</li>
 * </ol>
 *
 * <p>The rule's allow-list covers idempotency-token, trace-token,
 * and csrf-token — three "...token" header keys that, in practice,
 * are NOT credentials (the names just happen to contain the
 * substring).
 *
 * <p>Why credential headers are a real problem — what record
 * headers actually do:
 * <ol>
 *   <li>Headers are part of the binary record on the wire and at
 *       rest. They are NOT encrypted by the broker. SSL/TLS in
 *       transit covers the SOCKET, not the persistent storage —
 *       so the broker's log segments on disk contain header bytes
 *       in cleartext.</li>
 *   <li>Every consumer in the consumer group sees every header on
 *       every record. There is no per-consumer ACL on individual
 *       headers — a consumer that is authorized to read the topic
 *       is authorized to read every byte, including the headers.
 *       Authorization is per-topic at most.</li>
 *   <li>Mirror tools (MirrorMaker, Replicator, Confluent Cluster
 *       Linking) copy records byte-for-byte, headers included.
 *       A password header written into the topic is automatically
 *       replicated to every replica cluster, often crossing
 *       regional or even legal boundaries.</li>
 *   <li>Long-term sinks (S3 archival via the Confluent S3 sink
 *       connector, BigQuery via the BigQuery sink, Splunk via
 *       the Splunk sink) write the entire record including
 *       headers into immutable cold storage. The credential is
 *       now archived FOREVER. Even rotating the credential at
 *       source does not remove it from the archived tier — the
 *       attacker who already pulled the archive still has the
 *       old value.</li>
 *   <li>The right answer is the connection-layer auth APIs:
 *       mTLS for transport identity, SASL-OAUTHBEARER /
 *       SASL-SCRAM for principal auth, OAuth bearer tokens
 *       refreshed by the OAuthBearerLoginModule. None of these
 *       end up on the wire as record bytes — they live only in
 *       the producer/consumer's JVM heap and on the broker side
 *       for the duration of the connection.</li>
 *   <li>The reason developers write the credential header
 *       anyway: it FEELS like the easiest way to carry per-record
 *       authentication context to a downstream consumer that
 *       wants to call a third-party API on behalf of the record
 *       producer. The right fix is end-to-end token signing
 *       (JWT signed by the producer's HSM) where the signature
 *       is verifiable but the secret itself never leaves the
 *       signer. Putting the bearer token in a header is a
 *       shortcut that defers the problem to the sink operator.</li>
 * </ol>
 *
 * <p>What the rule catches (per-method, lexical scan):
 * <ol>
 *   <li>For each {@code MethodInsnNode} that is either
 *       {@code Headers.add(String, byte[])} or
 *       {@code new RecordHeader(String, byte[])}:
 *       walk backwards from the call site up to 30 significant
 *       instructions, collecting {@code LDC} String constants.</li>
 *   <li>For each string seen during the backward walk: lowercase
 *       it, check against the allow-list (skip if it matches),
 *       then check against the sensitive-substring set
 *       (password / passwd / secret / apikey / api_key / api-key /
 *       token / authorization / bearer / credential).</li>
 *   <li>Fire on the first match. The fire site is the header-emit
 *       instruction; the message names the offending string.</li>
 *   <li>The 30-step backward window is conservative — the typical
 *       distance between an LDC String and the call site is 2-4
 *       insns, so 30 leaves plenty of room for {@code getBytes()},
 *       method args built inline, etc. Stops at the previous
 *       header-emit call to avoid bleeding into the previous
 *       record's key.</li>
 * </ol>
 *
 * <p>This Bad class triggers SEVEN fires — one for each shape:
 * <ol>
 *   <li>Headers.add with "Authorization".</li>
 *   <li>Headers.add with "X-Api-Key".</li>
 *   <li>Headers.add with "x-bearer-token".</li>
 *   <li>Headers.add with "password" (lowercase exact).</li>
 *   <li>Headers.add with "secret-id" (substring "secret").</li>
 *   <li>new RecordHeader with "X-Service-Credential".</li>
 *   <li>new RecordHeader with "passwd" (legacy unix shorthand).</li>
 * </ol>
 */
public final class BadHeadersSensitiveKeys {

    public void emitAuthorizationHeader(ProducerRecord<String, String> record, String bearer) {
        Headers headers = record.headers();
        headers.add("Authorization", bearer.getBytes(StandardCharsets.UTF_8)); // reported
    }

    public void emitApiKeyHeader(ProducerRecord<String, String> record, String key) {
        Headers headers = record.headers();
        headers.add("X-Api-Key", key.getBytes(StandardCharsets.UTF_8)); // reported (substring "api-key")
    }

    public void emitBearerTokenHeader(ProducerRecord<String, String> record, String token) {
        Headers headers = record.headers();
        headers.add("x-bearer-token", token.getBytes(StandardCharsets.UTF_8)); // reported (substrings "bearer", "token")
    }

    public void emitPasswordHeader(ProducerRecord<String, String> record, String pw) {
        Headers headers = record.headers();
        headers.add("password", pw.getBytes(StandardCharsets.UTF_8)); // reported (substring "password")
    }

    public void emitSecretIdHeader(ProducerRecord<String, String> record, String secretId) {
        Headers headers = record.headers();
        headers.add("secret-id", secretId.getBytes(StandardCharsets.UTF_8)); // reported (substring "secret")
    }

    public RecordHeader buildCredentialHeader(String value) {
        return new RecordHeader("X-Service-Credential", value.getBytes(StandardCharsets.UTF_8)); // reported (substring "credential")
    }

    public RecordHeader buildPasswdHeader(String value) {
        return new RecordHeader("passwd", value.getBytes(StandardCharsets.UTF_8)); // reported (substring "passwd")
    }
}
