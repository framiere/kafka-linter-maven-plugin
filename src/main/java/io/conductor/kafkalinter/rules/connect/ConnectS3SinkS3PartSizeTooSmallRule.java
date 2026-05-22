package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when a Kafka Connect S3 sink connector
 * declares {@code s3.part.size} less than 5_242_880 bytes (5 MiB), which
 * is the AWS S3 multipart-upload minimum part size.
 *
 * <p>AWS S3 rejects {@code UploadPart} requests with bodies smaller than
 * 5 MiB (except the final part) with HTTP 400 {@code EntityTooSmall}.
 * The Confluent S3 sink ALWAYS uses multipart upload, so any value below
 * 5 MiB guarantees runtime failure on every flush against real AWS S3.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectS3SinkS3PartSizeTooSmallRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String S3_SINK_CLASS = "io.confluent.connect.s3.S3SinkConnector";
    private static final String S3_PART_SIZE = "s3.part.size";
    private static final long AWS_S3_MULTIPART_MIN_BYTES = 5L * 1024L * 1024L;

    private final Severity severity;

    public ConnectS3SinkS3PartSizeTooSmallRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_S3_SINK_S3_PART_SIZE_TOO_SMALL;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!S3_SINK_CLASS.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            Long partSize = tryParseLong(trimOrNull(p.getProperty(S3_PART_SIZE)));
            if (partSize == null) continue;
            if (partSize >= AWS_S3_MULTIPART_MIN_BYTES) continue;

            out.add(new Violation(
                    RuleId.CONNECT_S3_SINK_S3_PART_SIZE_TOO_SMALL, severity,
                    ctx.relativize(e.getKey()), "key:" + S3_PART_SIZE, 0,
                    "Connect S3 sink connector has s3.part.size=" + partSize
                            + " bytes, which is LESS THAN the AWS S3 multipart-"
                            + "upload minimum of " + AWS_S3_MULTIPART_MIN_BYTES
                            + " bytes (5 MiB). The 5 MiB minimum is an AWS S3 "
                            + "API CONTRACT (not a Confluent or plugin preference) "
                            + "— every `UploadPart` request except the final part "
                            + "of a multipart upload MUST be at least 5 MiB or "
                            + "AWS S3 rejects the request with HTTP 400 "
                            + "`EntityTooSmall`. The Confluent S3 sink ALWAYS "
                            + "uses multipart upload (no single-PUT fallback for "
                            + "any output format), so a sub-5MiB part size "
                            + "guarantees that every flush of every partition "
                            + "fails with `EntityTooSmall` against real AWS S3; "
                            + "the task fails; the connector reports `FAILED`; "
                            + "the data backlog grows; on-call is paged. The "
                            + "Confluent ConfigDef default is 26_214_400 (25 "
                            + "MiB), well above the minimum. Typical wrong-"
                            + "values origins: (a) MinIO local-dev leak — "
                            + "developer set `s3.part.size=1048576` for a local "
                            + "MinIO test (MinIO accepts sub-5MiB parts as a "
                            + "dev convenience), committed the .properties, the "
                            + "value reached production unchanged; (b) confusion "
                            + "with HDFS block size (a separate concept with "
                            + "different semantics); (c) misguided 'smaller "
                            + "parts = faster uploads' tuning that ignored the "
                            + "AWS minimum; (d) Helm-chart default set by "
                            + "someone who misread the docs. Fix: set `s3.part.size` "
                            + "to a value >= " + AWS_S3_MULTIPART_MIN_BYTES + " "
                            + "(typically 26_214_400 = 25 MiB, the Confluent "
                            + "default), or remove the explicit key. NOTE: the "
                            + "Confluent S3 sink's ConfigDef validator should "
                            + "reject sub-5MiB values at startup in current "
                            + "versions (10.x+), but the validator was "
                            + "inconsistent in older versions (some intermediate "
                            + "releases relaxed or removed it to support MinIO "
                            + "testing). This static-analysis rule is a "
                            + "defense-in-depth check that fires at PR time "
                            + "regardless of the operator's plugin version."));
        }
        return out;
    }

    private static Long tryParseLong(String s) {
        if (s == null) return null;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
