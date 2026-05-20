package io.conductor.kafkalinter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleIdTest {

    @Test
    void valueOfReturnsTheSameInstanceAsTheStaticConstant() {
        assertSame(RuleId.PRODUCER_IN_LOOP, RuleId.valueOf("PRODUCER_IN_LOOP"));
    }

    @Test
    void valueOfUnknownThrows() {
        assertThrows(IllegalArgumentException.class, () -> RuleId.valueOf("NOPE"));
    }

    @Test
    void valuesContainsCoreRules() {
        assertTrue(RuleId.values().size() >= 9,
                "registry should hold at least the original 9 kafka-clients rules");
        assertTrue(RuleId.values().contains(RuleId.CONSUMER_AUTO_COMMIT_TRUE));
        assertTrue(RuleId.values().contains(RuleId.KAFKA_CLIENTS_EOL));
        assertTrue(RuleId.values().contains(RuleId.STREAMS_EOS_V1_DEPRECATED));
    }

    @Test
    void metadataIsExposed() {
        RuleId r = RuleId.PRODUCER_SEND_BLOCKING_GET;
        assertEquals("PRODUCER_SEND_BLOCKING_GET", r.id());
        assertEquals(Severity.ERROR, r.defaultSeverity());
        assertEquals(Confidence.HIGH, r.confidence());
        assertEquals("kafka-clients", r.category());
        assertNotNull(r.tagline());
        assertNotNull(r.mechanism());
        assertNotNull(r.impact());
        assertNotNull(r.whyMatters());
        assertTrue(r.docPath().endsWith(".md"));
    }

    @Test
    void allRulesHaveCompleteDidacticBlock() {
        for (RuleId r : RuleId.values()) {
            assertTrue(r.tagline().length() > 20, r.id() + " tagline too short");
            assertTrue(r.mechanism().length() > 40, r.id() + " mechanism too short");
            assertTrue(r.impact().length() > 40, r.id() + " impact too short");
            assertTrue(r.whyMatters().length() > 40, r.id() + " whyMatters too short");
        }
    }
}
