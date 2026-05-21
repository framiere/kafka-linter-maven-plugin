package sample;

import org.apache.kafka.streams.KafkaStreams;

/**
 * RULE: STREAMS_CLEANUP_IN_PROD.
 *
 * Each of the three methods below calls {@code KafkaStreams.cleanUp()} in
 * main (non-test) code. cleanUp() deletes the entire local state directory
 * for this application.id — on the next start() the topology must replay
 * the full changelog topic from offset 0, turning a seconds-long restart
 * into a minutes-or-hours-long outage during which no records are consumed
 * or produced.
 *
 * The fourth method shows the correct shape: it touches the KafkaStreams
 * instance for an operational task but never calls cleanUp() — must NOT fire.
 */
public final class BadCleanupInProd {

    private final KafkaStreams streams;

    public BadCleanupInProd(KafkaStreams streams) {
        this.streams = streams;
    }

    /** Anti-pattern: wipe state on every restart "for a clean slate". */
    public void wipeStateOnBoot() {
        this.streams.cleanUp(); // FIRES
    }

    /** Anti-pattern: "recover" from any error by deleting and replaying everything. */
    public void recoverByWipe() {
        if (this.streams.state() == KafkaStreams.State.ERROR) {
            this.streams.cleanUp(); // FIRES
        }
    }

    /** Anti-pattern: parameter-passed instance, same hazard. */
    public static void resetState(KafkaStreams s) {
        s.cleanUp(); // FIRES
    }

    /** Control: no cleanUp() call — must NOT fire. */
    public KafkaStreams.State currentState() {
        return this.streams.state();
    }
}
