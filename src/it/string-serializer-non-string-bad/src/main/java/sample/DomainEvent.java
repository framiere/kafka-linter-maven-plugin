package sample;

/**
 * Tiny non-String domain class used as the "wrong-typed value"
 * payload across the Bad and Good fixtures in this IT.
 *
 * Its {@code toString()} returns the default {@code Object} form
 * ({@code sample.DomainEvent@<hex>}) — exactly the accidental shape
 * the rule warns about, since {@code StringSerializer} calls
 * {@code .toString()} on its argument.
 */
public class DomainEvent {
    public final String id;
    public final String body;

    public DomainEvent(String id, String body) {
        this.id = id;
        this.body = body;
    }
}
